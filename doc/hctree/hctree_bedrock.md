

Hctree/Bedrock
==============

Leader/Follower Replication and Hctree/Bedrock
==============================================

<style>
  .pikchr-svg {
    margin: auto;
  }
  .hctree-bg {
    margin-left: 10%;
    margin-right: 10%;
    border: 1px dashed green;
    color: green;
    padding-left: 1ex;
    padding-right: 1ex;
  }
</style>

One of the 
[three goals for hctree](index.html) - as yet unimplemented and as yet
only vaguely conceived - is to provide support for leader/follower 
replication for use in a system like [bedrockdb](https://bedrockdb.com). This
page discusses exactly what those features might be by examining how a
high-concurrency hctree/bedrock system might work.

It is organized as follows:

1. **[Distributed Journal Changes](#journal_changes)** - in which two modifications
   to bedrock's distributed journal required to improve concurrency are described.

2. **[Node Catchup](#node_catchup)** - how the modified journal entry format
   permits concurrency during catchup.

3. **[Followers and ASYNC Transactions](#followers)** - 
   how the modified journal entry format permits concurrency when applying
   ASYNC transactions, and how readers must be managed on follower nodes to
   ensure they see only consistent database snapshots.

4. **[Leaders and ASYNC Transactions](#leaders)** - how leaders can generate
   journal entries while processing ASYNC transactions without sacrificing
   database concurrency.

5. **[Concurrent QUORUM Transactions](#quorum)** - how QUORUM transactions
   would work, and how they can be run concurrently.

6. **[Conclusion](#conclusion)** - a summary of the required hctree features 
   identified in the previous 4 sections.



<a name=journal_changes></a>
1.\ Distributed Journal Changes
-------------------------------

Bedrock links successfully committed write-transactions performed on the leader
node into a 
[distributed journal](https://bedrockdb.com/blockchain.html), where each
journal entry is an SQL script made up (more or less) of the write statements
from the original transaction.  The journal is distributed to follower nodes,
which evaluate journal entry scripts in order to bring the follower node
database up to date.

<div class=hctree-bg>
<b>Hctree background:</b>

Hctree assigns to each transaction a unique integer id - the total number of
transactions written to the database since the beginning of time. It calls
this value the "commit id" (CID).

Bedrock documentation and code sometimes calls this value the "commit count",
or the "transaction id", which is tricky because hctree uses "transaction id"
for something else. This page uses "commit id" or CID.

Whatever it's called, each write-transaction/journal entry is assigned a
CID as part of committing the transaction.

</div>

To improve concurrency, the distributed journal format used by an
hctree/bedrock system might differ from the current Bedrock in two
ways:

  1.  The contents of each journal entry is changed to a key-value format,
      where each key identifies a database row by table and PRIMARY KEY or
      rowid, and each value is either the new contents of the database row, or
      else a tombstone to indicate that the row is to be deleted.

      This change is to improve concurrency on follower nodes, and during
      synchronization/catchup.

  2.  The most recent part of a journal might be missing entries.

      Bedrock always commits transactions to the local db in order of CID.
      This means that journal entries are also committed in order and thus the
      journal always contains a contiguous array of entries. In a
      high-concurrency system, this restriction on commit order must be
      relaxed, and so it is no longer possible to guarantee that a local node's
      journal always contains a contiguous set of entries - some entries
      from the most recent part of the journal may be missing.

      This change is to allow (a) concurrent commits on leader nodes, and (b)
      concurrent QUORUM transactions.
 
### 1.1.\ Modified journal entry format

If we had the database:

```
    CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
    INSERT INTO t1 VALUES(1, 'one', 'i');
    INSERT INTO t1 VALUES(2, 'two', 'ii');
    INSERT INTO t1 VALUES(3, 'three', 'iii');
```
and a transaction run on the leader node contains: 
```
    UPDATE t1 SET c=NULL WHERE c IN('i', 'ii');
    INSERT INTO t1(a, b, c) VALUES(NULL, 'four', 'iv');
    DELETE FROM t1 WHERE b='three';
```
produces the following journal entry:

~~~ pikchr
arrow
CX: box "t1: rowid=1: (1, 'one', NULL)"  ljust \
    "t1: rowid=2: (2, 'two', NULL)"  ljust \
    "t1: rowid=3: DELETE"            ljust \
    "t1: rowid=4: (4, 'four', 'iv')" ljust fit
arrow
text "CID=X" with s at CX.n
~~~

notes:

  *  the first two entries in the journal entry ("t1: rowid=1..." and
     "t1:rowid=2...") correspond to the two rows modified by the UPDATE
     statement in the original transaction. The entire new row, including
     unmodified fields, is present in the journal entry.

  *  the third entry ("t1: rowid=3...") represents the single row 
     deleted by the DELETE statement in the original transaction.

  *  the final entry ("t1: rowid=4...") represents the row added by the
     INSERT statement in the original transaction. There is no difference
     between this entry and the two created by the UPDATE statement - in
     both cases the entry consists of the rowid and the new version of
     the tuple.

In other words, an hctree/bedrock journal entry contains the results of
executing the SQL transaction on the leader node instead of the SQL script
itself. This is very similar to what the SQLite [sessions
module](https://sqlite.org/sessionintro.html) does.

### 1.2.\ Relaxed Write-Order Restriction and Synchronization

Each time a transaction is committed to a bedrock node, be it a leader or
follower node, a corresponding journal entry must be written to the journal
table as part of the same atomic transaction.

Current bedrock also requires that transactions be committed in order of
CID value on both leaders and followers. During synchronization, this allows it
to assume that the journal table for a bedrock node's local database contains a
contiguous set of journal entries from CID=*iCidMin* to CID=*iCidLast*, where
*iCidLast* is the CID of the last transaction committed to the database before
the node was halted. And that the last entry is accompanied by a checksum that
summarizes all journal entries from CID=1 (the beginning of time) to
CID=*iCidLast*.

In order to achieve proper write concurrency, the restriction on commit
order must be relaxed. Which affects the possible states of the journal that
synchronization has to deal with. For example, if transactions CID=5
and CID=6 are committing concurrently and the hctree/bedrock process crashes or
is shut down, then following a restart it may be that transaction CID=6
committed but transaction CID=5 did not. This means synchronization must be
able to deal with a journal table that contains CID=6, but not CID=5.
More generally, the at synchronization time, the journal table contains:

  *  a contiguous set of journal entries from CID=*iCidMin* to
     CID=*iCidLastContiguous*, and a checksum for all journal entries
     from CID=1 to CID=*iCidLastContiguous*.

  *  optionally, a sparse population of journal entries with CID values 
     between CID=(*iCidLastContiguous*+1) and CID=*iCidLast*. This
     sparse population should span a relatively short amount of time - perhaps
     the same order of magnitude as the amount of time taken to commit a QUORUM
     transaction.

This makes synchronization more complicated in practice of course, but it
doesn't really change it logically. During synchronization, in current
bedrock:

  *  each node sends a summary of the contents of its journal table to all
     other nodes.

  *  any two nodes that disagree on the contents of some transaction CID=X 
     immediately disconnect from each other.

  *  otherwise, each node sends other nodes any journal entries they are
     missing.

None of the above changes for hctree/bedrock. The "summary" must change of
course, but could be as simple as a single checksum for all transactions
between CID=1 and CID=*iCidLastContiguous*, accompanied by the full data
for all transactions from the sparsely populated range.

~~~ pikchr
linewid=0.1
boxwid=0.2
boxht=0.2

START: arrow thin ; 
ICIDMIN1: box "12" ; arrow thin ; box "13";
arrow thin ; box "14" ; arrow thin ; box "15" ;
arrow thin ; box "16" ; arrow thin ; box "17" ;
arrow thin ; box "18" ; arrow thin ; 
ICIDLAST1: box "19" ;

arrow thin ; box thin dashed ; arrow thin ; box thin dashed ; 
arrow thin ; box thin dashed ; arrow thin ; box thin dashed ; 
arrow thin ; box thin dashed ; arrow thin ; box thin dashed ; 
arrow thin

NEXT: arrow thin from START.w + (0.0, -0.5)
ICIDMIN2: box "12" ; arrow thin ; box "13";
arrow thin ; box "14" ; arrow thin ; box "15" ;
arrow thin ; box "16" ; arrow thin ; box "17" ;
arrow thin ; 
ICIDLASTCONT2: box "18" ; 

FLC: arrow thin ; box thin dashed ;
arrow thin ; box thin dashed ; arrow thin ; box "21" ;
arrow thin ; box "22"        ; arrow thin ; box thin dashed ;

arrow thin ; box "25" ; arrow thin ; box thin dashed ;
LA: arrow thin

text "Example bedrock journal" "pre-synchonization:" with e at START.w
text "Example hctree/bedrock" "journal pre-synchonization:" with e at NEXT.w

L: line from FLC.c+(0.0,-0.15) to FLC.c+(0.05,-0.20) to LA.c+(-0.05,-0.20) to LA.c+(0.0,-0.15) thin 

text with c at L.c+(0.0,-0.1) "sparsely populated range"

text with c at ICIDLAST1.c+(-0.1,0.5) "iCidLast" italic
line from last text.s to ICIDLAST1.n thin

text with c at ICIDMIN1.c+(0.1,0.5) "iCidMin" italic
line from last text.s to ICIDMIN1.n thin

text with c at ICIDMIN2.c+(0.1,-0.5) "iCidMin" italic
line from last text.n to ICIDMIN2.s thin

text with c at ICIDLASTCONT2.c+(-0.1,-0.5) "iCidLastContiguous" italic
line from last text.n to ICIDLASTCONT2.s thin

~~~ 

One more rule:

When a new, fully-synchronized, leader takes over and begins accepting write
requests, if there are any missing journal entries in its local journal -
holes in the journal - these must be dealt with before proceeding. There are
two ways to do so:

  *  If it can be proven that none of the transactions that follow the
     hole in the journal could have depended on the missing transaction,
     then the journal hole can be filled in with an empty transaction (one
     that modifies no rows or schema entries).

  *  Otherwise, the transaction entries that follow the hole in the journal
     must be removed from the journal table and rolled back from the databse.

Follower nodes must of course be instructed to do the same.

The second of the above two options seems dramatic. It discards data, after
all. However, it is necessary to guarantee that the database snapshot following
the change-of-leader is consistent both internally and with the application
logic. <a href=replication.md#rollback_example>More detail here.</a>

<a name=catchup></a>
2.\ Node Catchup
----------------

When a node comes back online as a follower it has to apply journal entries 
to its local database in order to "catch up" to the current state of the
distributed database. Perhaps many journal entries.

<div class=hctree-bg>
<b>Hctree background:</b>

  *  Internally, hctree uses CID values as well. 

  *  Each table and index entry in an hctree database carries with it 
     the CID value<sup>\*</sup> of the transaction that wrote the entry.

  *  After a table or index entry has been deleted from an hctree database,
     something similar to a tombstone marker remains in the database,
     carrying with it the CID<sup>\*</sup> of the transaction that deleted the
     entry.

More detail on the things similar to tombstone markers [here](design.wiki#mvcc).

<sup>\*</sup>Actually this is not true - each entry contains a value that can be
mapped to the CID - but close enough.

</div>

Normally, hctree assigns its CID values itself, internally. However, we
could create a special "follower mode" with two features:

  *  It allows CID values to be assigned externally. That way, the CID values
     assigned to each journal entry transaction during catchup can be the same
     as the distributed journal CID values (or rather - assigned so that they
     are in the same order).

  *  It prevents old data from clobbering new data. When writing a database
     entry, it checks if the new entry will clobber an existing entry with
     a larger CID value. And if so, skips the write operation.

For example, say the journal contains two entries that both write to the row
with rowid=11 in table "t1":

~~~ pikchr
arrow
C6: box "...other write ops..." italic ljust "t1: rowid=11: (11, 'eleven', NULL)" ljust "...other write ops..." italic ljust fit
arrow
C7: box "...other write ops..." italic ljust "t1: rowid=11: (11, 'eleven', 'xi')" ljust "...other write ops..." italic ljust fit
arrow
text "CID=6" with s at C6.n
text "CID=7" with s at C7.n
~~~

During catchup, if these two transactions are executed concurrently, one 
of two things happens to the contended row (hctree's atomic update mechanism
guarantees this):

  *  The CID=6 write is applied, then an attempt to apply the CID=7 update is 
     made.
  *  The CID=7 write is applied, then an attempt to apply the CID=6 update is
     made.

In the first case above, there is no problem and the second write operation can
go ahead. CID=7 should follow CID=6 after all. In the second case, the thread
applying the CID=6 update can see that it would be clobbering a row written by
a transaction with CID=7 and simply skip the write. Either way, the final state
of the database reflects CID=7, which is correct. Due to the tombstone
marker analogues, this works with DELETE operations as well.

It follows that, so long as threads follow this rule of never clobbering
newer data with older data, journal entry transactions may be run in
any order during a catchup operation, using as many 
[concurrent threads as proves performant](threadtest.wiki#results).

<a name=followers></a>
3.\ Followers and ASYNC Transactions
--------------------------------

In a busy system, follower nodes are continually receiving a stream of
new journal entries corresponding to asynchronous transactions. These 
can be handled using "follower mode" in the same way as during catchup -
divided up between as many concurrent threads as desired, each of which is
careful never to clobber new data with old.

<div class=hctree-bg>
<b>Hctree background:</b>

  *  Each table and index entry in an hctree database is actually the
     head of a linked list containing recent historical versions of
     the entry (from newest to oldest). Each linked list entry contains
     a CID and data for the version of the database entry.

  *  When reading from an hctree db, the readers snapshot is defined by
     a CID value - the "snapshot id". All transactions with CIDs equal
     to or less than the snapshot id are included in the reader's snapshot.

  *  If a reader encounters a database entry with a CID greater than its
     snapshot id, it searches backwards in the linked list to find an
     older version that may be included in its snapshot.

More detail [here](design.wiki#mvcc).
</div>

<a name=snapshot_rules></a>
However, we do have to be a bit careful about readers. A reader must
not see an inconsistent snapshot, where a consistent snapshot S is 
defined as one that would be produced if all journal entries from CID=1
to CID=S were applied sequentially to the database. There are two
rules:

  *  Snapshot S is available if all transactions with CID values less
     than or equal to S have been completely committed.

  *  Except, if the transaction with CID value S1 omits a write because it
     would clobber a value written by transaction S2, where (S2>S1), then
     transactions S1 to (S2-1), inclusive, never become available to readers.

More concisely, snapshot S is available if all the rows that make up snapshot S
have been written to the database.

While processing a stream of transactions from a leader node, a follower
has to maintain value S0, the snapshot id for the newest snapshot that is
available in the local database. This is the snapshot that new readers will
read from.

### 3.1.\ Example of being a bit careful about readers

~~~ pikchr
arrow
C12: box "t1: rowid=5: (5, 'A1', 'B1')"   ljust \
         "t1: rowid=6: (6, 'C', 'D')"   ljust fit 
arrow
C13: box "t1: rowid=5: (5, 'A2', 'B2')"   ljust \
         "t1: rowid=7: (7, 'E', 'F')"   ljust fit 
arrow
C14: box "t1: rowid=8: (8, 'G', 'H')"   ljust \
         "t1: rowid=9: (9, 'I', 'J')"   ljust fit 
arrow
C15: box "t1: rowid=5: (5, 'A3', 'B3')"   ljust \
         "t1: rowid=10: (10, 'K', 'L')"   ljust fit 

text "CID=12" with s at C12.n
text "CID=13" with s at C13.n
text "CID=14" with s at C14.n
text "CID=15" with s at C15.n
~~~

The diagram above depicts a stream of 4 ASYNC transactions. Three of them
hit the same row, row rowid=5 in table t1. And write to other rows too.

Consider the effect of these all being committed concurrently on a follower
node, where the order in which the commits complete is CID=12, CID=15, 
CID=13, and finally CID=14.

  *  After **CID=12** finishes committing, S0 is set to 12. New readers
     can see the effects of CID=12, but not of any of the other transactions
     depicted.

  *  After **CID=15** is finished, S0 remains at 12. We cannot allow readers
     to see the effects of CID=15 until CID=13 and CID=14 are also available.

  *  Then **CID=13**. Due to a race condition, the CID=15 thread manages to
     write to rowid=5 before the CID=13 thread can get there. When the CID=13
     thread tries to update row rowid=5, it sees that it has already been
     update by a transaction with a larger CID (as 15&gt;13), and so omits this
     write.

     This means that S0 remains set to 12. We cannot set it to 13, as then
     readers would see the (7, 'E', 'F') record written by CID=13, but would
     still see the old (5, 'A1', 'A1') instead of CID=13's (5, 'A2', 'B2'),
     as it was never written to the db. An inconsistent snapshot.

  *  Once **CID=14** has finished committing, S0 is set to 15, as everything
     up to and including CID=15 has been committed.


<a name=leaders></a>
4.\ Leaders and Async Transactions
----------------------------------

Bedrock currently takes a mutex around all COMMIT operations. It does 3 things
under cover of this mutex: 

  *  Assigns a CID value.
  *  Writes a journal entry to the database for the transaction.
  *  Commits the transaction.

In order to maximize concurrency and throughput, an hctree/bedrock system
should avoid using such a mutex.

Hctree already assigns a CID value to each transaction internally, and
(obviously) commits transactions. In order to avoid sacrificing concurrency, it
also needs to build in support for writing journal entries at a low level.
Explanation follows:

<div class=hctree-bg>
<b>Hctree background:</b>

While processing SQL statements as part of a transaction, Hctree
accumulates all table and index b-tree inserts and deletes in memory.
Then, at COMMIT:

  1. All new keys and deletes are inserted into the database. At this 
     point they are ignored by all readers.

  2. The commit-id (CID) value is assigned to the transaction. CID values
     are 64-bit integers. They are assigned by incrementing a global counter.

  3. The transaction is validated (database is checked to see if any data
     read by the transaction has been modified). If validation fails, all
     keys and deletes inserted in step (2) above are removed from the db.

  4. If validation succeeds, an entry is set in an in-memory table to mark
     the transaction as fully committed. New clients are from this point
     able to see the data written by the transaction.

Multiple COMMIT operations can be concurrently ongoing. More detail regarding
[hctree COMMIT operations here](design.wiki#concurrency_write). 
</div>

The level of
detail above is salient for the following reasons: 

  *  Writing the journal entry to the database requires the CID value.

  *  The journal entry must be written to the database as part of its
     own atomic transaction - otherwise we would have no way to be sure 
     of the state of the local db following a restart.

  *  It would be quite sub-optimal to assign the CID value externally, 
     before the COMMIT starts. This is because it is not possible to start
     step (3) above until step (1) has finished for all transactions with
     a CID value smaller than the current transaction. Having to check
     this and wait on any slow transactions before starting step (3) is
     a concurrency killer.

It follows then that hctree should handle the journal table itself, as
a special case. So that each time a transaction is successfully committed,
hctree generates the journal entry, and:

  *  Writes it into the database journal table, so that it is committed
     atomically along with the rest of the transaction, and

  *  Returns it to the user - so that it can be sent to follower nodes.

When a transaction is rolled back after a CID is allocated (due to failed
validation), an empty entry is written to the journal and returned to the
user. The CID cannot be reused (some other thread may already be using CID
values greater than it), and followers need to know that the transaction is
finished so that it can keep track of the 
[available database snapshots](#snapshot_rules).

Leader nodes then simply write ASYNC transactions to the local database using
multiple threads. This automatically generates journal entries, both
on disk, and in-memory. These are periodically broadcast to followers.




<a name=quorum></a>
5.\ Quorum Transactions
-----------------------

<div class=hctree-bg>
<b>Hctree background:</b>

  *  The "hctree background" block in the section above contains a four step
     description of how transactions are committed in hctree.

  *  Not shown is that immediately after the CID is allocated in
     step 2, the snapshot containing the transaction is made available to
     local readers. That is, if the transaction being commited has CID=S,
     then the snapshot id for subsequent readers is S. This is true even
     though it is not known at that point whether or not S will be committed
     or rolled back (as validation has not yet been performed).

  *  If a reader encounters a database entry with a CID for which validation is
     pending, it blocks until the transaction is either committed or rolled
     back.

  *  This cannot deadlock as transactions never block during validation -
     they make the pessimistic assuption that all data will be committed.

</div>

Obtaining quorum for a QUORUM transaction is a type of transaction validation
and can be performed as part of the same step.

  *  The leader runs the QUORUM transaction as normal, allocating a
     CID and generating a journal entry for it. However, it stops
     just before step (4) in the commit procedure - after validation
     has succeeded.

  *  The journal entry is broadcast to all followers. Followers 
     apply the transaction in follower mode, and also stop just before
     the transaction is marked as committed (before step (4)).

  *  At this point, readers may continue to query the leader's database,
     but if they attempt to read any key written by our QUORUM transaction,
     they will block until it is marked as committed or rolled back.
     Same goes for writers - they may continue, but will block if they
     read any keys written by the QUORUM transaction.

  *  The same is true on follower nodes - readers may continue but
     they are blocked if they attempt to read a key belonging to a
     QUORUM transaction.

     We [said above](#snapshot_rules) that the snapshot "S" could not be made
     available on a follower node until all transactions with CID values less
     than or equal to S have been committed. This is an exception - even though
     the QUORUM transaction has not been committed, having written the keys
     into the database is enough for readers to access the snapshot.

  *  Once sufficient followers have replied to the leader, it 
     broadcasts a message to finish committing the quorum transaction. Or to
     roll it back if required.

  *  The leader also commits or rolls back the transaction on its local
     database and replies to the client.

Multiple threads can do this concurrently.



<a name=conclusion></a>
6.\ Conclusion
-----------------------

The approach described above requires the following new hctree features:

  1.  **Journal Entry Support**

    *  As part of a COMMIT operation on a leader node, after a CID is assigned
       to the transaction hctree should write a journal entry to a special
       table in the database.

    *  If transaction validation succeeds and the transaction is committed, the
       journal entry is committed to the db along with it.

    *  If transaction validation fails and the transaction is rolled back, an
       empty entry is written to the journal table instead.

  2.  **Follower Mode Writes**

    *  Follower mode writes are used to apply journal entries to a follower
       database.

    *  For a follower mode write, the CID value is specified by the writer, not
       generated internally by hctree.

    *  There is no transaction validation for follower mode transactions
       (although a post-validation callback may still be issued - see below).
       Instead, writers follow the rule of never clobbering newer data with
       older data.

  3.  **Follower Mode Snapshot Availability Queries**

    *  On a follower node, a client must be able to query for the CID of 
       the newest snapshot available.

  4.  **Post-validation Callback**

    *  The post-validation - or perhaps "custom validation" - callback is
       issued by hctree after a transaction is validated but before it is
       committed.

    *  It returns a value indicating whether the transaction should be
       committed or rolled back.

    *  The journal entry data and transaction CID are available to the callback
       code.

    *  Any reader that attempts to read a key while the transaction that wrote
       it is in the post-validation callback blocks until the transaction is either
       committed or rolled back (just as readers do if the writer transaction
       is still undergoing local validation).

    *  Other readers - those that do not read any keys written by the
       transaction still in the post-validation callback - may proceed as
       normal.





