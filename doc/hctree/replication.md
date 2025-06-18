
Replication APIs
================

Leader/Follower Replication Details
===================================

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
  .green {
    color: green;
  }
  pre {
    margin: 2em
  }
  .notice {
    color: red;
    font-size: 2em;
  }
</style>

<div class=notice>
This page is now historical. The new leader/follower replication API is
<a href=replication2.md>described here</a>.
</div>

This page builds on the [speculations here](hctree_bedrock.md).

<ol>
  <li> [Replicated Database Schema](#tables)
  <li> [Initializing a Replicated Database](#initializing)
  <li> [Selecting Leader/Follower Mode](#mode)
  <li> [Automatic Journalling For Leaders](#leaders)
  <li> [Applying Changes To Follower Databases](#followers)
  <li> [Validation Callback For Leader Nodes](#callback)
  <li> [Truncating the Journal Table](#truncating)
  <li> [Hash and Synchronization Related Functions](#hashes)
  <li> [Application Programming Notes](#applications)
  <li> [Table Details](#table_details)
  <li> [Data Format](#dataformat)
</ol>

<a name=tables></a>
1.\ Replicated Database Schema
==============================

A replicated database, one that supports leader/follower replication, 
contains the following two system tables:

```
    -- The "journal" table.
    CREATE TABLE sqlite_hct_journal(
      cid INTEGER PRIMARY KEY,
      schema TEXT,
      data BLOB,
      schemacid INTEGER,
      hash BLOB,
      tid INTEGER,
      validcid INTEGER
    );

    -- The "baseline" table.
    CREATE TABLE sqlite_hct_baseline(
      cid INTEGER,
      schemacid INTEGER,
      hash BLOB
    );
```

By default, the sqlite\_hct\_journal table - hereafter the "journal table" -
contains one entry for each transaction that has been written to the database
since the beginning of time. The four most interesting columns of the journal
table entry are:

  *  **cid**: The "Commit ID" of the transaction. A commit ID is assigned
     to each transaction, in incrementing order, starting from 1. Commit IDs
     define the logical order in which transactions are applied to the 
     database.

  *  **schema**: If the transaction modified the database schema, then
     this field contains an SQL script containing the CREATE and/or DROP
     statements that make up the schema modifications. Or, if the transaction
     did not modify the schema, then this column contains an empty (0
     character) string.

  *  **data**: This field contains a blob encoding all the data modifications
     made by the transaction. Or, if the transaction did not modify any data,
     a 0 byte blob.

  *  **schemacid**: This integer field identifies the CID value of the last
     transaction to modify the database schema that this transaction was
     executed against. Because schema transations are serialized in hctree - no
     other transaction may execute concurrently with a transaction that modifies
     the database schema - this completely identifies the version of the schema
     that the transaction must be executed against.

The current state of the database may be reconstructed by starting with an
empty database, and then for each journal table entry, in order of ascending
"cid":

  1.  Executing the "schema" SQL script, and then
  2.  Updating the database tables according to the contents of the "data"
      field.

The journal and baseline tables may be read like any other database table
using SQL SELECT statements. However they behave differently with respect
to transaction isolation. Specifically, all data committed to the journal
or baseline table becomes instantly visible to all SQL clients, even if
the write transaction is not included in the client's snapshot.

It is an error (SQLITE\_READONLY) to attempt to write to the journal or
baseline table directly. They may only be modified indirectly, using the
interfaces described in the following sections.

1.1. Truncated Journals
-----------------------

In practice, storing data for every transaction executed during the lifetime
of a database would soon become cumbersome. The API allows the journal to
be <a href=#truncating>truncated</a>. Truncating the journal atomically:

  *  Removes a contiguous set of the oldest entries from the journal 
     table, and
  *  Updates the baseline table.

The baseline table always contains a single row, summarizing all transactions
that have been removed from the journal table in the lifetime of the
database. Initally, the baseline table is populated as if:
```
    INSERT INTO sqlite_hct_baseline(0, 0, zeroblob(16));
```
Generally, the columns of the single row in the baseline table are populated 
as follows:

  *  **cid**: The CID of the last (newest) entry removed from the journal
              table.

  *  **schemacid**: The largest schemacid value for any entry removed from the
              journal table.

  *  **hash**: A cryptographic hash calculated based on the "cid", "schema",
               "data" and "schemacid" values of all entries removed from the
               journal table so far.


<a name=holes></a>
1.2. Holes in the Journal
-------------------------

Because transactions may be executed concurrently on both leader and 
follower nodes, a transaction with CID value C may be added to the journal
table before transaction C-1. This means that in general, a journal may
contain something other than a contiguous set of CID values starting from
(baseline.cid+1). In this case, the missing entries are refered to as 
"holes in the journal".


<a name=initializing></a>
2.\ Initializing a Replicated Database
======================================

To participate in leader/follower replication, a database must be configured
by calling the following function:

```
    /*
    ** Initialize the main database for replication. Return SQLITE_OK if
    ** successful. Otherwise, return an SQLite error code and leave an
    ** English language error message in the db handle for sqlite3_errmsg().
    */
    int sqlite3_hct_journal_init(sqlite3 *db);
```

This API call creates the journal and baseline tables in the database, and
adds the initial row to the baseline table.

When this API is called, the following must be true:

  *  The database must be completely empty (contain zero tables),
  *  The connection passed as the argument must be the only connection
     open on the database, and
  *  There must not be an open transaction on the database connection.

Some attempt is made to verify the three conditions above, but race conditions
are possible. For example, if another thread opens a connection to the 
database while sqlite3\_hct\_journal\_init() is being called, database
corruption may follow.

<a name=mode></a>
3.\ Selecting Leader/Follower Mode
==================================

Each replication-enabled database opened by a process is at all times in 
either LEADER or FOLLOWER mode. In FOLLOWER mode, it is an error to attempt
to write to the database using the SQL interface. In LEADER mode, it is
an error to call the sqlite3\_hct\_journal\_write() or
sqlite3\_hct\_journal\_snapshot() interfaces.

The LEADER/FOLLOWER setting is per-database, not per-database handle. If a
process has multiple handles open on the same replication enabled database,
then all handles share a single LEADER/FOLLOWER setting. Modifying the setting
via one handle modifies it for them all.

```
    #define SQLITE_HCT_JOURNAL_MODE_FOLLOWER 0
    #define SQLITE_HCT_JOURNAL_MODE_LEADER   1

    /* 
    ** Query the LEADER/FOLLOWER setting of the db passed as the 
    ** first argument. Return either an SQLITE_HCT_JOURNAL_MODE_XXX constant,
    ** or else a -1 to indicate that the main database of handle db is not a
    ** replication enabled hct database.
    */
    int sqlite3_hct_journal_mode(sqlite3 *db);

    /*
    ** Set the LEADER/FOLLOWER mode of the db passed as the first argument.
    ** Return SQLITE_OK if the db mode is successfully changed, or if
    ** it does not need to be changed because the requested mode is the
    ** same as the current mode. Otherwise, return an SQLite error code
    ** and leave an English language error message in the database handle
    ** for sqlite3_errmsg().
    **
    ** It is safe to call this function while there are ongoing read
    ** transactions. However, this function may not be called concurrently
    ** with any write transaction or sqlite3_hct_journal_write() call on
    ** the same database (database, not just database handle). Doing so
    ** may cause database corruption.
    */
    int sqlite3_hct_journal_setmode(sqlite3 *db, int eMode);
```

When a replication database is first opened, or when it is first marked as a
replication database by a call to sqlite3\_hct\_journal\_init(), it is in
FOLLOWER mode.

It may then be changed to LEADER mode using the API. Except, the mode may
only be changed to LEADER if there are no <a href=#holes>holes in the
journal</a>. A database in leader mode may be changed to FOLLOWER mode at
any time using the above API.


<a name=leaders></a>
4.\ Automatic Journalling For Leader Nodes
==================================

By default, whenever a transaction is committed to a database configured
for replication in LEADER mode, an entry is automatically added to the journal
table. 

In some cases, when a commit fails during transaction validation (after a
CID has been allocated), an entry is written to the journal table even though
the transaction has been rolled back. In this case both the "schema" and
"data" fields are 0 bytes in size.


<a name=followers></a>
5.\ Applying Changes To Follower Databases
==================================

To copy a journal entry from one database to another, the first 4 fields of
the journal entry must be passed to the following API:
```
    /*
    ** Write a transaction into the database.
    */
    int sqlite3_hct_journal_write(
        sqlite3 *db,                   /* Write to "main" db of this handle */
        i64 iCid,
        const char *zSchema,
        const void *pData, int nData,
        i64 iSchemaCid
    );
```
The database must be in FOLLOWER mode when this function is called. If
successful, a single call to sqlite3\_hct\_journal\_write() atomically updates
both the journal table and, according to the zSql and pData/nData arguments,
the database itself.

If the journal table already contains an entry with "cid" value iCid, then
the call fails with an SQLITE\_CONSTRAINT error. Or, if the iSchemaCid
value is not compatible with the current contents of the journal, this 
call also fails with SQLITE\_CONSTRAINT.

**Concurrency**

Each entry of the journal table represents a transaction. The contents of a
journal table, sorted in CID order, may be looked at as a series of groups of
transactions, where all members of a group have the same value for the
schemacid column. A group consists of:

  *   An entry with "schema" set to other than an empty string (i.e. a
      transaction that modifies the schema - a "schema transaction").
  *   Zero or more transactions that do not modify the db schema.

A schema transaction may not be applied concurrently with any other entry.
Before the schema transaction with cid=C can be applied, all transactions with
cid values less than C must have been completed. And schema transaction C must
have completed before any transaction with a cid value greater than C can be
applied. Any call to sqlite3\_hct\_journal\_write() that violates these
ordering rules fails with an SQLITE\_SCHEMA error. The application should
interpret this as "try that one again later".

However, within a group, non-schema transactions may be applied in any order,
using any number of threads (each with its own db handle).

<a name=snapshots></a>
**Snapshot Availability**

In a distributed system, it may be desired not to run a query on a follower
node until a certain transaction from the leader node has been propagated
and made available on the follower. For example, if a single client performs
a database write followed by a database read, then the results of the write
should be visible to the read, even if the read is performed on a follower
node.

The following API may be used to query for the current snapshot available
to readers on a follower node. If it is not new enough, the caller must
wait until further transactions have been applied to the db via
sqlite3\_hct\_journal\_write(), then retry this API call.
```
    /*
    ** Set output variable (*piCid) to the CID of the newest available 
    ** database snapshot. Return SQLITE_OK if successful, or an SQLite
    ** error code if something goes wrong.
    */
    int sqlite3_hct_journal_snapshot(sqlite3 *db, i64 *piCid);
```
In practice, the available snapshot S is the largest value for which all
journal entries with CID values S or smaller are already present in the
journal table.


<a name=callback></a>
6.\ Validation Callback for Leader Nodes
==================================

The sqlite3\_hct\_journal\_validation\_hook() API is used to register a
custom validation callback with a database handle. If one is registered,
the custom validation callback is invoked each time a transaction is commited
to an hctree database in LEADER mode.

More specifically, the validation hook is invoked after all new keys have 
already been written into the database, and after internal transaction
validation has succeeded. All that remains to commit the transaction is:

  *  To set an in-memory flag to indicate that the transaction has been
     committed to other clients, and

  *  To write a single zero into the start of the clients log file, so that
     the transaction will not be rolled back if a crash occurs.

If the validation hook returns 0, then the commit proceeds as normal. Or,
if it returns non-zero, then the transaction is rolled back and
SQLITE\_BUSY\_SNAPSHOT returned to the user, just as if internal transaction
validation had failed.
```
    /*
    ** Register a custom validation callback with the database handle.
    */
    int sqlite3_hct_journal_validation_hook(
        sqlite3 *db,
        void *pArg,
        int(*xValidate)(
            void *pCopyOfArg,
            i64 iCid
            const char *zSchema,
            const void *pData, int nData,
            i64 iSchemaCid
        )
    );
```
The first argument passed to the validation callback is a copy of the context
pointer supplied by the application as the second argument to
sqlite3\_hct\_journal\_validation\_hook(). The values passed to the following
arguments are identical to the leftmost 4 fields of the entry that will be
inserted into the journal table for the transaction, assuming it is committed.

The custom validation hook may be used for two purposes:

  1.  As an efficient way to obtain the 4 values that must be propagated to
      follower nodes for each transaction, and

  2.  To perform custom validation. An example of custom validation that might
      be required in a leader-follower system is that some transactions may 
      require that they be propagated to follower nodes before they can be
      committed.

For the purposes of <a href=snapshots>snapshot availability</a>, while the
validation hook is running for transaction T:

  *  The transaction counts as committed in the sense that, assuming all
     other conditions are met, transactions with CID values greater than
     that belonging to transaction T are visible to readers.

  *  However, any reader that attempts to read a key written (or deleted)
     by transaction T blocks (invokes the SQLite busy-handler callback) 
     until the validation hook returns and T is either committed or rolled
     back.

In cases where normal, local, validation of a transaction fails after a CID
value has been allocated, an <a href=#leaders>entry with zero length values
</a>for the schema and data columns is inserted into the journal table. In this
case the validation hook is invoked with the corresponding zero length
parameters, so that the empty transaction can be propagated to follower nodes.

<a name=truncating></a>
7.\ Truncating the Journal Table
==================================

In order to avoid the journal table from growing indefinitely, old entries 
may be deleted from it - on a FIFO basis only - once it is possible that they
will no longer be required for synchronization. The contents of the 
baseline table, always a single row, summarizes those journal entries 
already deleted from the journal table.

There is a C API to atomically remove rows from the journal table
and update the baseline table:

```
    /*
    ** Truncate the journal table of database zDb (e.g. "main") so that the
    ** smallest CID value it contains is iMinCid.
    */
    int sqlite3_hct_journal_truncate(sqlite3 *db, i64 iMinCid);
```

<a name=hashes></a>
8.\ Hash and Synchronization Related Functions
==================================

Synchronization between nodes is largely left to the application. The contents
of a journal table may be read using ordinary SQL queries, and missing
transactions applied to databases using the usual 
sqlite3\_hct\_journal\_write() interface described above. This section
describes the provided APIs for:

  *  Calculating hash values compatible with those written to the baseline
     table by the <a href=truncating>sqlite3\_hct\_journal\_truncate()</a>
     API. These are required to determine that two nodes really do have a
     compatible history and can be synchronized.

  *  Dealing with holes in the journal after node synchronization is 
     complete - when the missing transactions are completely lost and 
     cannot be found anywhere in the system.

Further nodes on how these functions might be used to implement node
synchronization may be <a href=#leader_failure>found here</a>.

<b>Calculating Hashes</b>

The hash value stored in the baseline table is SQLITE\_HCT\_JOURNAL\_HASHSIZE
(16) bytes in size. It may be calculated as follows:

  *  A hash for each transaction entry removed from the journal table
     may be calculated using sqlite3\_hct\_journal\_hashentry(). Internally,
     this hash is calculated using MD5.

  *  The baseline hash is the XOR of the hashes for all entries deleted
     from the journal table since the beginning of time. 

The following function may be used to calculate a hash for a single journal
table entry, based on the values of the "cid", "schema", "data" and 
"schemacid" columns:
```
    /*
    ** It is assumed that buffer pHash points to a buffer
    ** SQLITE_HCT_JOURNAL_HASHSIZE bytes in size. This function populates this
    ** buffer with a hash based on the remaining arguments.
    */
    void sqlite3_hct_journal_hashentry(
        void *pHash,              /* OUT: Hash of other arguments */
        i64 iCid,
        const char *zSchema,
        const void *pData, int nData,
        i64 iSchemaCid
    );
```

The following function may be used to calculate hash values compatible with
those stored by the system in the "hash" column of the sqlite\_hct\_baseline
table.
```
    /*
    ** Both arguments are assumed to point to SQLITE_HCT_JOURNAL_HASHSIZE
    ** byte buffers. This function calculates the XOR of the two buffers
    ** and overwrites the contents of buffer pHash with it.
    */
    void sqlite3_hct_journal_xor(void *pHash, const void *pData);
```

<a name=rollback></a>
<b>Dealing With Holes in the Journal</b>

If a journal <a href=#holes>contains holes</a>, the following are true:

  *  No data associated with journal entries that follow the first hole in
     the journal will be visible to readers

  *  It is not possible to switch the database from FOLLOWER to LEADER mode
     while holes exist.

The best way to deal with holes in the journal is to fill them in by calling
sqlite3\_hct\_journal\_write() with values corresponding to the missing
transactions obtained from elsewhere in the system. That way no data is
lost. This API provides an alternative for when the missing transactions
cannot be found anywhere in the system.

```
    /*
    ** Rollback transactions added using sqlite3_hct_journal_write().
    */
    int sqlite3_hct_journal_rollback(sqlite3 *db, i64 iCid);

    /* 
    ** Special values that may be passed as second argument to
    ** sqlite3_hct_journal_rollback().
    */
    #define SQLITE_HCT_ROLLBACK_MAXIMUM   0
    #define SQLITE_HCT_ROLLBACK_PRESERVE -1
```

The second argument passed to sqlite3\_hct\_journal\_rollback() must be either:

  *  A value greater than or equal to the CID of the first hole (missing
     transaction) in the journal. In this case all transactions with a CID
     greater than or equal to the specified value are rolled back and removed
     from the journal table.

  *  The value SQLITE\_HCT\_ROLLBACK\_MAXIMUM. This is equivalent to passing
     the CID of the first hole in the journal. All transactions following the
     first hole in the journal are rolled back and removed from the journal
     table.

  *  The value SQLITE\_HCT\_ROLLBACK\_PRESERVE. Calling
     sqlite3\_hct\_journal\_rollback() may rollback as many transactions as
     SQLITE\_HCT\_ROLLBACK\_MAXIMUM, except that if it can be proven that
     none of the transactions following a hole may not depend on the
     missing transaction, then the hole is filled in with a zero-entry (one
     with zero length values for both the data and schema fields). In other
     words, calling sqlite3\_hct\_journal\_rollback() this way makes a best
     effort to preserve as many transactions as possible, while still leaving
     the journal table without holes.

It is not possible to use this API to rollback further than the first hole in
the journal. This is because hctree does not guarantee that the information
required to do such a rollback is still present in the database file.

<a name=rollback_example></a>
<b>Example of why this is Necessary</b>

This might seem dramatic - it involves discarding transactions after all - but
is necessary under some circumstances. Suppose a failure in the system leaves
a follower node with transactions 1, 2, 3 and 5, but not 4. In this state
transaction 5 must be discarded, as it may depend on transaction 4 - without
transaction 4, transactions 1, 2, 3 and 5 may not constitute a valid database
state. This is true even if the application logic does not appear to demand
rigorous consistency. If, for example:

```
    -- Initial database state
    CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT UNIQUE);
    INSERT INTO t1 VALUES(101, 'abc');

    DELETE FROM t1 WHERE a=101;         -- transaction 4
    INSERT INTO t1 VALUES(102, 'abc');  -- transaction 5
```

In follower mode, sqlite3\_hct\_journal\_write() may be used to apply
transaction 5 to the db before transaction 4. If, following a failure in
the system, transaction 4 were lost while transaction 5 were not, the 
database UNIQUE constraint would be violated.

<a name=applications></a>
9.\ Application Programming Notes
==================================

This section contains a description of a simple replicated database system
that could be constructed using the APIs above, along with observations made
while designing and testing the same. This is not (unfortunately) a
comprehensive set of instructions for building a high-concurrency replicated
database. It is intended only to illustrate the roles that the APIs described
above are expected to play in such a system.

<b>Normal Operation</b>

Once the system is up and running, with one node elected as leader (and the
db in LEADER mode) and all others operating as followers (with the db in
FOLLOWER mode):

  *  Write transactions are processed only on the leader node. There is a 
     dynamic pool of writer database handles that may be used for writing.

  *  There is a <a href=#callback>custom validation hook</a> registered
     with each writer handle on the leader node. The custom validation hook
     serializes the 4 fields required to write a transaction to a follower
     database ("cid", "schema", "data" and "schemacid") and sends them
     to each follower node. Which then writes the transaction to its local db 
     using <a href=#followers>sqlite3_hct_journal_write()</a>.

  *  To maximize concurrency, each writer connection on the leader node might
     have its own dedicated follower database handle on each follower node,
     and its own dedicated sockets connection to each follower node as well.

~~~ pikchr
linewid=0.1

LEADER:   box height 1.5 width 1.5 radius 0.1
move
FOLLOWER: box same

text at LEADER.n + (0.0,0.1) "Leader Node"
text at FOLLOWER.n + (0.0,0.1) "Follower Node"

cylinder "Leader" "DB" height 0.7 width 0.5 at LEADER.c - (0.2,0)
cylinder "Follower" "DB" same at FOLLOWER.c + (0.2,0)

down
L1: box with ne at LEADER.ne - (0.1,0.2) height 0.2 width 0.2
move 0.1 ; L2: box same
move 0.1 ; L3: box same
move 0.1 ; L4: box same

down
F1: box with nw at FOLLOWER.nw - (-0.1,0.2) height 0.2 width 0.2
move 0.1 ; F2: box same
move 0.1 ; F3: box same
move 0.1 ; F4: box same

arrow <-> from L1.e to F1.w
arrow <-> from L2.e to F2.w
arrow <-> from L3.e to F3.w
SOCKET: arrow <-> from L4.e to F4.w

right
T1: text at LEADER.s - (0.2,0.3) "Pool of db" "connections for" "writing on leader,"
T2: text "each with its own" "socket connection" "to the follower node,"
T3: text "and its own" "follower connection" "replicating its" "htransactions"

BTOP1: line from L1.nw-(0.1,0.0) then go 0.1 sw
BBOT1: line from L4.sw-(0.1,0.0) then go 0.1 nw
BMID1: line from BTOP1.sw to BBOT1.nw
line from BMID1 to T1.n

BTOP2: line from F1.ne+(0.1,0.0) then go 0.1 se
BBOT2: line from F4.se+(0.1,0.0) then go 0.1 ne
BMID2: line from BTOP2.se to BBOT2.ne
line from BMID2 to T3.n

line from T2.n to SOCKET.c



~~~ 


  *  Read transactions may be executed on any node (leader or follower).
     In cases where a client executes a read transaction that must logically 
     follow a write transaction, it may use the
     <a href=#snapshots>sqlite3_hct_journal_snapshot()</a> API to verify
     that the snapshot it is reading is new enough to include the required
     write transaction (the cid of which the client obtained via the custom
     validation hook when it was executed).

<b>Adding a Node</b>

A new follower node may be added to the system at any point. Before it 
starts the new follower node must have some local database - an old version 
of the logical db.

  *  The new follower node must check that its current state really is a
     valid historical state of the logical database to which it is connecting,
     by comparing the contents of its <a href=#tables>sqlite_hct_baseline and 
     sqlite_hct_journal tables</a> with those of some other node. Because
     both the new follower and the existing node may have been updated by
     <a href=truncating>sqlite3_hct_journal_truncate()</a> at different points,
     <a href=hashes>sqlite3_hct_journal_hashentry()</a> and
     <a href=hashes>sqlite3_hct_journal_xor()</a> may be useful.

     If it is found that the new follower either:

       *  Has a journal entry with cid value C that is different from the
          journal entry with cid value C belonging to the existing node, or

       *  Has any journal entries that the existing node does not have.

     then the new follower may not connect to the system. How this is dealt
     with is out of scope here.

     To make things simpler, <a href=#rollback>
     sqlite3\_hct\_journal\_rollback</a>(SQLITE_HCT_ROLLBACK_MAXIMUM) may be 
     called before this step. This does not risk losing data, as it is
     not possible for the new follower to join the system if it has
     transactions in its journal that are not also held by other nodes in
     the system at the point when it joins. Depending on the history of the
     node, it may even reduce the chances of the node having an incompatible
     transaction in its journal.

  *  The new follower node must then obtain, by requesting it from existing
     nodes, data (i.e.  cid, schema, data and schemacid values) for all
     transactions required to bring the new nodes journal up to date. And
     call <a href=#followers>sqlite3_hct_journal_write()</a> for each to
     apply it to the local database.

  *  The new follower then informs the current leader of its existence, and
     begins receiving new transactions.

  *  Of course there is race-condition between the two steps above -
     transactions may be added to the database by the leader between
     when the new follower receives data from another node and when it
     begins receiving updates from the leader. This is easy to 
     resolve by reversing the order of the two steps - so that the new
     follower subscribes to the stream of new transactions from the leader
     node before it requests missing transactions from a peer node.

     If there have been no schema changes, then the new transactions may
     be applied to the database as soon as they are received - without waiting
     for the missing transactions from the peer node. Or, if there are one
     or more schema changes to apply, new transactions may be buffered in
     memory.

<a name=leader_failure></a>
<b>Failure of Leader Node</b>

When the current leader node fails or is shut down, the system must (somehow)
elect a new leader node from the remaining followers. At this point all
nodes in the system (including the new leader) are in FOLLOWER mode.

  *  The new leader must contact all other follower nodes, requesting any 
     journal table entries that it is missing. These are added to the 
     local db using <a href=#followers>sqlite3\_hct\_journal\_write()</a>.

  *  At this point the new leader has accumulated all transactions in
     the system. It then calls <a href=#rollback>
     sqlite3\_hct\_journal\_rollback</a>(SQLITE_HCT_ROLLBACK_PRESERVE) to
     remove any holes in the journal. The result is a journal with no holes
     that culminates in the newest database snapshot that could be
     reconstructed from the transactions found in the system following the
     failure of the old leader node.

  *  The leader then sends each existing follower all transactions that they
     are missing. The follower first calls <a href=#rollback>
     sqlite3\_hct\_journal\_rollback</a>(_maxcid_+1), where _maxcid_ is the
     largest CID value in the new leaders reconstructed journal table. It
     then applies all transactions sent by the leader using 
     <a href=#followers>sqlite3_hct_journal_write()</a>. At this point, the
     followers journal (and therefore database) is identical to the new
     leaders.

  *  The new leader calls <a href=#mode>sqlite3\_hct\_journal\_setmode()</a>
     to change to LEADER mode and begins accepting new write transactions.

<b>System Startup</b>

System startup or restart of a system consisting of multiple nodes, all
with potentially complementary or conflicting journals, may be a
complicated thing. However, a simple approach could be to:

  *  Require the user to nominate the first node to start - the initial 
     leader node.

  *  This initial leader starts, calls <a href=#rollback>
     sqlite3\_hct\_journal\_rollback</a>(SQLITE\_HCT\_ROLLBACK\_PRESERVE)
     to make its journal usable, and declares itself leader of a single node
     system.

  *  The initial leader node begins accepting connections from new follower
     nodes, using the procedure described above.

Of course, this simple approach could lead to connection errors if any
follower nodes have transactions in their journals that the initial leader
does not have. Various approaches could be developed to account for this.

<a name=table_details></a>
10.\ Table Details
==================

The journal table (sqlite\_hct\_journal):

<table width=80% align=center cellpadding=5 border=1>
<tr><th align=left> Column <th align=left> Contents
<tr><td> cid 
    <td> The integer CID (Commit ID) value assigned to the transaction. CID
         values are assigned in contiguous incrementing order.
<tr><td> schema
    <td> If the transaction made any modifications to the database schema,
         then this field contains an SQL script to recreate them. If the
         transaction did not modify the database schema at all, then this
         field is set to a string zero characters in length.
<tr><td> data
    <td> This field contains a blob that encodes the changes made to table
         contents by the transaction in a key-value format - where keys are
         the PRIMARY KEY field values of affected tables, and the values are
         either the new row data for the row, or a tombstone marker signifying
         a delete. The exact format is <a href=#dataformat>described here</a>.
<tr><td> schemacid
    <td> This field contains the "cid" value (an integer) corresponding to the
         transaction that created the version of the schema that this 
         transaction was executed. In other words, the cid of the transaction
         that most recently modified the schema.
<tr><td> hash 
    <td> Hash of fields "cid", "schema", "data" and "schemacid".
<tr><td> tid 
    <td> The integer TID (Transaction ID) value used internally for the
         transaction by the local hctree node. This is used internally and
         it not duplicated between leader and follower database nodes.
<tr><td> validcid 
    <td> This is used internally and is not copied between leader and follower
         database nodes.  
</table>

The baseline table (sqlite\_hct\_baseline):

<table width=80% align=center cellpadding=5 border=1>
<tr><th align=left> Column <th align=left> Contents
<tr><td> cid
    <td> The CID of the last journal entry deleted. All journal entries
         with CID values between 1 and this value, contiguous, have
         been deleted from the journal table. 
<tr><td> hash
    <td> A hash of the "hash" fields for all deleted journal entries.
<tr><td> schema\_version
    <td> The schemacid value of the last journal entry deleted.
</table>

<a name=dataformat></a>
11.\ Data Format
================

This section describes the format used by the blobs in the "data" column of
sqlite\_hct\_journal.

If the entry does not modify any table rows, then the data blob is zero
bytes in size. Otherwise, it consists of an 8-byte big-endian CID value
identifying the database snapshot against which the transaction was 
originally run, followed by a series of entries. Each entry begins with 
either 'T', or else an upper or lower-case 'I' or 'D'. The format of the rest
of the entry depends on its type. As follows:

<table width=80% align=center cellpadding=5 border=1>
<tr><th align=left> Character <th align=left> Type <th align=left> Format
<tr><td> 'T'
    <td> New table.
    <td> Nul-terminated UTF-8 table name.
<tr><td> 'i'
    <td> Insert on table with IPK or no PK.
    <td> A varint containing the rowid value. Followed by an SQLite format
         record containing the other record fields.
<tr><td> 'I'
    <td> Insert on table with explicit non-INTEGER PK.
    <td> An SQLite format record.
<tr><td> 'd'
    <td> Delete by rowid.
    <td> A varint containing the rowid to delete
<tr><td> 'D'
    <td> Delete by explicit non-INTEGER PK.
    <td> An SQLite record containing the PK of the row to delete.
</table>











