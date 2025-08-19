Hctree Leader/Follower Replication
==================================

<ol>
  <li> [Overview](#overview)
  <li> [The hct\_journal Table](#table)
  <li> [Database Modes](#modes)
  <li> [Leader Mode](#leader_mode)
  <li> [Follower Mode](#follower_mode)
  <li> [Local Transactions](#local)
</ol>

<a name=overview></a>
1.\ Overview
========

This page describes Hctree features to support leader/follower replication -
multi-node systems in which one node is designated the leader and all others
followers. Write transactions are initially executed against the leader
node, and the results forwarded to and applied to follower nodes.  Read
transactions may be executed against either the leader or a follower node.

Leader/follower replication features are based around a special system table,
hct\_journal. It has the following schema:

```
 CREATE TABLE hct_journal( 
    cid INTEGER PRIMARY KEY,    /* Sequence number - "Commit ID" */
    query TEXT,                 /* SQL commands for transaction */
    snapshot INTEGER,           /* "depends upon" cid value */
    logptr BLOB                 /* Used by hctree internally */
 );
```

The hct\_journal table contains one row for each transaction applied to the
distributed database. On leader nodes, the entries are added automatically
when transactions are committed to the database. On follower nodes, they 
are added when transactions forwarded from the leader node are applied to
the database. When a new node is attached to an existing leader/follower
cluster, the application layer may compare the contents of its hct\_journal
table with that of an existing node and request the missing transactions in
order to synchronize the local database with the system.

<a name=table></a>
2.\ The hct\_journal Table
======================

The columns of the hct\_journal table are as follows:

<table>
<tr><th> Name <th> Contents
<tr><td> cid <td> 
  The transaction "Commit ID". When a distributed transaction
  is first committed to the leader node database, it is assigned a commit id.
  Commit IDs are assigned sequentially starting from 1.
  <br><br>
  The distributed database may be recreated from scratch by executing the
  "query" SQL for all transactions, in ascending order of CID, starting 
  at 1.
<tr><td> query <td> The SQL commands that make up the transaction.
<tr><td> snapshot&nbsp; <td> The CID value identifying the database snapshot
  that this transaction depends upon. This value is always less than the CID
  value of the current transaction. This transaction may be successfully
  written to a follower database, or to a node being synchronized, only after
  all transactions with CID values less than or equal to "snapshot" have been
  committed.  
<tr><td> logptr <td> This is a blob value used internally by hctree.
</table>

The hct\_journal table should not be created directly. Instead, applications
should use the following API function:

```
 int sqlite3_hct_journal_init(sqlite3 *db);
```

As well as creating the hct\_journal table, this function marks it as special
internally, so that it behaves differently in two ways:

  *  Changes made to the hct\_journal table are visible to all readers as soon
     as they are committed, even if those readers opened their transaction 
     before the changes were committed to the hct\_journal table.

  *  Read-write conflicts are not possible on the hct\_journal table.
     Write-write conflicts still are.

This function not only creates the table, but also adds the initial record,
equivalent to:

```
 INSERT INTO hct_journal VALUES(1, '', 0, 0);
```

Rows should only be inserted into the hct\_journal table by APIs described on
this page. However, in order to save space, rows may be deleted from the
hct\_journal table using regular SQL DELETE statements at any time. If this is
required, rows must be deleted in order of cid value - committing a transaction
to delete row cid=(N+1) from the hct\_journal table while it still contains row
cid=N corrupts the journal. Furthermore, at least one row must remain in the
hct\_journal table at all times. An empty hct\_journal table is corrupt.

The restrictions above are not enforced by the library - it is possible to
insert, update or delete rows within the hct\_journal table using normal SQL
commands at any time. However, the results of proceeding or restarting with
a corrupt hct\_journal table are undefined.

Some similar systems maintain running checksums or hash values on the contents 
of the journal table in order to make synchronization easier. Hctree leaves
this to the application layer.

Because hctree supports multiple concurrent writers, the transaction with
CID value (N) may be committed and written to the hct\_journal table before
the transaction with CID value (N-1). This implies that at any time, the
contents of the hct\_journal table may be *non-contiguous*. 
<a name=contiguous></a> Definitions:

  *  An hct\_journal table is said to be *contiguous* if the CID values 
     for all rows in the table form a contiguous block, not necessarily
     starting from 1.

  *  An hct\_journal table is said to be *non-contiguous* if there are one
     or more missing entries - if the cid's are non-contiguous.

<a name=modes></a>
3.\ Database Modes
===============

A database may be one of three modes:

  *  NORMAL mode,
  *  FOLLOWER mode, or
  *  LEADER mode.

The mode of a database is a property of a database. If there is more than one
connection to a single database, they all share the same database mode.

The database mode is not persistent. When a database is first opened, it is in
NORMAL mode. It may then be changed to FOLLOWER or LEADER mode using the
following API:

```
  /*
  ** Query the mode of the main database opened by database handle db.
  */
  int sqlite3_hct_journal_mode(sqlite3 *db);

  /*
  ** Set the mode of the main database opened by database handle db.
  */
  int sqlite3_hct_journal_setmode(sqlite3 *db, int eMode);

  /* 
  ** Values for sqlite3_hct_journal_mode().
  */
  #define SQLITE_HCT_NORMAL   0
  #define SQLITE_HCT_LEADER   1
  #define SQLITE_HCT_FOLLOWER 2
```

Restrictions:

  *  A database may only be changed to LEADER mode if it has a 
     <a href=#contiguous>contiguous hct_journal table</a>. 

  *  Attempting to change a database from NORMAL to either LEADER or FOLLOWER
     mode, or from LEADER or FOLLOWER back to NORMAL, requires exclusive
     access. The results of attempting to do so while any other client is
     reading or writing to the same database are undefined.

  *  Attempting to change a database from FOLLOWER to LEADER, or from LEADER
     to FOLLOWER, requires that no other client be simultaneously writing to
     the same database (readers are ok). The results of attempting to do so
     while any other client is writing to the same database are undefined.

In LEADER or FOLLOWER mode, the only way to write to the database is to commit
a transaction using sqlite3_hct_journal_leader_commit() or
sqlite3_hct_journal_follower_commit() APIs, respectively. Attempting to write to the
database any other way is an error. Similarly, using either of the
aforementioned functions while in NORMAL mode is an error.

<a name=leader_mode></a>
4.\ Leader Mode
===========

When in leader mode, transactions are still begun using the usual "BEGIN"
or "BEGIN CONCURRENT" SQL commands. However, transactions may not be committed
using the COMMIT or RELEASE commands. Nor may implicit transactions created by
writing to the database without a prior BEGIN command be committed. Instead:

  *  Distributed transactions (those that will be forwarded to follower nodes)
     must be committed by invoking the sqlite3\_hct\_journal\_leader\_commit()
     API, and

  *  <a href=#local>Local transactions</a> (those that will not be forwarded to
     follower nodes) must be committed using the
     sqlite3\_hct\_journal\_local\_commit() API.

The signature of the sqlite3\_hct\_journal\_leader\_commit() method is as
follows:

```
 /*
 ** Commit a leader transaction.
 */
 int sqlite3_hct_journal_leader_commit(
   sqlite3 *db,                   /* Commit transaction for this db handle */
   const u8 *aData, int nData,    /* Data to write to "query" column */
   i64 *piCid,                    /* OUT: CID of committed transaction */
   i64 *piSnapshot                /* OUT: Depends-on snapshot */
 );
```

The aData/nData parameters to the sqlite3\_hct\_journal\_leader\_commit() are
used to pass the text that should be stored in the "query" column of the
hct\_journal table after the transaction is committed. By convention, this
should be the text of the SQL commands required to recreate the transaction.

If the transaction is successfully committed, then an entry is automatically
written to the hct\_journal table. The CID and depends-on snapshot values
are automatically written along with the new hct\_journal table entry, and
are also made available to the caller via output parameters (\*piCid) and
(\*piSnapshot).

If an error (including a conflict) occurs while committing the transaction, there are two possibilities:

  *  The error (or conflict) was detected before a CID value was allocated. In
     this case, both (\*piCid) and (\*piSnapshot) are set to 0.

  *  The error (or conflict) was detected after a CID value was allocated. In
     this case a record is written to the hct\_journal table, with the "query"
     column set to NULL and "snapshot" to 0. In this case the output variable
     (\*piCid) is set to the allocated CID value, and (\*pSnapshot) is set to 0.
     The leader should propagate this "empty" transaction to all followers.

Hctree allows multiple database connections to commit transactions using
sqlite3\_hct\_journal\_leader\_commit() concurrently. This means that the
hct\_journal table might be <a href=#contiguous>non-contiguous</a> at some
points in time. If a process crash occurs in leader mode and, following
recovery, the hct\_journal table is found to be non-contiguous, "zero"
entries are inserted in all the gaps in order to make it contiguous. A "zero"
entry is one where the "query" column is empty and the "snapshot" column is
set to 0.

<a name=follower_mode></a>
5.\ Follower Mode
=============

When in follower mode, transactions are still begun using the usual "BEGIN"
or "BEGIN CONCURRENT" SQL commands. However, transactions may not be committed
using the COMMIT or RELEASE commands. Nor may implicit transactions created by
writing to the database without a prior BEGIN command be committed. Instead:

  *  Distributed transactions (those forwarded to the follower nodes)
     must be committed by invoking the sqlite3\_hct\_journal\_follower\_commit()
     API, and

  *  <a href=#local>Local transactions</a> (those that will not be forwarded to
     follower nodes) must be committed using the
     sqlite3\_hct\_journal\_local\_commit() API.

The sqlite3\_hct\_journal\_follower\_commit() API has the following signature:

```
 /*
 ** Commit a follower transaction.
 */
 int sqlite3_hct_journal_follower_commit(
   sqlite3 *db,
   const u8 *aData, int nData,   /* Value for "query" column */
   i64 iCid,                     /* CID of committed transaction */
   i64 iSnapshot                 /* Snapshot value for transaction */
 );
```

While the system is running, leader nodes forward transactions to followers.
For each transaction executed on the leader, three values are passed to
each follower (these are the same as the values stored in the first three
columns of the hct\_journal table for the transaction):

  *  The "cid" value - the distributed sequence number of the transaction.

  *  The "query" value - the SQL to execute to effect the transaction.

  *  The "snapshot" value - the minimum snapshot that must be available within
     a follower database before this transaction can be applied.

When it receives the above data, the follower node:

  1) Opens a transaction using "BEGIN".

  2) Executes the SQL for the transaction - the "query" value.

  3) Commits the transaction and writes the hct\_journal entry by calling
     sqlite3\_hct\_journal\_follower\_commit(), passing the three values
     described above.

It is not possible to commit a follower transaction until the required snapshot
is available within the database. A snapshot is a CID number. The snapshot is
available in the database once all transactions with a CID value less than
or equal to the snapshot number have been committed. For a follower transaction
to be successfully committed, the required snapshot must be available when
the "BEGIN" command issued. If it is not, then the eventual call to
sqlite3\_hct\_journal\_follower\_commit() returns SQLITE\_BUSY\_SNAPSHOT and
the transaction is not committed. An application may check if a snapshot
is available using the following API:

```
 /*
 ** Set output variable (*piSnapshot) to the snapshot number of the newest
 ** available database snapshot. All snapshots with a snapshot number less
 ** than or equal to the final value of (*piSnapshot) are available. Return
 ** SQLITE_OK if successful, or an SQLite error code if something goes wrong.
 */
 int sqlite3_hct_journal_snapshot(sqlite3 *db, sqlite3_int64 *piSnapshot);
```

When a new node is attached to a database, it presumably requests all the
transactions it is missing from an existing node. These may then be applied
to the database in follower mode in the same way as if they were received as
executed from the leader node.

Hctree allows multiple database connections to commit transactions using
sqlite3\_hct\_journal\_follower\_commit() concurrently. This means that the
hct\_journal table might be <a href=#contiguous>non-contiguous</a> at some
points in time. If a process crash occurs in leader mode and, following
recovery, all transactions in the hct\_journal time following the first gap
are rolled back and the entries removed from hct\_journal. Ensuring that
the hct\_journal table is always contiguous following startup.

<a name=local></a>
6.\ Local Transactions
==================

Local transactions are transactions executed in LEADER or FOLLOWER mode that
will not be distributed. They are started using the usual BEGIN or "BEGIN
CONCURRENT" commands, but must be committed using the following API:

```
 /*
 ** Commit a local transaction in either FOLLOWER or LEADER mode.
 */
 int sqlite3_hct_journal_local_commit(sqlite3 *db);
```

Local transactions are subject to the following caveats:

  *  In both LEADER and FOLLOWER modes, at most 15 local transactions may
     be committed between commits of distributed transactions.

  *  In FOLLOWER mode, only write/write conflicts may be detected, read/write
     conflicts do not prevent transactions from being committed. This is
     because read/write conflict detection is not performed at all in FOLLOWER 
     mode, as transaction dependencies were already determined on the leader
     node and encoded in the "snapshot" field of the hct\_journal table.

Local transactions are intended to be used to remove rows from the hct\_journal
table, or to write to other tables containing hash values or other things used
by synchronization. It is not recommended to edit the same tables as are used
by distributed transactions using local transactions.




