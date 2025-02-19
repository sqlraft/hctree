
Simpler Leader/Follower Replication for Hctree
====================================================

Hctree currently has a set of fairly complicated features that can be used to
implement the type of leader/follower replication that bedrock uses:

  <https://sqlite.org/hctree/doc/hctree/doc/hctree/replication.md>

This page describes a simpler alternative. The key features of which are that:

  *  **Transactions are stored as SQL text**, as in current bedrock.

  *  **Each row of the journal table contains a "depends upon snapshot" field**.
     This field describes which transactions from earlier in the journal table
     must have been executed on a follower node before it is safe to execute
     the current transaction. When applying transactions to a database a
     follower node may run as many transactions as desired concurrently,
     provided that no transaction is run before those that it depends upon have
     been committed.

The current version stores changes as key-value pairs, like the sessions
module does. This maximises concurrency on follower nodes, but it seems
like the implementation has the following drawbacks:

  *  For some operations, the full values of all modified rows might be
     signicantly larger than the SQL statements that generate them.
     Specifically, when a single field of a large JSON object is updated,
     the UPDATE statement may be *much* smaller than the new row value.

  *  The key-value transaction record is less valuable as an audit trail
     than the actual SQL commands would be.

  *  Schema modifications have to be handled in a different way from data 
     updates.

  *  It's really quite complicated internally. More things to go wrong.

Hence this alternative.

Journal Table Schema
====================

In current bedrock, the journal table schema is:

```
  CREATE TABLE journal( 
      id INTEGER PRIMARY KEY,     /* Sequence number */
      query TEXT,                 /* SQL text for transaction */
      hash TEXT                   /* Checksum of this and preceding entries */
  );
```

To optimize for hctree and concurrency it becomes:

```
  CREATE TABLE hct_journal( 
      cid INTEGER PRIMARY KEY,    /* Sequence number - "Commit ID" */
      query TEXT,                 /* SQL commands for transaction */
      snapshot INTEGER,           /* "depends upon" cid value */
      tid INTEGER                 /* Used by hctree - "Transaction Id" */
  );
```

There is one entry in this table for each distributed transaction executed
against the database. Each transaction is assigned a sequential id starting
from 1. The "cid" column stores the transaction id, and the "query" column
stores the full text of the SQL required to recreate the transaction. By
executing the SQL for each row in the hct_journal table, in ascending order of
cid starting from 1, the database may be completely recreated from scratch.

The "snapshot" field contains an integer value that is always smaller than
the "cid" value. When the transaction is distributed to follower nodes, or
to nodes running catchup, it is only safe to apply a transaction with
"snapshot" value S once all transactions with "cid" values less than or
equal to S from the hct_journal table have been committed.

The hct_journal table must be created using the following function:

```
  int sqlite3_hct_journal_init(sqlite3 *db);
```

As well as creating the hct_journal table, this function marks it as special
internally, so that it behaves differently in two ways:

  *  Changes made to the hct_journal table are visible to all readers as soon
     as they are committed, even if those readers opened their transaction 
     before the changes were committed to the hct_journal table.

  *  Read-write conflicts are not possible on the hct_journal table.
     Write-write conflicts still are.

This function not only creates the table, but also adds the initial record,
equivalent to:

```
  INSERT INTO hct_journal VALUES(1, '', 0, 0);
```

Rows are only inserted into the hct_journal table by APIs described on this
page.  However, in order to save space, rows may be deleted from the
hct_journal table using regular SQL DELETE statements. If this is required,
rows must be deleted in order of cid value - committing a transaction to delete
row cid=(N+1) from the hct_journal table while it still contains row cid=N
corrupts the journal. Furthermore, at least one row must remain in the
hct_journal table at all times. An empty hct_journal table is corrupt.

Definitions:

  *  An hct_journal table is said to be *contiguous* if the cid's form
     a contiguous block, not necessarily starting from 1.

  *  An hct_journal table is said to be *non-contiguous* if there are one
     or more missing entries - if the cid's are non-contiguous.

Database Modes
==============

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
  ** Query/set the mode of the main database opened by database handle db.
  */
  int sqlite3_hct_journal_mode(sqlite3 *db, int eMode, int *peMode);

  /* 
  ** Values for sqlite3_hct_journal_mode().
  */
  #define SQLITE_HCT_NORMAL   0
  #define SQLITE_HCT_LEADER   1
  #define SQLITE_HCT_FOLLOWER 2
```

Restrictions:

  *  A database may only be changed to LEADER mode if it has a contiguous
     hct_journal table. 

  *  Attempting to change a database from NORMAL to either LEADER or FOLLOWER
     mode requires exclusive access. The results of attempting to do so while
     any other client is reading or writing to the same database are undefined.

  *  Attempting to change a database from FOLLOWER to LEADER, or from LEADER
     to FOLLOWER, requires that no other client be simultaneously writing to
     the same database (readers are ok). The results of attempting to do so
     while any other client is writing to the same database are undefined.

In LEADER or FOLLOWER mode, the only way to write to the database is to commit
a transaction using sqlite3_hct_journal_leader_commit() or
sqlite3_hct_journal_follower_commit() APIs, respectively. Attempting to write to the
database any other way is an error. Similarly, using either of the
aforementioned functions while in NORMAL mode is an error.


Leader Node Functionality
==============

On a leader node, writers must commit transactions using the following API
instead of the usual "COMMIT" SQL command:

```
  /*
  ** Commit a leader transaction.
  */
  int sqlite3_hct_journal_leader_commit(
     sqlite3 *db,                   /* Commit transaction for this db handle */
     const u8 *aData, int nData,    /* Data to write to "query" column */
     i64 *piCid,                    /* OUT: CID of committed transaction */
     i64 *piSnapshot                /* OUT: Min. snapshot to recreate */
  );
```

Calling the above function writes a record into the hct_journal table and
commits the transaction. Parameters aData/nData are used to pass the SQL 
text that should be stored in the "query" column of the new record. Output
parameters piCid and piSnapshot are used to return the values stored in
the "cid" and "snapshot" columns of the new record. The leader node can
then pass them, along with the "query" column value to each follower
node to distribute the transaction.

If an error (including a conflict) occurs while committing the transaction,
there are two possibilities:

  *  The error (or conflict) was detected before a CID value was allocated.
     In this case, both (*piCid) and (*piSnapshot) are set to 0.

  *  The error (or conflict) was detected after a CID value was allocated.
     In this case a record is written to the hct_journal table, with
     the "query" column set to NULL and "snapshot" to 0. In this case the
     output variable (*piCid) is set to the allocated CID value, and
     (*pSnapshot) is set to 0. The leader should propagate this "empty"
     transaction to all followers.

Follower Node Functionality
==============

While the system is running, leader nodes forward transactions to followers.
For each transaction executed on the leader, three values are passed to
each follower:

  *  The "cid" value - the distributed sequence number of the transaction.

  *  The "query" value - the SQL to execute to effect the transaction.

  *  The "snapshot" value - the minimum snapshot that must be available within
     a follower database before this transaction can be applied.

The available snapshot number in a follower database is the highest CID value
in the hct_journal table that belongs to an unbroken sequence of CID values
beginning with the smallest one in the table. In other words, the CID value
immediately before the first missing transaction. Readers that open a new
transaction on the follower database see data from all transactions with a CID
value equal to or smaller than the available snapshot number. Data from 
transactions with larger CID values is not visible.

A transaction received from a leader node may not be executed against the
follower database until the available snapshot is greater than or equal
to the snapshot value specified for the transaction. The current available
snapshot may be queried using the following API:

```
  /*
  ** Set output variable (*piSnapshot) to the number of the newest available
  ** database snapshot. Return SQLITE_OK if successful, or an SQLite
  ** error code if something goes wrong.
  */
  int sqlite3_hct_journal_snapshot(sqlite3 *db, i64 *piSnapshot);
```

If the required snapshot is available, then the transaction can be executed
against the follower db, using the normal SQL APIs. Except, the transaction
must be committed using the following API:

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

The aData/nData, iCid and iSnapshot values should be those received from the
leader for the transaction.

When a new node is attached to a database, it presumably requests all the
transactions it is missing from an existing node. These may then be applied
to the database in follower mode in the same way as if they were received as
executed from the leader node.

Application Programming Notes
==============

