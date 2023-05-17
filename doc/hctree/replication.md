
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
</style>

This page builds on the [speculations here](hctree_bedrock.md).

<ol>
  <li> [Configuring a Database For Replication](#configuring)
  <li> [Automatic Journalling For Leaders](#leaders)
  <li> [Applying Changes To Follower Databases](#followers)
  <li> [Custom Validation Callback](#callback)
  <li> [Truncating the Journal Table](#truncating)
  <li> [Hash and Synchronization Related Functions](#hashes)
  <li> [Application Programming Notes](#applications)
  <li> [Data Formats](#formats)
</ol>

<a name=configuring></a>
1.\ Configuring a Database For Replication
------------------------------------------

To participate in leader/follower replication, a database must be configured
by calling the following function:

```
    /*
    ** Initialize the main database for replication.
    */
    int sqlite3_hct_journal_init(sqlite3 *db);
```

This API must be called before the database is created - immediately after
an empty db is opened. Then, when the database is created, it sets a header
flag to mark it as a replication database and ensures that the special
sqlite\_hct\_journal and sqlite\_hct\_baseline tables are created:

```
    CREATE TABLE sqlite_hct_journal(
      cid INTEGER PRIMARY KEY,
      schema TEXT,
      data BLOB,
      schema_version BLOB,
      hash BLOB,
      tid INTEGER,
      validcid INTEGER
    );
    CREATE TABLE sqlite_hct_baseline(
      cid INTEGER,
      schema_version BLOB,
      hash BLOB
    );
```

The journal table is initially empty. The baseline table initially contains a
single row, populated as if: 

```
    INSERT INTO sqlite_hct_baseline VALUES(
        0,                        -- cid
        zeroblob(16),             -- schema_version
        zeroblob(16)              -- hash
    );
```

where 16 is the size in bytes of the fixed-size &lt;algorithm-name&gt;
hashes used in the journal and baseline tables:

```
    #define SQLITE_HCT_JOURNAL_HASHSIZE 16
```

As with other system tables (sqlite\_schema, sqlite\_stat1 etc.) the journal
and baseline tables may be read using SQL SELECT statements, but may not be
written by ordinary clients using UPDATE, DELETE or INSERT statements.
They may only be modified indirectly, using the interfaces described in the 
following sections.

<a name=leaders></a>
2.\ Automatic Journalling For Leaders
-------------------------------------

By default, whenever a transaction is committed to a database configured
for replication, an entry is automatically added to the journal table.
The three most interesting columns of the journal table entry are:

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

Logically, the current state of the database may be reconstructed by starting
with an empty database, and then for each journal table entry, in order of
ascending "cid":

  1.  Executing the "schema" SQL script, and then
  2.  Updating the database tables according to the contents of the "data"
      field.

During step (2), no trigger or foreign key processing is performed. The 
effects of these are already accounted for in the contents of the data
field.

In some cases, when a commit fails during transaction validation (after a
CID has been allocated), an entry is written to the journal table even though
the transaction has been rolled back. In this case both the "schema" and
"data" fields are 0 bytes in size.

The columns of a journal table entry are populated as follows:

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
         a delete. See TODO for details.
<tr><td> schema\_version 
    <td> A hash value. If the current row contains an empty "schema" column,
         then this column is populated with a copy of the schema\_version 
         value for the previous transaction. Or, if the current row contains
         schema modifications, then this column contains a hash of the
         schema\_version of the previous transaction and the contents of
         the "schema" column. In other words, this column contains a hash
         of all non-empty schema columns in the journal table.
<tr><td> hash 
    <td> Hash of fields "cid", "schema_version", "schema" and "data".
<tr><td> tid 
    <td> The integer TID (Transaction ID) value used internally for the
         transaction by the local hctree node. This is used internally and
         it not duplicated between leader and follower database nodes.
<tr><td> validcid 
    <td> This is used internally and is not copied between leader and follower
         database nodes.  
</table>

<a name=followers></a>
3.\ Applying Changes To Follower Databases
------------------------------------------

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
        const void *pSchemaVersion
    );
```
If successful, a single call to sqlite3\_hct\_journal\_write() atomically
updates both the journal table and, according to the zSql and pData/nData
arguments, the database itself.

If the journal table already contains an entry with "cid" value iCid, then
the call fails with an SQLITE\_CONSTRAINT error. Or, if the pSchemaVersion
hash is not compatible with the current contents of the journal, this call 
also fails with SQLITE\_CONSTRAINT.

**Concurrency**

Each entry of the journal table represents a transaction. The contents of a
journal table, sorted in CID order, may be looked at as a series of groups of
transactions, where all members of a group have the same value for the
schema\_version column. A group consists of:

  *   An entry with "sql" set to other than an empty string (i.e. a
      transaction that modifies the schema - a "schema transaction").
  *   Zero or more transactions that do not modify the db schema.

A schema transaction may not be applied concurrently with any other entry.
Before the schema transaction with cid=C can be applied, all transactions with
cid values less than C must have been completed. And schema transaction C must
have completed before any transaction with a cid value greater than C can be
applied. Any call to sqlite3\_hct\_journal\_write() that violates these
ordering rules fails with an SQLITE\_SCHEMA error. The application should
interpret this as "try that one again later".

However, within a group, transactions may be applied in any order, using any
number of threads (each with its own db handle).

<a name=snapshots></a>
**Snapshot Availability**

In a distributed system, it may be desired not to run a query on a follower
node until a certain transaction from the leader node has been propagated
and made available on the follower. For example, if a single client performs
a database write followed by a database read, then the results of the write
should be visible to the read, even if the read is performed on a follower
node.
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
4.\ Custom Validation Callback
------------------------------

The sqlite3\_hct\_journal\_validation\_hook() API is used to register a
custom validation callback with a database handle. If one is registered,
the custom validation callback is invoked each time a transaction is commited
to an hctree database with replication enabled - either using normal SQL
statements, or by calling the sqlite3\_hct\_journal\_write() function.

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
            const void *pSchemaVersion
        )
    );
```
The first argument passed to the validation callback is a copy of the context
pointer supplied by the application as the second argument to
sqlite3\_hct\_journal\_validation\_hook(). The values passed to the following
arguments are identical to the leftmost 4 fields of the entry that will be
inserted into the journal table for the transaction, assuming it is committed.

The custom validation hook may be used for two purposes:

  1.  On leader nodes, as an efficient way to obtain the 4 values that must be
      propagated to follower nodes for each transaction, and

  2.  To perform custom validation. An example of custom validation that might
      be required in a leader-follower system is that some transactions may 
      require that they be propagated to follower nodes before they can be
      committed.

The custom validation hook is invoked for both transactions performed using
the ordinary SQL interfaces and for transactions written into the database
using API function sqlite3\_hct\_journal\_write(). For the purposes of 
<a href=snapshots>snapshot availability</a>, while the validation hook is
running for transaction T:

  *  The transaction counts as committed in the sense that, assuming all
     other conditions are met, transactions with CID values greater than
     that belonging to transaction T are visible to readers.

  *  However, any reader that attempts to read a key written (or deleted)
     by transaction T blocks (invokes the SQLite busy-handler callback) 
     until the validation hook returns and T is either committed or rolled
     back.

<a name=truncating></a>
5.\ Truncating the Journal Table
--------------------------------

In order to avoid the journal table from growing indefinitely, old entries 
may be deleted from it - on a FIFO basis only - once it is possible that they
will no longer be required for synchronization. The contents of the 
baseline table, always a single row, summarizes those journal entries 
already deleted from the journal table.

<table width=80% align=center cellpadding=5 border=1>
<tr><th align=left> Column <th align=left> Contents
<tr><td> cid
    <td> The CID of the last journal entry deleted. All journal entries
         with CID values between 1 and this value, contiguous, have
         been deleted from the journal table. 
<tr><td> hash
    <td> A hash of the "hash" fields for all deleted journal entries.
<tr><td> schema\_version
    <td> The schema_version value of the last journal entry deleted.
</table>

Instead, there is a C API to atomically remove rows from the journal table
and update the baseline table:

```
    /*
    ** Truncate the journal table of database zDb (e.g. "main") so that the
    ** smallest CID value it contains is iMinCid.
    */
    int sqlite3_hct_journal_truncate(sqlite3 *db, i64 iMinCid);
```

<a name=hashes></a>
6.\ Hash and Synchronization Related Functions
----------------------------------------------

Synchronization between nodes is largely left to the application. The contents
of a journal table may be read using ordinary SQL queries, and missing
transactions applied to databases using the usual 
sqlite3\_hct\_journal\_write() interface described above.

From the point of view of readers, the sqlite\_hct\_journal and
sqlite\_hct\_baseline tables do not follow the same rules for consistency
or isolation as other tables. New journal entries created by SQL transactions
become visible to readers as soon as the creating transaction is committed or
rolled back. Similarly, the effects of sqlite3_hct_journal_write() and
sqlite3_hct_journal_truncate() calls are immediately visible to all readers.


<b>Calculating Hashes</b>

The following function may be used to calculate hash values compatible with
those stored by the system in the "hash" column of the sqlite\_hct\_baseline
table.
```
    /*
    ** Both arguments are assumed to point to SQLITE_HCT_JOURNAL_HASHSIZE
    ** byte buffers. This function updates the hash stored in buffer pHash
    ** based on the contents of buffer pData.
    */
    void sqlite3_hct_journal_hash(void *pHash, const void *pData);
```
When it truncates entries from the start of the journal table, the
sqlite3\_hct\_journal\_truncate() effectively starts with the hash value
from the baseline table in buffer pHash, then calls
sqlite3\_hct\_journal\_hash() on the hash column of each entry being deleted
from the sqlite\_hct\_journal table, in ascending order of CID. The final
value of buffer pHash is then stored in the baseline table as part of the
new baseline record.

The value of the hash column of a journal table entry may be calculated based
on the four values that define the transaction using the following API 
function:
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
        const void *pSchemaVersion
    );
```

A schema version may be calculated based on the previous schema version and
the SQL script in the "schema" column of a journal entry:
```
    /*
    ** Update the hash in pHash based on the contents of the nul-terminated
    ** string passed as the second argument. If the string is zero bytes in
    ** length, then the value stored in buffer pHash is unmodified.
    */
    void sqlite3_hct_journal_hashschema(void *pHash, const char *zSchema);
```

<b>Plugging Journal Holes</b>

If a journal table is "missing" entries - that is it does not contain only
entries with a contiguous set of CID values starting from 
(baseline.cid+1) - it is said to contain "holes". So long as a journal contains
holes, the following are true:

  *  No data associated with journal entries that follow the first hole in
     the journal will be visible to readers

  *  It is not possible to write to the database using normal SQL interfaces.
     Any attempt to do so fails with an SQLITE_BUSY_SNAPSHOT error.

Calling the following API writes empty entries (entries containing no data or
schema modifications) to the journal table into all holes up to and including
cid=iCid.
```
    /*
    ** Write empty records for any missing journal entries with cid values
    ** less than or equal to iCid.
    */
    int sqlite3_hct_journal_patchto(sqlite3 *db, i64 iCid);
```

<a name=applications></a>
7.\ Application Programming Notes
---------------------------------

TODO

<a name=formats></a>
8.\ Data Formats
----------------

This section describes the format used by the blobs in the "data" column of
sqlite\_hct\_journal. Each blob consists of a series of entries. Each entry
begins with either 'T', 'I' or 'D'. The format of the rest of the entry 
depends on its type. As follows:

<table width=80% align=center cellpadding=5 border=1>
<tr><th align=left> Character <th align=left> Type <th align=left> Format
<tr><td> 'T'
    <td> New table.
    <td> Nul-terminated table name.
<tr><td> 'i'
    <td> Insert on table with IPK or no PK.
    <td> A varint containing the rowid value. Followed by an SQLite format
         record containing the other record fields.
<tr><td> 'I'
    <td> Insert on table with explicit non-INTEGER PK.
    <td> an SQLite format record.
<tr><td> 'd'
    <td> Delete by rowid.
    <td> A varint containing the rowid to delete
<tr><td> 'D'
    <td> Delete by explicit non-INTEGER PK.
    <td> An SQLite record containing the PK of the row to delete.
</table>











