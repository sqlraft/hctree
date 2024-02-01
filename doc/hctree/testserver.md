
Replication Test API
====================

Replication Test API
====================

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

<ol>
  <li><a href=#overview>Overview</a>
  <li><a href=#leader>Leader Scripts</a>
  <li><a href=#follower>Follower Scripts</a>
  <li><a href=#config>Configuration Options</a>
</ol>

There are currently no applications that use the hct <a href=replication.md>
replication API</a>. However, this page describes a <a href=https://tcl.tk>
Tcl</a> extension that may be used to experiment with its performance and
capabilities.

<a name=overview></a>
1.\ Overview
============

The extension is built as part of the "testfixture" Tcl interpreter binary. To 
build "testfixture", run the following from the root directory of the source
tree:

```
    ./configure && make testfixture
```

Extension code is available in the <a href=../../src/test_hctserver.c>src/test_hctserver.c</a> file in the main source tree.

Using the extension to test the replication capabilities of hctree requires
writing at least two scripts:

  *  A "leader" script for a leader process. A leader process opens
     an hct replication-enabled database in leader mode. It is configured
     with one or more "jobs" - tcl scripts that each run in their own thread
     and have read/write access to the database. A leader process listens
     for connections from follower processes.

  *  A "follower" script for a follower process. A follower process opens
     an hct replication-enabled database in follower mode. A follower
     process connects to a leader process, synchronizes its database with
     that of the leader, then subscribes to the leader for updates to keep
     the follower db up to date. The leader sends updates to all connected
     follower processes as transactions are committed by the configured
     Tcl jobs.

There are example leader and follower scripts in the doc/hctree directory
of the source tree. To run the test system, first build testfixture as
described above. Then, still in the root directory of the source tree,
run both of the following commands. The commands must be run concurrently, and
the leader script must be started before the follower - so that the follower
script can connect to the leader.

```
    ./testfixture ./doc/hctree/testfixture_leader.tcl
    ./testfixture ./doc/hctree/testfixture_follower.tcl
```

When started, both leader and follower processes call 
<a href=replication.md#rollback> sqlite3\_hct\_journal\_rollback</a> on
the database to remove all journal entries following the first hole in
the journal.

When a follower process is started, it immediately connects to the leader, 
which is listening on a well-known port for connections. It then sends the
XOR of the hash of all journal entries in its database, along with the
largest CID value that it has, to the leader. Call this a "synchronization
request". Because sqlite3\_hct\_journal\_rollback() has been called to remove
all holes from the journal, this is sufficient for the leader node to determine
if the follower nodes database is compabitible with the leader node database.
If it is, then the leader node replies with all journal entries that it has
that the follower node does not. The follower node then uses multiple threads
to apply the journal entries to its local database using 
<a href=replication.md#followers>sqlite3\_hct\_journal\_write()</a>.

Along with the missing journal entries, the leader node sends the number of
jobs to the follower when it first connects. After it has applied the journal
entries, the follower node establishes one new connection to the leader node
for each job. On the leader, one new connection is assigned to each job
thread. Thereafter, when the thread commits a transaction to the database, the
registered <a href=replication.md#callback>sqlite3\_hct\_journal\_hook()</a>
callback serializes and sends the new journal entry via its dedicated socket
connection to each connected follower node. 

Meanwhile, on the follower node, a separate thread with its own database
connection has been launched for each job on the leader. These threads
wait on their dedicated socket for journal entries sent by job threads on the
leader node, applying them with 
<a href=replication.md#followers>sqlite3\_hct\_journal\_write()</a> as soon 
as they are received. These are called subscriber threads, as they subscribe
to changes from a job thread on the leader.

At this point, there is a possibility that the follower will miss any 
changes committed on the leader after its initial synchronization request
is sent but before the subscriber connections are established. To ensure
these are captured, nce the subscriber connections and threads have been 
established the follower node sends another synchronization request. As
before, multiple threads are used to apply the received changes to the
local follower database.

This is an implementation of the system <a href=replication.md#applications>
described (and diagramed) here</a> under "Normal Operation" and "Adding a 
Node". It does not attempt to handle complicated scenarios like leader node
failure or network partitioning. Nor does it attempt any kind of flow 
control.

<a name=leader></a>
2.\ Leader Scripts
==================

The full <a href=testserver_leader.tcl>leader script</a> described below is
available here. As is the <a href=testserver_follower.tcl>follower script</a>.

A leader script generally does the following:

  1.  Initializes an hctree replication enabled database. Alternatively, it
      may simply open an existing db. For example:
```
    file delete -force my_test.db
    sqlite3 dbhdl file:my_test.db?hctree=1 -uri 1
    sqlite3_hct_journal_init dbhdl
    sqlite3_hct_journal_setmode dbhdl LEADER
    dbhdl eval {
      CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
    }
    dbhdl close
```
  2. Creates and configures a "testserver" object.
```
    hct_testserver T my_test.db
    T configure -seconds 120
```
  The above configures the testserver object to run for 120 seconds before
  exiting. This is advisory, in fact the testserver run command will run
  until the last configured job (see step 3) has exited. Each Tcl job script
  has access to the "hct\_testserver\_timeout" command which returns true
  if the testserver has been running for more than the configured number of
  seconds (in this case 120), or false otherwise.

  3. Adds one or more jobs - Tcl scripts - that write to the database.
     The following adds 6 separate jobs to server object T created in
     step 2 above.
<pre>
    <span>#</span> Set variable $script to contain the script run by each job.
    set script {
      set nBusy 0
      set nCommit 0
      while {[hct_testserver_timeout]==0} {
        db eval {
          BEGIN CONCURRENT;
            REPLACE INTO t1 VALUES(
                random() % 10000, randomblob(100), randomblob(100)
            );
        }
        set rc [catch {db eval COMMIT} msg]
        incr nCommit
        if {$rc!=0} {
          if {$msg=="database is locked"} {
            db eval ROLLBACK
            incr nBusy
          } else {
            # An unexpected error. Rethrow an exception.
            error $msg
          }
        }
        after 10      ;# Sleep 10ms
      }
      puts "$nCommit attempted commits, $nBusy busy errors"
    }
&nbsp;
    <span>#</span> Configure the testserver object with 6 jobs, all running the same script.
    for {set iJob 0} {$iJob < 6} {incr iJob} {
      T job $script
    }
</pre>
  4. Runs the server. This command launches a separate thread for each
     job added to the server object, while also listening for connections
     from follower processes. It returns when the last of the job scripts
     has exited.
```
    T run
```

<a name=follower></a>
3.\ Follower Scripts
====================

The full <a href=testserver_follower.tcl>follower script</a> described below is
available here. As is the <a href=testserver_leader.tcl>leader script</a>.

A follower script is simpler than a leader script. It

  1.  Initializes an empty hctree replication enabled database. Or, it may
      open an existing database. For example:
```
    file delete -force my_follower.db
    sqlite3 dbhdl file:my_follower.db?hctree=1 -uri 1
    sqlite3_hct_jounal_init dbhdl
    dbhdl close
```
  2.  Create and configure a "testserver" object in follower mode. And to
      use 8 threads to synchronize the database.
```
    hct_testserver T my_follower.db
    T configure -follower 1 -syncthreads 8
```
  3.  Run the server object.
```
    T run
```
 
<a name=config></a>
4.\ Configuration Options
=========================

The testserver object created by the "hct\_testserver" Tcl command supports
the following configuration options.

<table width=90% align=center>
<tr><td>-follower<td>(default 0)
<tr><td><td>This boolean option should be set to 0 for leader nodes, or 1 for
            followers.

<tr><td>-host<td> (default "localhost")
<tr><td><td>This option has no effect on leader nodes. For follower nodes,
            it determines the host of the leader node to connect to.

<tr><td>-port<td> (default 21212)
<tr><td><td>For leader nodes, the port to listen for connections on. For
            follower nodes, the port to connect to.

<tr><td>-seconds<td> (default 0)
<tr><td><td>The number of seconds after the testserver "run" sub-command
            is invoked before the "hct\_testserver\_timeout" command returns
            non-zero in job threads. If this option is set to 0, then
            hct\_testserver\_timeout never returns non-zero.

<tr><td>-syncbytes<td> (default 0)
<tr><td><td>Normally, upon startup a follower sends a single synchronization
            request to the leader, processes the results, then sets up the
            subscriber connections and threads. However, if this option is 
            set to a non-zero value, then instead the follower sends another
            synchronization request, and continues sending them and processing
            the results until either (a) the leader replies with less than
            N bytes of data, where N is the value of this option, or (b)
            leader replies with more data than it did to the previous
            synchronization request.

<tr><td>-syncthreads&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<td> (default 1)
<tr><td><td>The number of threads used to process the reply to synchronization
            requests. Must be greater than 0.
</table>



