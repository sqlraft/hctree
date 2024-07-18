

sqlite3 db ""
foreach a $argv {
  set fd [open $a]
  set txt [read $fd]
  close $fd
  db eval $txt
}

set G(width) 10.0
set G(height) 1.2
set G(border.bcw2)    lightgreen
set G(border.hctree)  lightsteelblue
set G(border.hct1024) pink
set G(color.bcw2)     lightgreen
set G(color.hctree)   $G(border.hctree)
set G(color.hct1024)  $G(border.hct1024)

proc sputs {text} {
  upvar chart chart
  set data [uplevel [list subst -nocommands $text]]
  set data [string trim [regsub -all -lineanchor {^[ ]*} $data ""]]

  append chart "$data\n"
}

proc make_chart {testcase} {
  global G
  set chart ""

  db eval {
    SELECT system, test, nthread, nsecond, data FROM result WHERE test=$testcase
  } {
    set nOk 0
    set nBusy 0
  
    array unset A 
    array set A $data
    foreach k [array names A] {
      foreach {t s} [split $k ,] {}
      if {$s=="ok"}   { incr nOk $A($k) }
      if {$s=="busy"} { incr nBusy $A($k) }
    }
  
    incr R($system,$nthread,ok) $nOk
    incr R($system,$nthread,busy) $nBusy
    incr R($system,$nthread,total) [expr $nOk + $nBusy]
    incr T($system,$nthread) $nsecond
  }

  # Scale all values in the R array so that they are per second. Also set
  # local variable $max to the largest scaled value in the array.
  #
  set max 0
  foreach k [array name R] {
    foreach {sys nt topic} [split $k ,] {}
    set R($k) [expr $R($k) / $T($sys,$nt)]
    if {$R($k)>$max} { set max $R($k) }
  }

  set scale [expr $G(height) / $max]

  sputs {
    <center><verbatim type=pikchr>
    boxwid=0.10
    movewid=0.1
  }
  
  sputs {
    FIRST: box color none fill none width 35%
  }

  # Find all the threads values in the dataset.
  set lThread [db eval {SELECT DISTINCT nthread FROM result ORDER BY 1 ASC}]

  foreach thread $lThread {
    set sprev -100.0

    foreach sys {bcw2 hctree hct1024} {
      set nTrans $R($sys,$thread,ok)
      set K1 "[expr int($nTrans/1000)]"
      if {$nTrans<10000} { set K1 [expr ($nTrans/100)/10.0] }
      set S1 [expr $nTrans * $scale]
      set K1 ${K1}k

      set adj 0.0
      if {abs($S1-$sprev) < 0.06} { set adj [expr 0.12 - ($S1-$sprev)] }
      set adjust "+ (0.0,$adj)"
      set sprev [expr $S1+$adj]

      sputs {
        box color $G(border.$sys) fill $G(color.$sys) height $S1 behind FIRST with sw at last box.se + (0.025, 0.00)
        text "$K1" small small with s at last box.n $adjust fill grey
      }
    }

    sputs {
      text "$thread" with n at 2nd last box.s
      box color none fill none width 30% with sw at last box.se
    }
  }

  sputs {
    line color grey from first box.sw to last box.se
    text "Threads:" with ne at last line.w
    VERT: line color grey from last line.sw up $G(height)
  }

  set loc "with nw at VERT.n + (0.1,0.0)"
  foreach sys {bcw2 hctree hct1024} {
    sputs {
      box width 0.5 height 0.2 color $G(border.$sys) fill $G(color.$sys) $loc
      text "$sys" with n at previous.n
    }
    set loc "with w at last box.e + (0.1,0.0)"
  }
 
  foreach peg {100000 50000 25000 10000 5000 1000} {
    if {($max / $peg)>=5} break
  }
 
  set iPeg 0
  for {set p $peg} {$p < $max} {incr p $peg} {
    incr iPeg
    set tag "[expr $p/1000]K"
    set S [expr $p*$scale]
    sputs {
      line color grey from first line.w + (0.0,$S) left 0.1
      PEG$iPeg: text "$tag" with e at last line.w
    }
  }
 
  sputs {
    line invis from (PEG$iPeg.w - (0.1,0.0), PEG1.w) up until even with PEG$iPeg.w "transactions/second" aligned
  }
  
  sputs {
    </verbatim></center>
  }

  return $chart
}


puts [subst {

<title> Thread Test </title>

<h1> Thread Test </h1>

<p>This project contains no code stable enough to deploy. The database backend
works well enough to run some test cases, but 
<a href=index.html#status>is still quite incomplete</a>.

<p>Even so, the prototype is advanced enough to use to test whether or not
multiple writers really can run concurrently on multi-processor systems.
That is the goal of the tests on this page - to verify the extent that
the database allows multiple clients in separate application threads to
run concurrent read/write transactions.

<h1> Tested Configurations </h1>

<p>Performance of hctree in two configurations is compared to stock SQLite
modified with the begin-concurrent and wal2 patches. Each test is run with
three systems:

<ul>
  <li> <p>System <b>hctree</b> - this is hctree with all default options,
       including the default 4096 byte page-size.

  <li> <p>System <b>hct1024</b> - hctree with 1024 byte pages.

  <li> <p>System <b>bcw2</b> - this is stock SQLite with the begin-concurrent
       and wal2 patches applied. Specifically, the 
       <a href=../../../../timeline?r=begin-concurrent-pnu-wal2>
       begin-concurrent-pnu-wal2</a> branch. 
</ul>

<p>The bcw2 system is configured as follows:

<pre>
      PRAGMA journal_mode = wal2;
      PRAGMA mmap_size = 1000000000;
      PRAGMA synchronous = off;
      PRAGMA journal_size_limit = 16777216;
</pre>

Additionally:

  *  All "COMMIT" operations are protected by an application mutex.

  *  One thread is designated the checkpointer. Checkpoints are performed
     outside the mutex when the system indicates that one is possible via
     the wal-hook (i.e. when there is 16MB of checkpointable data in
     one wal file).

  *  The VFS is "unix-excl", so no system calls are made for locks, and the
     mutex-free locking patch has been applied. The library is built with
     the following options:

<pre>
      -DSQLITE_SHARED_MAPPING=1
      -DSQLITE_DEFAULT_MEMSTATUS=0
      -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS=1
</pre>

<h1>Test Overview</h1>

<p>All tests use the same database. The schema is:

<pre>
      CREATE TABLE tbl(
         a INTEGER PRIMARY KEY,
         b BLOB(200),
         c CHAR(64)
      );
      CREATE INDEX tbl_i1 ON tbl(substr(c, 1, 16));
      CREATE INDEX tbl_i2 ON tbl(substr(c, 2, 16));
</pre>

<p>The initial database contains 1,000,000 rows. Column "a" contains values
1 to 1,000,000. Column "b" contains a 200 byte blob value. Column "c"
contains a 64 byte text value.

<p>Each test consists of between 1 and 16 threads, all running transactions
as fast as possible for 30 seconds. Each transaction consists of:

<pre>
      BEGIN;
        -- repeat <i>nScan</i> times:
        SELECT * FROM tbl WHERE substr(c, 1, 16)>=hex(frandomblob(8)) ORDER BY substr(c, 1, 16) LIMIT 10;
</pre>
<pre>
        -- repeat <i>nUpdate</i> times:
        UPDATE tbl SET b=updateblob(b, ?, ?), c=hex(frandomblob(32)) WHERE a = ?;
      COMMIT;
</pre>

<p>Where <i>nScan</i> and <i>nUpdate</i> are parameters for the test. Four
separate tests are run, with different values for <i>nScan</i> and
<i>nUpdate</i>:

  *  <b>update1</b>: nUpdate=1, nScan=0
  *  <b>update10</b>: nUpdate=10, nScan=0
  *  <b>update1_scan10</b>: nUpdate=1, nScan=10
  *  <b>update10_scan10</b>: nUpdate=10, nScan=10

Prepared statements are used while testing - the SQL compiler is not run during the tests.

<h1 id=results>Test Results</h1>

All tests were run on a 16-core AMD 5950X processor with 32G of memory
using tmpfs for a file-system. Advanced BIOS CPU frequency management features
like "Core Boost", "AMD Cool & Quiet" and "Simultaneous Multi-Threading" are
all disabled.

<h2>Test case update1 - nUpdate=1, nScan=0</h2>
[make_chart update1]

<p>This is a tough test case for bcw2: small, write-heavy, transactions using
simple prepared statements on a database small enough to fit in main memory.
This minimizes the time spent outside COMMIT processing, and bcw2 has to
serialize commit processing. So adding extra threads doesn't improve
transaction throughput as much as in other cases.

<p>For both hctree configurations, each transaction writes to 5 leaf pages of
this database - one to update the main table, two to delete the original index
entries, and two to add the new index entries. In order to write to a page,
hctree loads the page from its original location in the file, modifies it, then
stores it at a new location. With 4KiB pages (configuration "hctree"), this
saturates the memory bus on the test system as throughput approaches 700,000
transactions per second (which would be 14GiB/s in each direction on the 
memory bus - 28GiB/s total), limiting performance.

Configuration "hct1024", which uses 1KiB pages and so does not cause the 
memory bus to become saturated, appears to be CPU limited. If the test system
had more cores, it might scale further.

This <a href="pagetest.c">C program</a> may be used to determine the maximum
rate of page read/write cycles supported by a test system.

<h2>Test case update10 - nUpdate=10, nScan=0</h2>
[make_chart update10]

This chart is similar to update1. Each transaction does 10 times the work, 
and seems to take roughly 10 times as long to run.

Systems bcw2 and hct1024 benefit from lower transaction overhead to increase
maximum updates-per-second when compared to update1. As does hctree at lower
thread counts. At higher thread counts, hctree is subject to the same memory
bus bandwidth limitation as it was in update1.

<h2>Test case update1_scan10 - nUpdate=1, nScan=10</h2>
[make_chart update1_scan10]

This is a good test case for bcw2. That it is read-heavy means that the
fraction of time spent by each transaction in the serialized "COMMIT" command
is lower, and so the system scales better as threads are added. Peak
transaction throughput is similar to update1.

Both hctree and hct1024 scale fine for this test case too.

<h2>Test case update10_scan10 - nUpdate=10, nScan=10</h2>
[make_chart update10_scan10]

<h2>Test case scan10 - nScan=10</h2>
[make_chart scan10]

All systems do well with this read-only test case.

}]


