

sqlite3 db ""
foreach a $argv {
  set fd [open $a]
  set txt [read $fd]
  close $fd
  db eval $txt
}

set G(width) 10.0
set G(height) 1.2
set G(border.bcw2)   lightgreen
set G(border.hctree) orange
set G(color.bcw2)    lightgreen
set G(color.hctree)  orange

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
    SELECT system, test, nthread, nsecond, data FROM result WHERE test=$testcase} {
    set nOk 0
    set nBusy 0
  
    array unset A 
    array set A $data
    foreach k [array names A] {
      foreach {t s} [split $k ,] {}
      if {$s=="ok"}   { incr nOk $A($k) }
      if {$s=="busy"} { incr nBusy $A($k) }
    }
    if {$nthread==16 && $test=="update1" && $system=="hctree"} {
      puts stderr "nOk=$nOk"
    }
  
    incr R($system,$nthread,ok) $nOk
    incr R($system,$nthread,busy) $nBusy
    incr R($system,$nthread,total) [expr $nOk + $nBusy]
    incr T($system,$nthread) $nsecond
  }
    if {$test=="update1"} {
      puts stderr "update1: $R(hctree,16,ok)"
      puts stderr "nsec: $T(hctree,16)"
    }

  foreach k [array name R] {
    foreach {sys nt topic} [split $k ,] {}
    set R($k) [expr $R($k) / $T($sys,$nt)]
  }

  set max $R(hctree,16,total)
  set scale [expr $G(height) / $max]

  sputs {
    <center><verbatim type=pikchr>
    boxwid=0.18
    movewid=0.1
  }
  
  sputs {
    box color none fill none width 35%
  }
  for {set thread 1} {$thread <= 16} {incr thread} {
    set nTrans $R(bcw2,$thread,ok)
    set K1 "[expr int($nTrans/1000)]K"
    if {$nTrans<10000} {set K1 $nTrans}
    set S1 [expr $nTrans * $scale]
    set nTrans $R(hctree,$thread,ok)
    set K2 "[expr int($nTrans/1000)]K"
    if {$nTrans<10000} {set K2 $nTrans}
    set S2 [expr $nTrans * $scale]
  
    sputs {
      box color $G(border.bcw2) fill $G(color.bcw2) height $S1 with sw at last box.se
      text "$K1" small small with s at last box.n fill grey
      box color $G(border.hctree) fill $G(color.hctree) height $S2 with sw at last box.se
      text "$K2" small small with s at last box.n fill grey
      text "$thread" with n at last box.sw
      box color none fill none width 35% with sw at last box.se
    }
  }
  
  sputs {
    line color grey from first box.sw to last box.se
    text "Threads:" with ne at last line.w
    VERT: line color grey from last line.sw up $G(height)
    box width 0.5 height 0.2 color $G(border.hctree) fill $G(color.hctree) with nw at VERT.n + (0.1,0.0)
    text "hctree" with n at previous.n
    box width 0.5 height 0.2 color $G(border.bcw2) fill $G(color.bcw2) with n at last box.s + (0.0,-0.1)
    text "bcw2" with n at previous.n
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

<p>Performance is compared to stock SQLite modified with the begin-concurrent
and wal2 patches (system "bcw2"). This is to show that single-threaded
performance is similar to stock SQLite, and that multi-threaded performance is,
for some cases at least, already better than is possible with SQLite.

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

<p>The SQLite/begin-concurrent/wal2 - bcw2 - tests use code from the
begin-concurrent-pnu-wal2 branch. They are configured as follows:

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
     mutex-free locking patch has been applied.

<h1 id=results>Test Results</h1>

All tests were run on a 16-core AMD 5950X processor with 32G of memory
using tmpfs for a file-system. Advanced BIOS CPU frequency management features
like "Core Boost", "AMD Cool & Quiet" and "Simultaneous Multi-Threading" are
all disabled.

<h2>Test case update1 - nUpdate=1, nScan=0</h2>
[make_chart update1]

<h2>Test case update10 - nUpdate=10, nScan=0</h2>
[make_chart update10]

<h2>Test case update1_scan10 - nUpdate=1, nScan=10</h2>
[make_chart update1_scan10]

<h2>Test case update1 - nUpdate=10, nScan=10</h2>
[make_chart update10_scan10]

}]


