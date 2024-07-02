


set G(filename) test.db
set G(nRow)     1000000

set G(nSleep)   10
set G(nSecond)  30

set G(system) [lindex $argv 0]
if {[llength $argv]!=1 || [lsearch {hctree hct1024 bcw2} $G(system)]<0 } {
  puts stderr "Usage $argv0 hctree|bcw2|hct1024"
  exit -1
}

# Setup SQL scripts for each of the 4 test cases:
#
#   update1:         1 update per transaction.
#   update10:        10 updates per transaction.
#   update1_scan10:  1 update, 10 scans per transaction.
#   update10_scan10: 1 update, 10 scans per transaction.
#   scan10         : 10 scans per transaction.
#
foreach {testname nUp nScan} {
  update1          1  0
  update10        10  0
  update1_scan10   1 10
  update10_scan10 10 10
  scan10           0 10
} {
  set update "UPDATE tbl0 SET c=hex(frandomblob(32)) WHERE a=frandomid(${G(nRow)});"
  set scan   "SELECT * FROM tbl0 WHERE substr(c, 1, 16)>=hex(frandomblob(8)) ORDER BY substr(c, 1, 16) LIMIT 10;"

  set body "
    [string repeat $update $nUp]
    [string repeat $scan   $nScan]
  "

  if {$nUp==0} {
    set BEGIN "BEGIN"
  } else {
    set BEGIN "BEGIN CONCURRENT"
  }
  if {$nUp==0} {
    set mutexcommit "COMMIT;"
  } else {
    set mutexcommit ".mutexcommit"
  }

  set G(sql.bcw2.$testname) [subst {
    $BEGIN;
      $body
    $mutexcommit
  }]
  set G(sql.hctree.$testname) [subst {
    $BEGIN;
      $body
    COMMIT;
  }]
  set G(sql.hct1024.$testname) $G(sql.hctree.$testname)
}

puts "CREATE TABLE IF NOT EXISTS result(system, test, nthread, nsecond, data);"

proc setup_database {} {
  global G
  set file $G(filename)
  set nRow $G(nRow)

  file delete -force $file ${file}-data ${file}-pagemap
  if {$G(system)=="hctree"} {
    sqlite3 db file:${file}?hctree=1 -uri 1
  } elseif {$G(system)=="hct1024"} {
    sqlite3 db file:${file}?hctree=1 -uri 1
    db eval { PRAGMA page_size = 1024 }
  } else {
    sqlite3 db $file
    db eval { PRAGMA page_size = 4096 }
  }

  db eval {
    CREATE TABLE tbl0(
        a INTEGER PRIMARY KEY,
        b BLOB,
        c CHAR(64)
    );
    CREATE INDEX tbl0_i1 ON tbl0(substr(c, 1, 16));
    CREATE INDEX tbl0_i2 ON tbl0(substr(c, 2, 16));
    PRAGMA journal_mode = wal2;
  }

  set nRow [expr $nRow]
  db eval BEGIN
  for {set ii 0} {$ii < $nRow} {incr ii} {
    db eval {
      INSERT INTO tbl0 VALUES(NULL, zeroblob(200), hex(randomblob(32)));
    }
  }
  db eval COMMIT

  db close
}

proc run_one_test {nThread testname} {
  global G
  sqlite_thread_test T $G(filename)
  T config -sqlconf {
    PRAGMA mmap_size = 1000000000;
    PRAGMA synchronous = off;
  }

  for {set ii 0 } {$ii<$nThread} {incr ii} {
    T thread $ii $G(sql.$G(system).$testname)
  }

  T configure -nsecond $G(nSecond)
  T configure -nwalpage 4096

  puts "-- launching $nThread threads for $G(nSecond)s - testcase \"$testname\"...."
  T run
  catch { array unset A }
  set data [T result]
  T destroy

  set nTotal 0
  foreach {k v} $data {
    if {[string match *ok $k]} { incr nTotal $v }
  }
  puts "-- $testname, $nThread threads: [expr $nTotal/$G(nSecond)] per second"

  puts "INSERT INTO result(system, test, nthread, nsecond, data) VALUES('$G(system)', '$testname', $nThread, $G(nSecond), '$data');"
}

puts "-- setup database..."
setup_database

#run_one_test 14 update1
#exit

set lThread [list 16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1]
set lTest [list update1 update10 update1_scan10 update10_scan10 scan10]

foreach nThread $lThread {
  foreach testname $lTest {
    puts "-- sleeping $G(nSleep) seconds..."
    after [expr {$G(nSleep) * 1000}]
    run_one_test $nThread $testname
  }
}



