
file delete -force my_test.db
sqlite3 dbhdl file:my_test.db?hctree=1 -uri 1
sqlite3_hct_journal_init dbhdl
sqlite3_hct_journal_setmode dbhdl LEADER
dbhdl eval {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
}
dbhdl close

hct_testserver T my_test.db
T configure -seconds 120

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

# Configure the testserver object with 6 jobs, all running the same script.
for {set iJob 0} {$iJob < 6} {incr iJob} {
  T job $script
}

T job {
  while {[hct_testserver_timeout]==0} {
    after 1000
    puts "max cid: [db one {SELECT max(cid) FROM sqlite_hct_journal}]"
  }
}

T run

