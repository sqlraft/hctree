
file delete -force my_follower.db
sqlite3 dbhdl file:my_follower.db?hctree=1 -uri 1
sqlite3_hct_journal_init dbhdl
dbhdl close

hct_testserver T my_follower.db
T configure -follower 1 -syncthreads 8

T job {
  while {1} {
  break
    after 1000
    puts "available snapshot: [sqlite3_hct_journal_snapshot db]"
  }
}

T run
