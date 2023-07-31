



proc show_db_entries {} {
  execsql_pp {
    SELECT slot, pgno, ikey, tid, rangetid, rangeoldpg, child, ovfl, record 
      FROM hctentry, hctpgmap 
      WHERE slot>=33 AND logical_in_use AND pgno=value;
  }
}

proc show_db_page {pgno} {
  execsql_pp "
    SELECT * FROM hctentry WHERE pgno=$pgno
  "
}

proc catch_hct_journal_init {db} {
  set rc [sqlite3_hct_journal_init $db]
  if {$rc!="SQLITE_OK"} {
    return [list 1 [sqlite3_errmsg $db]]
  }
  return "0 {}"
}

proc test_dbs_match {tn sql} {
  uplevel [list do_execsql_test $tn $sql [db2 eval $sql]]
}

proc hct_delete_db {name} {
  forcedelete $name
  foreach f [glob -nocomplain ${name}-*] {
    forcedelete $f
  }
}

proc hct_copy_db {from to} {
  hct_delete_db $to

  file copy -force $from $to
  foreach f [glob -nocomplain ${from}-*] {
    set fto "$to-[string range $f [string length $from]+1 end]"
    file copy -force $f $fto
    #puts "copying $f to $fto"
  }
}



