



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

