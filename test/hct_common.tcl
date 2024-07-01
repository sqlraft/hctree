



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

proc foreach-perm {var list body {prefix {}}} {
  upvar $var myvar

  set nItem [llength $list]
  if {$nItem==1} {
    set myvar [concat $prefix $list]
    uplevel $body
  } else {
    for {set i 0} {$i < $nItem} {incr i} {
      set item [lindex $list $i]
      set sublist [lreplace $list $i $i] 
      set newprefix [concat $prefix [list $item]]
      uplevel [list foreach-perm $var $sublist $body $newprefix]
    }
  }

}

proc hct_db_contents {db} {
  set ret [list]
  $db eval {
    SELECT name FROM sqlite_schema 
    WHERE name NOT LIKE 'sqlite%' AND type='table'
  } {
    lappend ret "table $name"
    lappend ret {*}[$db eval "SELECT * FROM $name"]
  }
  set ret
}

proc do_perm_test {tn sql} {
  hct_copy_db test.db save.db

  set cid1 [db one {SELECT max(cid) from sqlite_hct_journal}]

  db eval $sql
  set tlist [list]
  db eval { 
    SELECT cid, schema, data, schemacid 
    FROM sqlite_hct_journal WHERE cid>$cid1 ORDER BY cid 
  } {
    lappend tlist $cid
    set T($cid) [list $cid $schema $data $schemacid]
  }

  hct_copy_db save.db test.db2
  sqlite3 db2 test.db2
  set aSnap($cid1) [hct_db_contents db2]
  foreach t $tlist {
    sqlite3_hct_journal_write db2 {*}$T($t)
    set cid [lindex $T($t) 0]
    set aSnap($cid) [hct_db_contents db2]
  }
  db2 close

  foreach-perm p $tlist {
    set testname "$tn.($p)"
    hct_copy_db save.db test.db2
    sqlite3 db2 test.db2

    set nBusy 0
    set iSnap $cid1               ;# Currently visible snapshot for db2

    while {[llength $p]>0} {
      set t [lindex $p 0]
      set p [lrange $p 1 end]

      set res [sqlite3_hct_journal_write db2 {*}$T($t)]

      if {$res=="SQLITE_BUSY"} {
        lappend p $t
        incr nBusy
        if {$nBusy>100} { error "busy loop!" }
      } elseif {$res!="SQLITE_OK"} {
        error $res
      }

      set iNewSnap [sqlite3_hct_journal_snapshot db2]
      if {$iNewSnap<$iSnap} {
        error "snapshot regressed from $iSnap to $iNewSnap"
      }
      set iSnap $iNewSnap

      set contents [hct_db_contents db2]
      if {$contents != $aSnap($iSnap)} {
        error "bad snapshot $iSnap - got $iThis"
      }
    }

    db eval { 
      SELECT name FROM sqlite_schema 
      WHERE type='table' AND name NOT LIKE 'sqlite%'
    } {
      test_dbs_match $testname.$name "SELECT * FROM $name"
    }
  }
}



