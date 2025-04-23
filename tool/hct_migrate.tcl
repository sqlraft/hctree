
#-------------------------------------------------------------------------
# Migration tool to convert an SQLite database to hctree format
# using multiple threads.
#

proc usage {} {
  puts stderr "Usage: $::argv0 ?SWITCHES? <source> <destination>"
  puts stderr ""
  puts stderr "   --jobs NJOB (default 12)"
  puts stderr "   --only TABLE (default \"\")"
  puts stderr ""
  exit -1
}

# Default values for the two options.
#
set G(-jobs)           12
set G(-only)           ""

if {[llength $argv]<2} usage

# Process command line arguments.
#
for {set i 0} {$i < [llength $argv]-2} {incr i} {
  set x [lindex $argv $i]

  # If there are two leading "-" characters, trim one off.
  if {[string range $x 0 1]=="--"} { set x [string range $x 1 end] }

  set nX [string length $x]
  if {$nX>=2 && [string compare -length $nX $x "-jobs"]==0} {
    if {$i == [llength $argv]-3} { usage }
    incr i
    set val [lindex $argv $i]
    if {[string is integer -strict $val]==0} { usage }
    set G(-jobs) $val
  } elseif {$nX>=2 && [string compare -length $nX $x "-only"]==0} {
    if {$i == [llength $argv]-3} { usage }
    incr i
    set G(-only) [lindex $argv $i]
  } else {
    usage
  }

}
set G(src)  [lindex $argv end-1]
set G(dest) [lindex $argv end]

# Do not run if the destination database already exists on disk.
#
if {[file exists $G(dest)]} {
  puts stderr "$G(dest) already exists"
  exit -1
}


# Argument db is a database handle open on the source db. This command 
# attempts to find if the rowid table named $name has an explicit INTEGER
# PRIMARY KEY column. If it does, then the index of the column is returned.
# Otherwise, if the table does not have an IPK column, the value returned 
# is -1.
#
proc find_ipk_column {db name} {
  $db one {
    SELECT coalesce(cid, -1) FROM pragma_table_info($name)
    WHERE pk AND (type='integer' COLLATE nocase) AND 1=(
        SELECT count(*) FROM pragma_table_info($name) WHERE pk
    );
  }
}

# Plan the migration. The outputs of this process is a list containing
# one element for each table or index in the source database. Each element
# is itself a list, consisting of:
#
#   * A source db root page number for the table or index,
#
#   * A unique table name of the form "imp<integer>".
#
#   * A "CREATE TABLE imp<integer>" statement to create a table with 
#     equivalent structure to the table or index on disk.
#
proc plan_migration {} {
  global G

  # Build up the return value in this variable. 
  set lRet [list]

  # This loop runs one iteration for each non-virtual, non-schema table 
  # or index in the database being migrated.
  set onlytbl $G(-only)
  set rc [catch {
    set srcdata [src eval {
      SELECT 
          name, 
          (
            CASE WHEN name=tbl_name THEN name ELSE tbl_name||'.'||name END
          ) AS display,
          rootpage,
          row_number() OVER () AS iImp
      FROM sqlite_schema WHERE ($onlytbl=tbl_name OR $onlytbl='') AND (
          type='index' OR (
              type = 'table' AND
              sql NOT LIKE 'create virtual%'
          )
      )
    }]
  } msg]

  if {$rc} {
    set exrc [sqlite3_extended_errcode src]
    puts stderr "ERROR querying source db schema: $msg ($exrc)"
    error $msg
  }

  foreach {name display rootpage iImp} $srcdata {
    set bIntkey 0
    set bPrimaryKey 0

    # This block sets:
    #
    #     $lCol
    #     $lColCollate
    #
    # If this is an index or WITHOUT ROWID table, set lCol and lColCollate to 
    # a list of the column names used by the imposter table. lColCollate 
    # differs from lCol only in that it includes "COLLATE <collation>" 
    # clauses for any columns that use something other than BINARY collation 
    # for text values.
    #
    # If this is a regular rowid table, leave lCol and lColCollate set to 
    # empty lists.
    set lColCollate [list]
    set lCol [list]
    set lColDir [list]
    src eval {
        SELECT seqno, coll, desc FROM pragma_index_xinfo($name)
    } {
      lappend lCol "c$seqno"
      if {$coll!="BINARY"} {
        lappend lColCollate "c$seqno COLLATE $coll"
      } else {
        lappend lColCollate "c$seqno"
      }
      if {$desc} {
        lappend lColDir "c$seqno DESC"
      } else {
        lappend lColDir "c$seqno"
      }
    }
  
    # This block populates the following variables:
    #
    #   $cols
    #   $bPrimaryKey
    #   $bIntkey
    #   $ct
    #
    if {[llength $lColCollate]>0} {
      # An index or WITHOUT ROWID table.
      set cols [join $lColCollate { ,}]
      set pk [join $lColDir { ,}]
      set ct "CREATE TABLE imp$iImp ($cols, PRIMARY KEY($pk)) WITHOUT ROWID;"
      set ct "($cols, PRIMARY KEY($pk)) WITHOUT ROWID;"
      append ct " -- $display"
      set bPrimaryKey 1
    } else {
      # A regular rowid table.
  
      # Find table's INTEGER PRIMARY KEY if it has one. If it does, set $pkcid
      # to the cid of the column. If it does not, set pkcid to -1.
      set pkcid [find_ipk_column src $name]
  
      src eval {
        SELECT cid FROM pragma_table_info($name)
      } {
        if {$cid==$pkcid} {
          lappend lColCollate "c$cid INTEGER PRIMARY KEY"
          set bPrimaryKey 1
        } else {
          lappend lColCollate "c$cid"
        }
        lappend lCol "c$cid"
      }
  
      set cols [join $lColCollate {, }]
      set ct "($cols);"
      append ct " -- $display"
      set bIntkey 1
    }

    set ct1 "CREATE TABLE real$iImp $ct"
    set ct2 "CREATE TABLE imp$iImp $ct"
    unset ct
  
    # Set variable zPrefix to an "INSERT INTO ... SELECT" statement to copy
    # data between the source and destination imposter tables. This is called
    # a prefix because a WHERE clause may be appended to it.
    #
    if {$bIntkey} {
      set p "INSERT INTO main.imp$iImp"
      if {$bPrimaryKey} {
        set zPrefix "$p SELECT * FROM src.imp$iImp"
      } else {
        set zPrefix "$p (rowid,[join $lCol ,]) SELECT rowid,* FROM src.imp$iImp"
      }
    } else {
      set zPrefix "INSERT INTO main.imp$iImp SELECT * FROM src.imp$iImp"
    }

    lappend lRet [list $rootpage "real$iImp" "imp$iImp" $zPrefix $ct1 $ct2"]
  }

  set lRet
}

sqlite_log_to_stdout
# sqlite_thread_test_config

sqlite3 dest "file:$G(dest)?hctree=1" -uri 1
sqlite3 src $G(src)

dest eval { CREATE TABLE xyz(x); DROP TABLE xyz; }
  
set plan [plan_migration]

sqlite_migrate M $G(src) $G(dest) $G(-jobs)
foreach p $plan {
  foreach {pgno tname1 tname2 insert ct1 ct2} $p { }
  if 0 {
    puts "PGNO/TNAME: $pgno/$tname1/$tname2"
    puts "INSERT:     $insert"
    puts "CT1:        $ct1"
    puts "CT2:        $ct2"
  }
  # set insert "INSERT INTO $tname2 SELECT * FROM src.$tname2"
  M copy $pgno $tname1 $insert $ct1 $ct2

  set src_root_to_name($pgno) $tname1
}

puts "Populating database...."
M run
M destroy

puts "Fixing schema..."
dest eval { SELECT rootpage, name FROM sqlite_schema } {
  set dest_name_to_root($name) $rootpage
}
dest trans {
  dest eval {
    PRAGMA writable_schema = 1;
    DELETE FROM sqlite_schema WHERE 1;
  }
  src eval {
    SELECT rowid, type, name, tbl_name, rootpage, sql FROM sqlite_schema
  } {
    if {[info exists src_root_to_name($rootpage)]} {
      set rootpage $dest_name_to_root($src_root_to_name($rootpage))
    } elseif {$rootpage!=0} {
      continue;
    }

    dest eval {
      INSERT INTO sqlite_schema(rowid, type, name, tbl_name, rootpage, sql)
        VALUES($rowid, $type, $name, $tbl_name, $rootpage, $sql);
    }
  }
}


dest close
src close
puts "DONE!"



