
#-------------------------------------------------------------------------
# Migration tool to convert an SQLite database to hctree format
# using multiple threads.
#

if {[llength $argv]!=2} {
  puts stderr "Usage: $argv0 <source> <destination>"
  exit -1
}

# Divide large tables and indexes into this many jobs.
set NJOB 16

# Only divide up a b-tree if it is at least this many nodes from root to leaf.
set NMINDEPTH 3

set G(src)  [lindex $argv 0]
set G(dest) [lindex $argv 1]

if {[file exists $G(dest)]} {
  puts stderr "$G(dest) already exists"
  exit -1
}

proc lshuffle {lIn} {
  set lInter [list]
  foreach i $lIn {
    lappend lInter [list [expr rand()] $i]
  }
  set lOut [list]
  foreach i [lsort -index 0 $lInter] {
    lappend lOut [lindex $i 1]
  }
  set lOut
}

# Load the dbdata.so extension into the supplied handle.
#
proc load_dbdata {db} {
  $db enable_load_extension 1
  $db eval {
    SELECT load_extension('./dbdata');
  }
}

# Return the number of pages from root to leaf of the b-tree with root
# page $iRoot in database $db.
#
proc btree_depth {db iRoot} {
  set pg $iRoot
  set nPath 0
  $db transaction {
    while {$pg!=""} {
      incr nPath
      set pg [$db one {
          SELECT min(child) AS pg FROM sqlite_dbptr WHERE pgno=$pg
      }]
    }
  }
  return $nPath
}

# Create the hctree destination database and copy the source schema into
# it.
proc create_destination_schema {} {
  global G
  set nItem 0                     ;# Number of schema items created

  sqlite3 src $G(src)
  sqlite3 dest "file:$G(dest)?hctree=1" -uri 1
  #sqlite3 dest $G(dest)

  dest transaction {
    src eval {
      SELECT sql, sql LIKE 'create virtual%' AS virtual FROM sqlite_schema 
      WHERE sql!='' AND name NOT LIKE 'sqlite_%'
    } {
      if {$virtual} {
        puts stderr "Error - source schema contains virtual tables"
        exit -1
      }
      incr nItem
      dest eval $sql
    }
  }

  src close
  dest close

  return $nItem
}

# Plan the migration. The outputs of this process are as follows:
#
#   * A series of CREATE TABLE statements to be used to create
#     imposter tables for each table and index in the schema. Along
#     with each CREATE TABLE are two root page numbers - one in the
#     source database and one in the destination.
#
#   * A series of INSERT statements that can be used to copy data
#     between the source and destination databases.
#
# The actual data returned is a list of two elements, one for each
# of the bullet-points above.
#
# The first element is itself a list, each element of which is a
# three element list containing the CREATE TABLE, the source db root
# page, and the destination db root page, in that order.
#
# The second element is a list of INSERT statements.
#
#
proc plan_migration {} {
  global G

  # The two elements of the return value.
  set lCreateTable [list]
  set lInsertStmt  [list]

  sqlite3 src $G(src)
  sqlite3 dest $G(dest)
  load_dbdata src
  
  set iImp 0
  src eval {
    SELECT type, name, tbl_name, rootpage, sql FROM sqlite_schema WHERE 
        sql!='' AND 
        name NOT LIKE 'sqlite_%' AND 
        sql NOT LIKE 'create virtual%' AND
        (type = 'table' OR type = 'index');
  } {
    incr iImp
    set lPk [list]
    set lCol [list]
  
    set bIntkey 0
    set bPrimaryKey 0

    set zDisplayname $tbl_name
    if {$name!=$tbl_name} {
      append zDisplayname ".$name"
    }
  
    src eval {
        SELECT seqno, coll FROM pragma_index_xinfo($name)
    } {
      lappend lPk "c$seqno"
      if {$coll!="BINARY"} {
        lappend lCol "c$seqno COLLATE $coll"
      } else {
        lappend lCol "c$seqno"
      }
    }
  
    if {[llength $lCol]>0} {
      # An index or WITHOUT ROWID table.
      set cols [join $lCol { ,}]
      set pk [join $lPk { ,}]
      set ct "CREATE TABLE imp$iImp ($cols, PRIMARY KEY($pk)) WITHOUT ROWID;"
      append ct " -- $zDisplayname"
      set bPrimaryKey 1
    } else {
      # A regular rowid table.
  
      # Find table's INTEGER PRIMARY KEY if it has one. If it does, set $pkcid
      # to the cid of the column. If it does not, set pkcid to "".
      set pkcid [src one {
        SELECT cid FROM pragma_table_info($name)
        WHERE pk AND 1=(
          SELECT count(*) FROM pragma_table_info($name) WHERE pk
        )
      }]
  
      src eval {
        SELECT cid FROM pragma_table_info($name)
      } {
        if {$cid==$pkcid} {
          lappend lCol "c$cid INTEGER PRIMARY KEY"
          set bPrimaryKey 1
        } else {
          lappend lCol "c$cid"
        }
        lappend lPk "c$cid"
      }
  
      set cols [join $lCol {, }]
      set ct "CREATE TABLE imp$iImp ($cols);"
      append ct " -- $zDisplayname"
      set bIntkey 1
    }
  
    set nDepth [btree_depth src $rootpage]
    append ct " (depth=$nDepth)"

    set destroot [dest one {
      SELECT rootpage FROM sqlite_schema WHERE name=$name
    }]
    lappend lCreateTable [list $ct $rootpage $destroot]

    set lKey [list]
    if {$nDepth >= $::NMINDEPTH} {
      if {$bIntkey} {
        src eval {
          WITH pages(path, pgno) AS (
            VALUES('/', $rootpage)
            UNION ALL
            SELECT format('/%03d/', row_number() OVER ()), child FROM sqlite_dbptr
              WHERE pgno = $rootpage
          )
    
          SELECT format('%s%03d', p.path, d.cell) AS path, value 
          FROM sqlite_dbdata('main', 1) AS d, pages p
          WHERE d.pgno=p.pgno AND d.field=-1
          ORDER BY 1
        } {
          lappend lKey $value
        }
  
      } else {
        set nReject 0
        src eval {
          WITH pages(path, pgno) AS (
            VALUES('/', $rootpage)
            UNION ALL
            SELECT format('/%03d/', row_number() OVER ()), child FROM sqlite_dbptr
              WHERE pgno = $rootpage
          )
    
          SELECT 
            format('%s%03d', p.path, d.cell) AS path, 
            '(' || group_concat(quote(value), ',') || ')' AS value,
            max(value IS NULL OR typeof(value)=='real') AS bnull
          FROM sqlite_dbdata AS d, pages p
          WHERE d.pgno=p.pgno
          GROUP BY p.path, d.cell
          ORDER BY 1
        } {
          if {$bnull==0} {
            lappend lKey $value
          } else {
            # Ignoring this key as it contains either a NULL or a real value.
            incr nReject
          }
        }

        set nKey [expr [llength $lKey] + $nReject]
        if {($nReject*5)>=$nKey} {
          puts "WARNING: Ignoring $nReject/$nKey keys for $zDisplayname"
        }
      }
  
      set nJob $::NJOB
      set nKey [llength $lKey]
      set nRegPerJob [expr (($nKey+1+$nJob-1) / $nJob)]
      set lKey2 [list]
      for {set ii [expr $nRegPerJob-1]} {$ii < $nKey} {incr ii $nRegPerJob} {
        lappend lKey2 [lindex $lKey $ii]
      }
      set lKey $lKey2
      unset lKey2
    }
  
    if {$bIntkey} {
      set p "INSERT INTO main.imp$iImp"
      if {$bPrimaryKey} {
        set prefix "$p SELECT * FROM src.imp$iImp"
      } else {
        set prefix "$p (rowid,[join $lPk ,]) SELECT rowid, * FROM src.imp$iImp"
      }
      set pk rowid
    } else {
      set prefix "INSERT INTO main.imp$iImp SELECT * FROM src.imp$iImp"
      set pk "([join $lPk ,])"
    }
  
    if {[llength $lKey]==0} {
      lappend lInsertStmt $prefix
    } else {
      lappend lInsertStmt "$prefix WHERE $pk < [lindex $lKey 0]"
      for {set ii 0} {$ii < [llength $lKey]-1} {incr ii} {
        set one [lindex $lKey $ii]
        set two [lindex $lKey $ii+1]
        lappend lInsertStmt "$prefix WHERE $pk >= $one AND $pk < $two"
      }
      lappend lInsertStmt "$prefix WHERE $pk >= [lindex $lKey end]"
    } 
  }

  src close

  list $lCreateTable $lInsertStmt
}

set n [create_destination_schema]
puts "Created destination schema with $n items..."

set plan [plan_migration]
set nTab    [llength [lindex $plan 0]]
set nInsert [llength [lindex $plan 1]]
puts "Created plan with $nInsert inserts on $nTab imposter tables..."

set nJob $NJOB
if {$nJob>$nInsert} { set nJob $nInsert }

foreach {lCreateTable lInsertStmt} $plan {}
if 0 {
  foreach c $lCreateTable { puts $c }
  foreach ins $lInsertStmt { puts $ins }
} else {
  foreach c $lCreateTable { puts "TABLE: [lindex $c 0]" }
  sqlite_migrate M $G(src) $G(dest) $nJob
  foreach ct $lCreateTable {
    M imposter {*}$ct
  }
  foreach ins $lInsertStmt {
    M insert $ins
  }

  puts "Running...."
  M run
  puts "DONE!"
  foreach {k v} [M stats] { puts "$k  $v" }
}




