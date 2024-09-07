
#-------------------------------------------------------------------------
# Migration tool to convert an SQLite database to hctree format
# using multiple threads.
#

if {[llength $argv]!=2} {
  puts stderr "Usage: $argv0 <source> <destination>"
  exit -1
}

# $NJOB is the number of threads to use. $NDIV is the maximum number of
# INSERT statements to divide populating a single table or index into.
set NJOB 16
set NDIV 16

# Only divide up a b-tree if it is at least this many nodes from root to leaf.
set NMINDEPTH 3

set G(src)  [lindex $argv 0]
set G(dest) [lindex $argv 1]

set G(divkeys) 1024

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

# This command inserts the blocks of divider data into the destination 
# database. Argument $ct contains the imposter CREATE TABLE statement and
# the root page numbers in the source and destination db, in the following
# format:
#
#     CREATE-TABLE SOURCE-ROOT-PGNO DEST-ROOT-PGNO
#
# Paramter $lInsert is the list of INSERT statements that will be used to 
# populate the table.
#
proc insert_dividers {ct lInsert} {
  global G

  # Open a new database handle on the destination and attach the source db.
  sqlite3 db $G(dest)
  db eval "ATTACH '$G(src)' AS src"

  # Set up the IMPOSTER table on both the source and destination
  #
  foreach {zSql iSrc iDest} $ct {}
  sqlite_imposter db main 1 $iDest
  db eval $zSql
  sqlite_imposter db main 0 0
  sqlite_imposter db src 1 $iSrc
  db eval $zSql
  sqlite_imposter db src 0 0

  sqlite_migrate_mode db 1
  set lRet [lrange $lInsert 0 0]
  foreach i [lrange $lInsert 1 end] {
    db eval "$i LIMIT $G(divkeys)"
    lappend lRet "$i LIMIT -1 OFFSET $G(divkeys)"
  }
  sqlite_migrate_mode db 0

  db close
  return $lRet
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
proc plan_migration {} {
  global G

  # The two elements of the return value.
  set lCreateTable [list]
  set lInsertStmt  [list]

  sqlite3 src $G(src)
  sqlite3 dest $G(dest)
  load_dbdata src
  
  # This loop runs one iteration for each non-virtual, non-schema table 
  # or index in the database being migrated.
  src eval {
    SELECT 
        type, name, tbl_name, rootpage, sql,
        row_number() OVER () AS iImp
    FROM sqlite_schema WHERE 
        sql!='' AND 
        name NOT LIKE 'sqlite_%' AND 
        sql NOT LIKE 'create virtual%' AND
        (type = 'table' OR type = 'index');
  } {
    set bIntkey 0
    set bPrimaryKey 0

    # Set zDisplayname to the name to use for this schema object when printing
    # messages for users to read. For a regular table, this is just the table
    # name. For an index "$table_name.$index_name".
    set zDisplayname $tbl_name
    if {$name!=$tbl_name} {
      append zDisplayname ".$name"
    }
  
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
    src eval {
        SELECT seqno, coll FROM pragma_index_xinfo($name)
    } {
      lappend lCol "c$seqno"
      if {$coll!="BINARY"} {
        lappend lColCollate "c$seqno COLLATE $coll"
      } else {
        lappend lColCollate "c$seqno"
      }
    }
  
    # This block populates the following variables:
    #
    if {[llength $lColCollate]>0} {
      # An index or WITHOUT ROWID table.
      set cols [join $lColCollate { ,}]
      set pk [join $lCol { ,}]
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
          lappend lColCollate "c$cid INTEGER PRIMARY KEY"
          set bPrimaryKey 1
        } else {
          lappend lColCollate "c$cid"
        }
        lappend lCol "c$cid"
      }
  
      set cols [join $lColCollate {, }]
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
            SELECT format('/%03d/', row_number() OVER () -1), child 
            FROM sqlite_dbptr
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
            SELECT format('/%03d/', row_number() OVER ()-1), child FROM sqlite_dbptr
              WHERE pgno = $rootpage
          )
    
          SELECT 
            format('%s%03d', p.path, d.cell+1) AS path, 
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

      set nDiv $::NDIV
      set nKey [llength $lKey]
      set nRegPerJob [expr (($nKey+1+$nDiv-1) / $nDiv)]
      set lKey2 [list]
      for {set ii [expr $nRegPerJob-1]} {$ii < $nKey} {incr ii $nRegPerJob} {
        lappend lKey2 [lindex $lKey $ii]
      }
      set lKey $lKey2
      unset lKey2
    }
  
    # By this point lKey is set to a list of the divider keys
    if {$bIntkey} {
      set p "INSERT INTO main.imp$iImp"
      if {$bPrimaryKey} {
        set prefix "$p SELECT * FROM src.imp$iImp"
      } else {
        set prefix "$p (rowid,[join $lCol ,]) SELECT rowid, * FROM src.imp$iImp"
      }
      set pk rowid
    } else {
      set prefix "INSERT INTO main.imp$iImp SELECT * FROM src.imp$iImp"
      set pk "([join $lCol ,])"
    }
  
    set lInsert [list]
    if {[llength $lKey]==0} {
      lappend lInsert $prefix
    } else {
      lappend lInsert "$prefix WHERE $pk < [lindex $lKey 0]"
      for {set ii 0} {$ii < [llength $lKey]-1} {incr ii} {
        set one [lindex $lKey $ii]
        set two [lindex $lKey $ii+1]
        lappend lInsert "$prefix WHERE $pk >= $one AND $pk < $two"
      }
      lappend lInsert "$prefix WHERE $pk >= [lindex $lKey end]"
    } 

    set lInsert [insert_dividers [list $ct $rootpage $destroot] $lInsert]
    set lInsertStmt [concat $lInsertStmt $lInsert]
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
  foreach ins [lshuffle $lInsertStmt] {
    M insert $ins
  }

  puts "Running...."
  M run
  puts "DONE!"
  foreach {k v} [M stats] { puts "$k  $v" }
}




