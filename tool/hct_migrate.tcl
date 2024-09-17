
#-------------------------------------------------------------------------
# Migration tool to convert an SQLite database to hctree format
# using multiple threads.
#

proc usage {} {
  puts stderr "Usage: $argv0 ?SWITCHES? <source> <destination>"
  puts stderr ""
  puts stderr "   --mbperjob MB (default 128)"
  puts stderr "   --jobs NJOB (default 12)"
  exit -1
}

set G(-mbperjob) 128
set G(-jobs)      12

# Process any switches
for {set i 0} {$i < [llength $argv]-2} {incr i 2} {
  set x [lindex $argv $i]
  set y [lindex $argv [expr $i+1]]

  if {[string is integer -strict $y]==0} {
    error "option requires integer argument: $x"
  }

  # If there are two leading "-" characters, trim one off.
  if {[string range $x 0 1]=="--"} { set x [string range $x 1 end] }

  set nX [string length $x]
  if {$nX>=2 && [string compare -length $nX $x "-mbperjob"]==0} {
    set G(-mbperjob) $y
  } elseif {$nX>=2 && [string compare -length $nX $x "-jobs"]==0} {
    set G(-jobs) $y
  } else {
    usage
  }
}

set G(src)  [lindex $argv end-1]
set G(dest) [lindex $argv end]


# Only divide up a b-tree if it is at least this many nodes from root to leaf.
set NMINDEPTH 3


set G(divkeys) 1024

if {[file exists $G(dest)]} {
  puts stderr "$G(dest) already exists"
  exit -1
}

proc lshuffle {lIn} {
return $lIn
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

# Argument aData is a blob. This command decodes the value of the SQLite
# varint stored at offset iOff of that blob and returns the value. It
# throws an exception if the varint appears to be malformed.
#
proc decode_sqlite_varint {aData iOff} {

  # Figure out how many bytes of data are available. If the varint appears
  # to be longer than this, it is corrupt - throw an exception.
  set nData [expr [string length $aData] - $iOff]
  if {$nData>9} {set nData 9}

  # Decode any bytes that may be required for decoding.
  #
  binary scan [string range $aData $iOff [expr $iOff+$nData-1]] c* lByte

  set ret 0
  set i 0
  foreach b $lByte {
    set ret [expr ($ret << 7) + ($b & 0x7F)]
    incr i
    if {($b & 0x80)==0 || $i==9} break
    if {$i>=$nData} { error "malformed SQLite varint: $lByte" }
  }

  return $ret
}

proc decode_2byte_int {aData iOff} {
  binary scan [string range $aData $iOff end] S val
  expr {$val & 0xFFFF}
}

proc decode_4byte_int {aData iOff} {
  binary scan [string range $aData $iOff end] I val
  expr {$val & 0xFFFFFFFF}
}

# Argument aPg is a blob containing an SQLite b-tree page. This command 
# returns the number of cells on the page.
#
proc btree_page_ncell {aPg} {
  decode_2byte_int $aPg 3
}

proc btree_page_nchild {aPg} {
  binary scan $aPg c b0
  set ret 0
  if {$b0==0x02 || $b0==0x05} {
    set ret [expr [btree_page_ncell $aPg]+1]
  }
  return $ret
}

proc btree_page_child {aPg iChild} {
  set nChild [btree_page_nchild $aPg]
  if {$iChild==($nChild-1)} {
    set ret [decode_4byte_int $aPg 8]
  } else {
    set iOff [decode_2byte_int $aPg [expr 12 + $iChild*2]]
    set ret [decode_4byte_int $aPg $iOff]
  }
  return $ret
}

# Argument aPg is a blob containing an SQLite b-tree page. This command
# returns, as an integer, the number of bytes of content stored on the
# page and its overflow pages.
#
proc btree_data_on_page {aPg} {

  binary scan $aPg c b0

  switch -- $b0 {
    2 { ;# Table b-tree interior cell
      return 0
    }
    5 { ;# Index b-tree interior cell
      set iHdr 12
    }
    10 { ;# Index b-tree leaf cell
      set iHdr 8
    }
    13 { ;# Table b-tree leaf cell
      set iHdr 8
    }

    default {
      error "corrupt btree page b0=$b0"
    }
  }

  set ret 0

  set nCell [btree_page_ncell $aPg]
  for {set i 0} {$i<$nCell} {incr i} {
    set iOff [decode_2byte_int $aPg [expr ($i*2 + $iHdr)]]
    if {$iHdr==12} {incr iOff 4}
    set nByte [decode_sqlite_varint $aPg $iOff]
    incr ret $nByte
  }

  return $ret
}

# Argument db is a database handle open on the source db. This command
# returns a blob of data containing a copy of page number $pgno.
#
proc fetch_page {db pgno} {
  $db one { SELECT data FROM sqlite_dbpage WHERE pgno=$pgno }
}

# Argument db is a database handle open on the source db. This command
# attempts to estimate the size of the b-tree with root page iRoot within
# that database.
#
proc btree_size {db iRoot} {
  # Make a random walk to a leaf...
  #
  # In total, how much data is on the leaf - including overflow pages?
  set nPath 10

  set lnData [list]
  set lnPage [list]

  set nTotal 0

  for {set i 0} {$i<$nPath} {incr i} {
    set pgno $iRoot
    set nLeaf 1
    set nData 0
    while {1} {
      set aPg [fetch_page src $pgno]
      set nChild [btree_page_nchild $aPg]
      if {$nChild==0} break
      set nLeaf [expr $nLeaf * $nChild]
      set iChild [expr int(rand() * $nChild)]
      set pgno [btree_page_child $aPg $iChild]
    }

    set nData [btree_data_on_page $aPg]
    set nEst [expr $nLeaf * $nData]
    puts "estimated data size: [expr $nEst/(1024*1024)] MB ($nLeaf leaves)"
    incr nTotal $nEst
  }

  set ret [expr $nTotal / $nPath]
  puts "estimate for $iRoot: [expr $nTotal / $nPath]"
  return $ret
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

# Create the hctree destination database and copy the source schema into
# it.
proc create_destination_schema {} {
  global G
  set nItem 0                     ;# Number of schema items created

  sqlite3 src $G(src)

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
  load_dbdata src
  
  # This loop runs one iteration for each non-virtual, non-schema table 
  # or index in the database being migrated.
  src eval {
    SELECT 
        name, 
        (
          CASE WHEN name=tbl_name THEN name ELSE tbl_name||'.'||name END
        ) AS display,
        rootpage,
        row_number() OVER () AS iImp
    FROM sqlite_schema WHERE type='index' OR (
        type = 'table' AND
        name NOT LIKE 'sqlite_%' AND 
        sql NOT LIKE 'create virtual%'
    );
  } {
    set bIntkey 0
    set bPrimaryKey 0

    # Find the depth of the current b-tree. A b-tree that consists of a 
    # root page only has depth=1. Or, if the leaves are direct children of 
    # the root page, depth=2. And so on.
    set nBtreeDepth [btree_depth src $rootpage]

    if {$nBtreeDepth>=3} {
      # If the b-tree is at least 3 levels deep, then it may be broken up
      # into multiple INSERT statements. Estimate the total size in bytes 
      # of all data stored in the b-tree. This will be used to determine
      # how many INSERT statements to divide the table contents into.
      set nBtreeSize [btree_size src $rootpage]
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
      set ct "CREATE TABLE imp$iImp ($cols);"
      append ct " -- $display"
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
          puts "WARNING: Ignoring $nReject/$nKey keys for $display"
        }
      }

      set nDiv [expr $nBtreeSize / ($G(-mbperjob)*1024*1024)]
      if {$nDiv<=0} {set nDiv 1}
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

sqlite3 dest "file:$G(dest)?hctree=1" -uri 1
#dest eval { PRAGMA hct_ndbfile = 16; }
set n [create_destination_schema]
puts "Created destination schema with $n items..."

set plan [plan_migration]
set nTab    [llength [lindex $plan 0]]
set nInsert [llength [lindex $plan 1]]
puts "Created plan with $nInsert inserts on $nTab imposter tables..."

set nJob $G(-jobs)
if {$nJob>$nInsert} { set nJob $nInsert }

if 0 {
set fsize [expr 13*1024*1024*1024]
set msize [expr $fsize / 512]
puts [time {
fallocate $G(dest) $fsize
fallocate $G(dest)-pagemap $msize
}]
}

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

dest close




