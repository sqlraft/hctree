

set unused_apis {
  i64 sqlite3BtreeOffset(BtCursor*);
  int sqlite3BtreeLockTable(Btree*, int, u8);
  int sqlite3BtreeSharable(Btree*);
  int sqlite3BtreeConnectionCount(Btree*);

  # Probably need this one...
  int sqlite3BtreeCheckpoint(Btree*, int, int*, int*);
}

set cursor_apis {
  int sqlite3BtreeNext(BtCursor*, int);
  int sqlite3BtreeCursorHasMoved(BtCursor*);
  void sqlite3BtreeClearCursor(BtCursor*);
  int sqlite3BtreeCursorRestore(BtCursor*, int*);
  void sqlite3BtreeCursorHintFlags(BtCursor*, unsigned);
  int sqlite3BtreeCloseCursor(BtCursor*);
  int sqlite3BtreeCursorIsValid(BtCursor*);
  int sqlite3BtreeCursorIsValidNN(BtCursor*);
  i64 sqlite3BtreeIntegerKey(BtCursor*);
  void sqlite3BtreeCursorPin(BtCursor*);
  void sqlite3BtreeCursorUnpin(BtCursor*);
  u32 sqlite3BtreePayloadSize(BtCursor*);
  sqlite3_int64 sqlite3BtreeMaxRecordSize(BtCursor*);
  int sqlite3BtreePayload(BtCursor*, u32, u32, void*);
  int sqlite3BtreePayloadChecked(BtCursor*, u32, u32, void *);
  const void *sqlite3BtreePayloadFetch(BtCursor*, u32*);
  int sqlite3BtreeFirst(BtCursor*, int*);
  int sqlite3BtreeLast(BtCursor*, int*);
  int sqlite3BtreeTableMoveto(BtCursor*, i64, int, int*);
  int sqlite3BtreeIndexMoveto(BtCursor*, UnpackedRecord*, int*);
  void sqlite3BtreeCursorDir(BtCursor*, int);
  int sqlite3BtreeEof(BtCursor*);
  i64 sqlite3BtreeRowCountEst(BtCursor*);
  int sqlite3BtreePrevious(BtCursor*, int);
  int sqlite3BtreeInsert(BtCursor*, const BtreePayload*, int, int);
  int sqlite3BtreeDelete(BtCursor*, u8);
  int sqlite3BtreeIdxDelete(BtCursor*, UnpackedRecord*);
  int sqlite3BtreePutData(BtCursor*, u32, u32, void*);
  void sqlite3BtreeIncrblobCursor(BtCursor*);
  int sqlite3BtreeCursorHasHint(BtCursor*, unsigned int);
  int sqlite3BtreeTransferRow(BtCursor*, BtCursor*, i64);
  int sqlite3BtreeClearTableOfCursor(BtCursor*);
  int sqlite3BtreeCount(sqlite3*, BtCursor*, i64*);
}

set tree_apis {
  int sqlite3BtreeCursor(Btree*, Pgno, int, struct KeyInfo*,BtCursor*);
  sqlite3_uint64 sqlite3BtreeSeekCount(Btree*);
  Pgno sqlite3BtreeLastPage(Btree*);
  int sqlite3BtreeClose(Btree*);
  int sqlite3BtreeSetCacheSize(Btree*, int);
  int sqlite3BtreeSetSpillSize(Btree*, int);
  int sqlite3BtreeSetMmapLimit(Btree*, sqlite3_int64);
  int sqlite3BtreeSetPagerFlags(Btree*, unsigned);
  int sqlite3BtreeSetPageSize(Btree*, int, int, int);
  int sqlite3BtreeGetPageSize(Btree*);
  int sqlite3BtreeGetReserveNoMutex(Btree*);
  int sqlite3BtreeGetRequestedReserve(Btree*);
  Pgno sqlite3BtreeMaxPageCount(Btree*, Pgno);
  int sqlite3BtreeSecureDelete(Btree*, int);
  int sqlite3BtreeSetAutoVacuum(Btree*, int);
  int sqlite3BtreeGetAutoVacuum(Btree*);
  int sqlite3BtreeNewDb(Btree*);
  int sqlite3BtreeBeginTrans(Btree*, int, int*);
  int sqlite3BtreeIncrVacuum(Btree*);
  int sqlite3BtreeCommitPhaseOne(Btree*, const char*);
  int sqlite3BtreeCommitPhaseTwo(Btree*, int);
  int sqlite3BtreeCommit(Btree*);
  int sqlite3BtreeTripAllCursors(Btree*, int, int);
  int sqlite3BtreeRollback(Btree*, int, int);
  int sqlite3BtreeBeginStmt(Btree*, int);
  int sqlite3BtreeSavepoint(Btree*, int, int);
  int sqlite3BtreeCreateTable(Btree*, Pgno*, int);
  int sqlite3BtreeClearTable(Btree*, int, i64*);
  int sqlite3BtreeDropTable(Btree*, int, int*);
  void sqlite3BtreeGetMeta(Btree*, int, u32*);
  int sqlite3BtreeUpdateMeta(Btree*, int, u32);
  int sqlite3BtreePragma(Btree*, char**);
  Pager *sqlite3BtreePager(Btree*);
  const char *sqlite3BtreeGetFilename(Btree*);
  const char *sqlite3BtreeGetJournalname(Btree*);
  int sqlite3BtreeTxnState(Btree*);
  int sqlite3BtreeIsInBackup(Btree*);
  void *sqlite3BtreeSchema(Btree*, int, void(*)(void *));
  int sqlite3BtreeSchemaLocked(Btree*);
  int sqlite3BtreeIsReadonly(Btree*);
  int sqlite3BtreeSetVersion(Btree*, int);
  int sqlite3BtreeIntegrityCheck(sqlite3*, Btree*, Pgno*, Mem*, int, int, int*,char**);
  int sqlite3BtreeCheckpoint(Btree*, int, int *, int *);
  int sqlite3BtreeExclusiveLock(Btree*);
}

set check_for_null_object {
  sqlite3BtreePragma
  sqlite3BtreeTxnState
  sqlite3BtreeTripAllCursors
  sqlite3BtreeSavepoint
  sqlite3BtreeSecureDelete
  sqlite3BtreeCheckpoint
}

# Do not generate a wrapper for any of these functions.
#
set no_wrapper_for {
  sqlite3BtreeCursor
  sqlite3BtreeCursorHasMoved
  sqlite3BtreeCloseCursor
  sqlite3BtreeSeekCount
}

set extra_redefines {
  sqlite3BtreeFakeValidCursor
  sqlite3BtreeCursorSize
  sqlite3BtreeCursorZero
  sqlite3BtreeOpen
}

# Parse the C function signature passed as the only argument and return
# a key-value list containing the following elements:
#
#    return_type
#    name
#    param_type_list
#    param_name_list
#    param_list
#
proc parse_signature {sig} {
  if {[regexp {([^(]*)sqlite3([^(]*)\((.*)\)} $sig -> ret fname plist]==0} {
    error "regexp does not match.."
  }

  set namelist [list a b c d e f g h i j k]
  set idx 0

  set lType [list]
  set lName [list]
  set lBoth [list]

  set seen 0

  foreach type [split $plist ,] {
    set type [string trim $type]
    if {$type=="Btree*" && !$seen} {
      set name p
      set both "Btree *p"
      set seen 1
    } elseif {$type=="BtCursor*" && !$seen} {
      set name p
      set both "BtCursor *p"
      set seen 1
    } elseif { $type=="void(*)(void *)" } {
      set name xFree
      set both "void (*xFree)(void *)"
    } elseif { [string range $type end end]=="*" } {
      set name [lindex $namelist $idx]
      set ptype [string trim [string range $type 0 end-1]]
      set both "$ptype *$name"
      incr idx
    } else {
      set name [lindex $namelist $idx]
      set both "$type $name"
      incr idx
    }

    lappend lType $type
    lappend lName $name
    lappend lBoth $both
  }

  set ret [string trim $ret]
  return [list \
      return_type $ret       \
      name $fname             \
      param_type_list [join $lType ", "] \
      param_name_list [join $lName ", "] \
      param_list [join $lBoth ", "] \
  ]
}

proc mk_btcursor_methods {} {
  set ret "struct BtCursorMethods {\n"
  foreach c [split $::cursor_apis ";"] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]

    append ret "  $A(return_type)(*x$A(name))($A(param_type_list));\n"
  }
  append ret "};\n"
}

proc mk_btree_methods {} {
  set ret "struct BtreeMethods {\n"
  append ret "  BtCursorMethods const *pCsrMethods;\n"
  foreach c [split $::tree_apis ";"] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]

    append ret "  $A(return_type)(*x$A(name))($A(param_type_list));\n"
  }
  append ret "};\n"
}

proc mk_functions {} {
  set ret ""
  foreach c [concat [split $::cursor_apis ";"] [split $::tree_apis ";"]] {

    if {[string trim $c]==""} continue
    array set A [parse_signature $c]

    if {[lsearch $::no_wrapper_for "sqlite3$A(name)"]>=0} continue


    append ret "$A(return_type) sqlite3$A(name)($A(param_list)){\n"
    if {[lsearch $::check_for_null_object "sqlite3$A(name)"]>=0} {
      append ret "  if( p==0 ) return 0;\n"
    }
    if {$A(return_type)=="void"} {
      append ret "  p->pMethods->x$A(name)($A(param_name_list));\n"
    } else {
      append ret "  return p->pMethods->x$A(name)($A(param_name_list));\n"
    }
    append ret "}\n" 
  }
  set ret
}

proc mk_stock_define {} {
  set ret ""
  foreach c [concat [split $::cursor_apis ";"] [split $::tree_apis ";"]] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]
    append ret "#define sqlite3$A(name) sqlite3Stock$A(name)\n"
  }

  foreach r $::extra_redefines {
    set name [string range $r 7 end]
    append ret "#define sqlite3$name sqlite3Stock$name\n"
  }

  set ret
}

proc mk_stock_undef {} {
  set ret ""
  foreach c [concat [split $::cursor_apis ";"] [split $::tree_apis ";"]] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]
    append ret "#undef sqlite3$A(name)\n"
  }

  foreach r $::extra_redefines {
    set name [string range $r 7 end]
    append ret "#undef sqlite3$name\n"
  }

  append ret "#ifndef SQLITE_DEBUG\n"
  append ret "# define sqlite3BtreeSeekCount(X) 0\n"
  append ret "#endif\n"
  set ret
}

proc fix_return_type {in} {
  if {[string range $in end end]=="*"} {
    return $in
  }
  return "$in "
}

proc mk_hct_fwddecl {} {
  set ret ""
  set ret2 ""
  foreach c [split $::tree_apis ";"] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]
    set t [fix_return_type $A(return_type)]
    append ret "${t}sqlite3Hct$A(name)($A(param_type_list));\n"
    append ret2 "${t}sqlite3Stock$A(name)($A(param_type_list));\n"
  }
  foreach c [split $::cursor_apis ";"] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]
    set t [fix_return_type $A(return_type)]
    append ret "${t}sqlite3Hct$A(name)($A(param_type_list));\n"
    append ret2 "${t}sqlite3Stock$A(name)($A(param_type_list));\n"
  }

  set ret3 "BtCursor *sqlite3StockBtreeFakeValidCursor(void);\n"

  return "$ret$ret2$ret3"
}

proc mk_tables {nm} {
  set lc [string tolower $nm]

  set ret ""
  append ret "static const BtCursorMethods ${lc}_btcursor_methods = {\n"
  foreach c [split $::cursor_apis ";"] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]
    append ret "  .x$A(name) = sqlite3${nm}$A(name),\n"
  }
  append ret "};\n"

  append ret "static const BtreeMethods ${lc}_btree_methods = {\n"
  append ret "  .pCsrMethods = &${lc}_btcursor_methods,\n"
  foreach c [split $::tree_apis ";"] {
    if {[string trim $c]==""} continue
    array set A [parse_signature $c]
    append ret "  .x$A(name) = sqlite3${nm}$A(name),\n"
  }
  append ret "};\n\n"

  set ret
}


set d "\n"
append d "/******************************************************************\n"
append d "** GENERATED CODE - DO NOT EDIT!\n"
append d "**\n"
append d "** Code generated by tool/hct_mkbtreewrapper.tcl\n"
append d "*/\n"
append d [mk_btcursor_methods]
append d [mk_btree_methods]
append d [mk_functions]
append d [mk_tables Hct]
append d [mk_tables Stock]
append d "/*\n"
append d "** END OF GENERATED CODE\n"
append d "******************************************************************/\n"

set srcdir [file join [file dirname [info script]] .. src]
set fd [open [file join $srcdir btwrapper.c] r]
set z [read $fd]
close $fd

set start {/[*] BEGIN_HCT_MKBTREEWRAPPER_TCL_CODE [*]/}
set end   {/[*] END_HCT_MKBTREEWRAPPER_TCL_CODE [*]/}
regexp "(.*$start).*($end.*)" $z -> one two

puts "editing src/btwrapper.c..."
set fd [open [file join $srcdir btwrapper.c] w]
puts -nonewline $fd "$one$d$two"
close $fd


puts "writing src/btreeModules.h..."
set fd [open [file join $srcdir btreeModules.h] w]
puts $fd [mk_hct_fwddecl]
close $fd

puts "writing src/btreeDefine.h..."
set fd [open [file join $srcdir btreeDefine.h] w]
puts $fd [mk_stock_define]
close $fd

puts "writing src/btreeUndef.h..."
set fd [open [file join $srcdir btreeUndef.h] w]
puts $fd [mk_stock_undef]
close $fd



