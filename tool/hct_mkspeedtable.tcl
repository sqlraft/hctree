package require sqlite3

#-------------------------------------------------------------------------
proc execsql_pp {sql {db db}} {
  set nCol 0
  $db eval $sql A {
    if {$nCol==0} {
      set nCol [llength $A(*)]
      foreach c $A(*) { 
        set aWidth($c) [string length $c] 
        lappend data $c
      }
    }
    foreach c $A(*) { 
      set n [string length $A($c)]
      if {$n > $aWidth($c)} {
        set aWidth($c) $n
      }
      lappend data $A($c)
    }
  }
  if {$nCol>0} {
    set nTotal 0
    foreach e [array names aWidth] { incr nTotal $aWidth($e) }
    incr nTotal [expr ($nCol-1) * 3]
    incr nTotal 4

    set fmt ""
    foreach c $A(*) { 
      lappend fmt "% -$aWidth($c)s"
    }
    set fmt "| [join $fmt { | }] |"
    
    puts [string repeat - $nTotal]
    for {set i 0} {$i < [llength $data]} {incr i $nCol} {
      set vals [lrange $data $i [expr $i+$nCol-1]]
      puts [format $fmt {*}$vals]
      if {$i==0} { puts [string repeat - $nTotal] }
    }
    puts [string repeat - $nTotal]
  }
}
#-------------------------------------------------------------------------

sqlite3 db ""
db eval {
  CREATE TABLE tests(
    name, nthread, nsecond, ntrans, nbusy
  );
}

foreach f $argv {
  set fd [open $f]
  while {![eof $fd]} {
    set line [gets $fd]
    if {[string match {INSERT*} $line]} {
      db eval $line
    }
  }
}

set lThread [db eval {
  SELECT DISTINCT nthread FROM tests ORDER BY typeof(nthread) DESC, nthread
}]
set lTest [db eval {
  SELECT DISTINCT name FROM tests ORDER BY name NOT LIKE '%sep%'
}]

puts -nonewline "<table border=1 cellpadding=10>"
puts -nonewline "<tr><th>Test"
foreach t $lThread {
  set title "HCT $t threads"
  if {$t=="stock"} {set title "Stock SQLite"}
  if {$t==1} {set title "HCT 1 thread"}
  puts -nonewline <th>$title
}
puts ""

foreach test $lTest {
  puts <tr><td>$test
  foreach t $lThread {
    set tcount $t
    if {$tcount=="stock"} {set tcount 1}
    puts -nonewline "<td nowrap>"
    db eval {
      WITH res(ntotal, npersecond, npersecondcpu, percentbusy) AS (
        SELECT ntrans, 
               ntrans/nsecond, 
               ntrans/(nsecond * $tcount),
               round((100.0 * nbusy) /ntrans, 1) || '%'
        FROM tests
        WHERE name=$test AND nthread=$t
      ),
      baserate(navg) AS (
        SELECT avg(ntrans/nsecond) FROM tests WHERE nthread=1 AND name=$test
      )

      SELECT npersecond 
          || '(' || percentbusy || ') ' 
          || '<b>' || CAST((100 * npersecond / (navg*$tcount))+0.5 AS integer) || '%</b>'  AS line
      FROM res, baserate;
    } {
      puts -nonewline $line
      puts -nonewline <br>
    }
  }
  puts ""
}
puts </table>
