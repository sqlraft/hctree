
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
  puts -nonewline <th>$t
}
puts ""

foreach test $lTest {
  puts <tr><td>$test
  foreach t $lThread {
    puts -nonewline <td>
    db eval {
      SELECT (ntrans/nsecond) || ' (' || round((100.0*nbusy)/ntrans,1) || '%)' || ' <b>' 
        || cast((100 * ntrans/(nsecond*(CASE WHEN typeof(nthread)=='integer' THEN nthread ELSE 1 END) )) / (
          SELECT avg(ntrans/nsecond) FROM tests WHERE name=$test AND nthread=1
        ) AS int) || '%</b>' 
        AS line
      FROM tests WHERE name=$test AND nthread=$t
    } {
      puts -nonewline [string map [list " " &nbsp;] $line]
      puts -nonewline <br>
    }
  }
  puts ""
}
puts </table>




