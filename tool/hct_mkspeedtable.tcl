
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

set lThread [db eval {SELECT DISTINCT nthread FROM tests}]
set lTest [db eval {SELECT DISTINCT name FROM tests}]

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
      SELECT (ntrans/nsecond) || ' (' || (nbusy/nsecond) || ')' || ' ' 
        || cast((100 * ntrans/nsecond) / (
          SELECT avg(ntrans/nsecond) FROM tests WHERE name=$test AND nthread=1
        ) AS int) || '%' 
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




