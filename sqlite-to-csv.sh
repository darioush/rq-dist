#! /bin/sh
sqlite3 result.db <<EOF
.headers on
.mode csv
.output result.csv
select * from testselection;
.exit
EOF
