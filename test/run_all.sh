#!/usr/bin/bash 

./run.sh 1 -R T1WKT -S T2WKT -a APRIL -p -q intersect
./run.sh 2 -R T1WKT -S T2WKT -a APRIL -p -q intersect


./run.sh 1 -R T1WKT -S T2WKT -q find_relation
./run.sh 2 -R T1WKT -S T2WKT -q find_relation