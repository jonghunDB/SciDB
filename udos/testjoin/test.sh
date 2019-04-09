#!/bin/bash
make clean
make

sudo cp /home/jh/scidbsource/udos/testjoin/libtest_join.so /opt/scidb/18.1/lib/scidb/plugins

scidb.py stopall mydb
scidb.py startall mydb

iquery -aq "load_library('test_join')"; > /dev/null 2>&1
iquery -aq "test_join(A,B);" > /dev/null 2>&1
