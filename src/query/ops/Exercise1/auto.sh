#!/bin/bash

iquery -aq "unload_library('_ProgressiveTopK')"
scidb.py stopall mydb
scidb.py startall mydb
iquery -aq "load_library('_ProgressiveTopK')"
#iquery -a
