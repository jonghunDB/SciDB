#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2015-2018 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#
"""This script tests SDB-5145.

Before the fix, a string attribute with length between 128 and 255 would throw an exception.

@author Donghui Zhang

Assumptions:
  - It assumes SciDB is running.
  - It assumes $SCIDB_INSTALL_PATH is set correctly.
"""

import sys
from shared import my_call

def main():
    query = ('build(<v:string>[i=0:0,1,0],\'{0}\')'.format(
             'B<name:string NOT NULL,port:uint16 NOT NULL,instance_id:uint64 NOT NULL,'
             'online_since:string NOT NULL,instance_path:string NOT NULL> [No=0:3,4,0]'))
    print "Use Java version of iquery:"
    my_call(True,  query)

    print "Use C++ version of iquery:"
    my_call(False, query)
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
