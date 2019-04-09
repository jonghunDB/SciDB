:
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014-2018 SciDB, Inc.
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

# nyse_loader - invoke loadpipe.py to get our trading data into SciDB
#
# The format string 'tsv:p' tells the load() operator to expect
# tab-separated value input per http://dataprotocols.org/linear-tsv/ ,
# but with the pipe character `|' as the field delimiter.  This is
# essentially the TAQ format.

set -x

nc -d -k -l 8080 | loadpipe.py -vvv -s nyse_flat -A nyse_day --format='tsv:p'