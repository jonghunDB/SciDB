########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2018 SciDB, Inc.
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
########################################

"""
This module contains shared Python submodules used in other SciDB scripts.
"""


class AppError(Exception):
    """An exception class that multiple modules in scidblib may use."""
    pass


import counter                      # noqa F401
import iquery_client                # noqa F401
import pgpass_updater               # noqa F401
import util                         # noqa F401
import scidb_afl as afl             # noqa F401
import scidb_control as control     # noqa F401
import scidb_math as math           # noqa F401
import scidb_progress as progress   # noqa F401
import scidb_psf as psf             # noqa F401
import scidb_schema as schema       # noqa F401
import statistics                   # noqa F401
