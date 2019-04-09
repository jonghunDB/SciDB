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

if (NOT WITHOUT_SERVER)
    set (monitor_src
        InstanceStats.cpp
        MonitorCommunicator.cpp
        MonitorConfig.cpp
        QueryStats.cpp
    )

    file(GLOB monitor_include "*.h")

    add_library(monitor_lib STATIC ${monitor_src} ${monitor_include})
    target_link_libraries(monitor_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES})
    target_link_libraries(monitor_lib network_lib)
endif()
