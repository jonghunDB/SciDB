/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2018-2018 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/**
 * @file Backtrace.cpp
 * @brief Write backtraces to SciDB log file
 */

#include <util/Backtrace.h>
#include <util/Platform.h>

#include <cassert>
#include <execinfo.h>
#include <log4cxx/logger.h>

using namespace std;

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.backtrace"));
bool didWarn = false;

}

namespace scidb {

void logBacktrace(const char* tag, int depth)
{
    string prefix;
    if (tag) {
        prefix = tag;
        prefix += ": ";
    }
    if (depth <= 0) {
        depth = BT_DEPTH;
    }

    if (isDebug()) {
        void *buffer[depth];
        int nptr = ::backtrace(buffer, depth);
        assert(nptr <= depth);
        char **strings;
        strings = ::backtrace_symbols(buffer, nptr);
        if (strings == nullptr) {
            for (int i = 0; i < nptr; ++i) {
                LOG4CXX_INFO(logger, prefix << '[' << i << "]: " << hex << buffer[i] << dec);
            }
            return;
        }

        for (int i = 0; i < nptr; ++i) {
            LOG4CXX_INFO(logger, prefix << '[' << i << "]: " << strings[i]);
        }
        ::free(strings);
    } else if (!didWarn) {
        LOG4CXX_WARN(logger, prefix << "Backtrace call(s) still present in non-Debug build!");
        didWarn = true;         // Locks?  We've heard of 'em...  ;-)
    }
}

} // namespace scidb
