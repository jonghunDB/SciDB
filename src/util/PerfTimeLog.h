/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2018 SciDB, Inc.
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
 * @file PerfTimeLog.h
 *
 * @brief time logging function used by SciDB, not by clients
 */

#ifndef PERF_TIME_LOG_H_
#define PERF_TIME_LOG_H_

#include <query/Query.h>

namespace scidb
{
/**
 * function called by Query destructor to log per-Query timing results
 * @parm usecElapsedStart - Query::_usecElapsedStart at destruction
 * @parm twUsecs - Query::_twUsecs[] at destruction
 * @parm query - Query::~Query()'s *this
 */
void perfTimeLog(int64_t usecElapsedStart, std::atomic_int_fast64_t twUsecs[PTW_NUM],
                 const Query& query);


} //namespace
#endif // PERF_TIME_LOG_H_
