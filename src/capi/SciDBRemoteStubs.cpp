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

/*
 * @file SciDBRemoteStubs.cpp
 *
 * @brief provide a dummy version of addTiming() for use by the client library
 *        which has no use for the perfTime information at this time
 */

#include <util/PerfTime.h>

namespace scidb
{

/**
 * see declarations in PerfTime.h
 * these are all dummy versions that do nothing on the client side
 * because it does not monitor where it spends its time
 * [if it wants to, it needs to define these]
 */
double perfTimeGetElapsed() {return 0.0;}
double perfTimeGetCPU()     {return 0.0;}
void perfTimeAdd(const perfTimeWait_t tw, const double sec) {;}

WaitTimerParams::WaitTimerParams(perfTimeWait_t tw_, uint64_t sampleInterval_, uint64_t* unsampledCount_) {;}
ScopedWaitTimer::ScopedWaitTimer(perfTimeWait_t tw) : _wtp(PTW_UNTIMED, 1, NULL) {;}
ScopedWaitTimer::ScopedWaitTimer(const WaitTimerParams& wtp) : _wtp(wtp) {;}
ScopedWaitTimer::~ScopedWaitTimer() {;}

}
