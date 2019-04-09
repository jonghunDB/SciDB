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
 * @file PerfTime.cpp
 *
 * @brief Implementation of PerfTimeLog.h, for scidb only (clients do not have this kind of logging)
 *
 */

#include <atomic>
#include <cmath>

#include <util/PerfTime.h>
#include <util/PerfTimeLog.h>
#include <query/Query.h>
#include <system/Config.h>

namespace scidb
{
log4cxx::LoggerPtr perfLogger = log4cxx::Logger::getLogger("scidb.perftime");

double perfTimeGetElapsed()  // no exceptions please
{
    try {
        struct timeval tv;
        auto result = ::gettimeofday(&tv, NULL);
        SCIDB_ASSERT(result==0);  // never fails in practice
        return double(tv.tv_sec) + double(tv.tv_usec) * 1.0e-6;         // slow, initial implementation
    }
    catch (...){;} // timing should not change the code path above

    return NAN; // NaN will keep things going while still invalidating the timing
}

double perfTimeGetCPU()  // no exceptions please
{
    try {
        struct rusage rUsage;
        auto result = ::getrusage(RUSAGE_THREAD, &rUsage);
        SCIDB_ASSERT(result==0);  // never fails in practice

        struct timeval cpuSum;
        timeradd(&rUsage.ru_utime, &rUsage.ru_stime, &cpuSum);      // linux macro
        return double(rUsage.ru_utime.tv_sec) +
               double(rUsage.ru_utime.tv_usec) * 1.0e-6 +
               double(rUsage.ru_stime.tv_sec) +
               double(rUsage.ru_stime.tv_usec) * 1.0e-6;
    }
    catch (...){;} // timing should not change the code path above
    return NAN; // NaN will keep things going while still invalidating the timing
}

// NOCHECKIN: Marty's duplication of stuff to review
//            File a ticket on this, see email to Marty 5/26/2016 prior to
//            removing this message
//            AND DON'T TOUCH THE FOLLOWING CODE

uint64_t perfTimeGetElapsedInMicroseconds()  // no exceptions please
{
    try {
        struct timeval tv;
        auto result = ::gettimeofday(&tv, NULL);
        SCIDB_ASSERT(result==0);  // never fails in practice

        return (static_cast<uint64_t>(tv.tv_sec) * 1000000) + static_cast<uint64_t>(tv.tv_usec);
    }
    catch (...){;} // timing should not change the code path above

    return 0;
}

uint64_t perfTimeGetCpuInMicroseconds(int who)  // no exceptions please
{
    try {
        struct rusage rUsage;
        auto result = ::getrusage(who, &rUsage);
        SCIDB_ASSERT(result==0);  // never fails in practice

        return (static_cast<uint64_t>(rUsage.ru_utime.tv_sec) * 1000000) +
                static_cast<uint64_t>(rUsage.ru_utime.tv_usec) +
               (static_cast<uint64_t>(rUsage.ru_stime.tv_sec) * 1000000) +
                static_cast<uint64_t>(rUsage.ru_stime.tv_usec);
    }
    catch (...){;} // timing should not change the code path above

    return 0;
}

struct PTC_TableEntry {
  const char* tc_str;
  const char* tc_message;
};

const PTC_TableEntry PTC_Table[PTW_NUM] =
{
#define PTC_ENTRY(C, CMESSAGE) [C] = {#C, CMESSAGE}
    PTC_TABLE_ENTRIES
#undef PTC_ENTRY
};

const char* tcName(const perfTimeCategory_t tc)
{
    SCIDB_ASSERT(tc < PTC_NUM);
    return PTC_Table[tc].tc_str ;
}


struct PTW_TableEntry {
  perfTimeWait_t     tw;
  const char*        tw_str;
  perfTimeCategory_t tc;
  const char*        tw_message;
};

// gives names to wait numbers (PTWs) and maps thems to more general categories (PTCs)
const PTW_TableEntry PTW_Table[PTW_NUM] =
{
#define PTW_ENTRY(W, C, W_MSG) [W] = {W, #W, C, W_MSG}
    PTW_TABLE_ENTRIES
#undef PTW_ENTRY
};


perfTimeCategory_t twToCategory(const perfTimeWait_t tw)
{
    SCIDB_ASSERT(tw < PTW_NUM);
    return PTW_Table[tw].tc;
}

const char* twName(const perfTimeWait_t tw)
{
    SCIDB_ASSERT(tw < PTW_NUM);
    return PTW_Table[tw].tw_str;
}


void logNoqueryWaits(const perfTimeWait_t tw, const double sec)
{
    LOG4CXX_DEBUG(perfLogger, "perfTimeAdd: didn't log " << twName(tw) << " " << sec);
}


void perfTimeAdd(const perfTimeWait_t tw, const double sec, uint64_t nestingDepth)
{
    SCIDB_ASSERT(tw < PTW_NUM);
    const int DEBUG_EXTRA_VERBOSE=0;

    std::shared_ptr<Query> query = Query::getQueryPerThread();
    if(query) {
        if(twToCategory(tw) == PTC_ZERO) {
            LOG4CXX_WARN(perfLogger, "perfTimeAdd: unexpected wait ignored: " << twName(tw)
                                     << " at nestingDepth: " << nestingDepth
                                     << ", " << sec << " s " << query->getQueryID());
            return;
        }
        query->perfTimeAdd(tw, sec);
    } else if (DEBUG_EXTRA_VERBOSE) {
        logNoqueryWaits(tw, sec);
    }
}

WaitTimerParams::WaitTimerParams(perfTimeWait_t tw_, uint64_t sampleInterval_, uint64_t* unsampledCount_)
:
    tw(tw_),
    sampleInterval(sampleInterval_),
    unsampledCount(unsampledCount_)
{;}

bool ScopedWaitTimer::_isWaitTimingEnabled = false;

void ScopedWaitTimer::adjustWaitTimingEnabled()
{
    _isWaitTimingEnabled = Config::getInstance()->getOption<int>(CONFIG_PERF_WAIT_TIMING);
    if (_isWaitTimingEnabled) {
        LOG4CXX_INFO(perfLogger, "The SciDB process does do perf wait timing.");
    } else {
        LOG4CXX_INFO(perfLogger, "The SciDB process does not do perf wait timing.");
    }
}

bool ScopedWaitTimer::isWaitTimingEnabled()
{
    return _isWaitTimingEnabled;
}


// if not thread_local, this would need to be std::atomic_uint_fast32_t
thread_local uint64_t ScopedWaitTimer::_nestingDepth(0);

ScopedWaitTimer::ScopedWaitTimer(const WaitTimerParams& wtp) // no exceptions please
:
    _wtp(wtp),
    _secStartElapsed(0),
    _secStartCPU(0),
    _isEnabled(wtp.tw != PTW_UNTIMED && isWaitTimingEnabled())
{
    init();
}

ScopedWaitTimer::ScopedWaitTimer(perfTimeWait_t tw)  // no exceptions please
:
    _wtp(WaitTimerParams(tw, 1/*sampleInterval*/, NULL/*unsampleCount*/)),
    _secStartElapsed(0),
    _secStartCPU(0),
    _isEnabled(tw != PTW_UNTIMED && isWaitTimingEnabled())
{
    init();
}

void ScopedWaitTimer::init() // no exceptions please
{
    if(!_isEnabled) { return; }

    SCIDB_ASSERT(_wtp.tw < PTW_NUM); // (except this one that prevents memory scribbling)
    SCIDB_ASSERT( _wtp.sampleInterval == 1 ||   // if sample interval not 1
                  _wtp.unsampledCount);         // must provide an unsampledCount (ptr)

    try {
        static int NESTING_ALLOWED=1 ; // change to 0 to debug unintended nesting
                                       // TODO: this may become a ctor parameter
                                       //   so that "top-level" ScopedWaitTimer won't be
                                       //   accidentally nested, but its use inside
                                       //   semaphore, event, etc can be more permissive
        if (!NESTING_ALLOWED) {
            assert(_nestingDepth==0);       // check that there is only one object at a time per thread
                                            // to avoid nesting these on the stack, which would
                                            // result in over-measurement.
        }
        _nestingDepth++ ;
        if(_nestingDepth==1) {   // only the outermost nesting does the timing in dtor
                                 // so this is a very helpful performance optimization

            if (_wtp.unsampledCount) {     // when present
                ++*(_wtp.unsampledCount);  // ctor increments (does not reset)
            }
            if(_wtp.sampleInterval == 1 ||                       // time all or
               *(_wtp.unsampledCount) >= _wtp.sampleInterval) {  // time this one?
                _secStartElapsed = perfTimeGetElapsed(); // vdso call
                _secStartCPU = perfTimeGetCPU();         // kernel trap :(
            }
        }
    }
    catch (...){;} // timing should not change the code path above
}

ScopedWaitTimer::~ScopedWaitTimer()  // no exceptions please
{
    if(!_isEnabled) { return; }

    // note: surround CPU mesurement with elapsed measurement
    //       so that secBlocked has less of a chance
    //       of being negative.  This does introduce
    //       an positive bias of elapsed vs cpu time
    //       but tends to keep secBlocked from going negative
    //       as much, which is a little less astonishing
    //       This is subject to change as we learn more
    //       about the clocks involved and their relative
    //       precisions
    try {
        assert(_nestingDepth > 0);

        if(_nestingDepth==1) {   // only the outermost nesting does the timing
            if(_wtp.sampleInterval == 1 ||                       // time all or
               *(_wtp.unsampledCount) >= _wtp.sampleInterval) {  // time this one?
                if (_wtp.unsampledCount) {    // when present
                    *(_wtp.unsampledCount)=0; // dtor resets (does not increment)
                }
                auto secCPU =     perfTimeGetCPU()     - _secStartCPU;     // kernel call
                auto secElapsed = perfTimeGetElapsed() - _secStartElapsed; // could be user land
                auto secBlocked =   secElapsed - secCPU ;
                perfTimeAdd(_wtp.tw, secBlocked*double(_wtp.sampleInterval), _nestingDepth);
            }
        }
        _nestingDepth-- ;
    }
    catch (...){;} // timing should not change the code path above
}

//
// printing
//
const int PERF_TIME_SUBTOTALS = 0;  // a debug convenience only, 0 on checkin
const int PERF_TIME_PERCENTS = 0;   // a debug convenience only, 0 on checkin

static void perfTimeLogDetail(int64_t usecElapsedStart, std::atomic_int_fast64_t twUsecs[PTW_NUM],
                              const Query& query); // forward
static void perfTimeLogPrintKey(); // forward


/**
 *
 * for additional logging see [[[below]]]
 *
 */
void perfTimeLog(int64_t usecElapsedStart, std::atomic_int_fast64_t twUsecs[PTW_NUM],
                 const Query& query)
{
    static uint64_t _perfTimeLogCount = 0;
    const uint LINES_LOGGED_PER_KEY_PRINTED= 500;

    if (_perfTimeLogCount++ % LINES_LOGGED_PER_KEY_PRINTED == 0) { perfTimeLogPrintKey(); }

    int64_t usecNow = int64_t(perfTimeGetElapsed()*1.0e6);
    int64_t usecTotal = usecNow - usecElapsedStart;

    // aggregate the detailed wait measurements _tw[PTW_*]
    // into the re-grouped categories posTcUsecs[PTC_*]
    int64_t posTcUsecs[PTC_NUM];
    memset(posTcUsecs, 0, sizeof(posTcUsecs));

    int64_t usecBias = 0;
    for (perfTimeWait_t tw=PTW_FIRST; tw< PTW_NUM; tw = perfTimeWait_t(tw+1)) {
        int64_t uSecs = twUsecs[tw];
        // because CPUtime and elapsedTime clocks are not synchronized
        // some instances of uSec can be slightly negative.
        // so we add a compensation, but warn if it is excessive in any
        // PTW, or if the total fudging is excessive
        if (uSecs >= 0) {
            perfTimeCategory_t tc = twToCategory(tw);
            SCIDB_ASSERT(tc < PTC_NUM) ;
            posTcUsecs[tc] += uSecs;
        } else {
            // uSecs negative
            if (uSecs < -5000) { // more than 5msec conpensation
                LOG4CXX_WARN(perfLogger, "QueryTiming: twUsecs["<<tw<<"]="<<uSecs<<" ignored");
            }
            usecBias -= uSecs; // sum the "negative errors" as a positive value
        }
    }
    if(usecBias > 100000) { // more than 100msec total bias
        LOG4CXX_WARN(perfLogger, "QueryTiming: total usecBias = " << usecBias);
    }

    int64_t usecCategorized = 0;
    for (unsigned i=PTC_FIRST; i< PTC_NUM; i++) {
        if (i != PTC_ACTIVE && i != PTC_IGNORE) {   // goal is for others to sum to active
            usecCategorized += posTcUsecs[i];
        }
    }

    // Trace because this is 'developer's debug'p message that will only be enabled
    // for a developer to evaluate how well this code works in practice
    LOG4CXX_TRACE(perfLogger, "QueryTiming: PTC_ACTIVE: "
                               << posTcUsecs[PTC_ACTIVE] << " vs sum of all others: " << usecCategorized);

    // because of usecBias, beware that the difference below could be negative
    int64_t usecUncategorized = std::max(posTcUsecs[PTC_ACTIVE] - usecCategorized, int64_t(0));

    std::stringstream ssStats;
    ssStats << std::fixed << std::setprecision(6)
            <<  "TOT "  << double(usecTotal) * 1.0e-6
            << " ACT "  << double(posTcUsecs[PTC_ACTIVE]) * 1.0e-6;
    if(PERF_TIME_PERCENTS) {
        ssStats << " ACT% " << double(posTcUsecs[PTC_ACTIVE]) / double(usecTotal) * 100.0;   // percent
    }
    if(PERF_TIME_SUBTOTALS) {
        ssStats << " Cat "  << double(usecCategorized) * 1.0e-6;
        if(PERF_TIME_PERCENTS) {
            ssStats << " Cat% " << double(usecCategorized) / double(posTcUsecs[PTC_ACTIVE]) * 100.0;  // percent
            // note that due to usecBias, this could be slightly more than 100%
        }
    }

    ssStats << " CPU "   << double(posTcUsecs[PTC_CPU]) * 1.0e-6
            << " wPG "   << double(posTcUsecs[PTC_PG]) * 1.0e-6
            << " wFSr "  << double(posTcUsecs[PTC_FS_RD]) * 1.0e-6
            << " wFSw "  << double(posTcUsecs[PTC_FS_WR]) * 1.0e-6
            << " wFSws " << double(posTcUsecs[PTC_FS_WR_SYNC]) * 1.0e-6
            << " wFSf "  << double(posTcUsecs[PTC_FS_FL]) * 1.0e-6
            << " wSMld " << double(posTcUsecs[PTC_SM_LOAD]) * 1.0e-6
            << " wBFrd " << double(posTcUsecs[PTC_BF_RD]) * 1.0e-6
            << " wSMcm " << double(posTcUsecs[PTC_SM_CMEM]) * 1.0e-6
            << " wREP "  << double(posTcUsecs[PTC_REP])*1.0e-6
            << " wNETr " << double(posTcUsecs[PTC_NET_RCV]) * 1.0e-6
            << " wNETs " << double(posTcUsecs[PTC_NET_SND])* 1.0e-6
            << " wNETrr "<< double(posTcUsecs[PTC_NET_RCV_R])* 1.0e-6
            << " wNETrc "<< double(posTcUsecs[PTC_NET_RCV_C])* 1.0e-6
            << " wSGr "  << double(posTcUsecs[PTC_SG_RCV]) * 1.0e-6
            << " wSGb "  << double(posTcUsecs[PTC_SG_BAR]) * 1.0e-6
            << " wBAR "  << double(posTcUsecs[PTC_BAR])*1.0e-6
            << " wJsrt"  << double(posTcUsecs[PTC_SORT_JOB])*1.0e-6
            << " wEXT "  << double(posTcUsecs[PTC_EXT]) * 1.0e-6
            << " wSEMo " << double(posTcUsecs[PTC_SEMA]) * 1.0e-6
            << " wEVo "  << double(posTcUsecs[PTC_EVENT]) * 1.0e-6
            << " wLTCH " << double(posTcUsecs[PTC_LATCH]) * 1.0e-6
            << " wRare " << double(posTcUsecs[PTC_RARE]) * 1.0e-6
            << " wZero " << double(posTcUsecs[PTC_ZERO]) * 1.0e-6;

    if(PERF_TIME_SUBTOTALS) {
        ssStats << " unCat "  << double(usecUncategorized) * 1.0e-6;
    }

    // this percentage is always printed, too easy to make mistakes without it on casual observation
    // its last so that those who don't want it can more easily exclude it
    ssStats << std::fixed << std::setprecision(1)
            << " OTH% " << double(usecUncategorized) / double(posTcUsecs[PTC_ACTIVE]) * 100.0;  // percent

    LOG4CXX_INFO (perfLogger, "QueryTiming " << query.getInstanceID() << " " << query.getQueryID()
                                       << " " << ssStats.str() << " " << query.getQueryString());


    perfTimeLogDetail(usecTotal, twUsecs, query);
}

/**
 * logs detailed information about the values logged by perfTimeLog
 */

static void perfTimeLogPrintKey()
{
    for(size_t tc= 0; tc < PTC_NUM; ++tc) {
        LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: " << PTC_Table[tc].tc_str
                                              << " : " << PTC_Table[tc].tc_message);
    }
}

/**
 * to enable the additional logging available from this routine, not usually enabled,
 * add a line like this one:
 *
 * log4j.scidb.perftime.detail=TRACE
 *
 * to scidb's log4cxx config file (e.g.  /opt/scidb/<VER>/share/scidb/log4cxx.properties)
 * For additonal info goto logging.apache.org/log4cxx and search for "configuration file".
 */

log4cxx::LoggerPtr perfLoggerDetail = log4cxx::Logger::getLogger("scidb.perftime.detail");

static void perfTimeLogDetail(int64_t usecTotal, std::atomic_int_fast64_t twUsecs[PTW_NUM],
                       const Query& query)
{
    std::stringstream ssStats;
    ssStats << std::fixed << std::setprecision(6)
        <<  "TOT "  << double(usecTotal) * 1.0e-6;
        for(size_t tw= 0; tw < PTW_NUM; ++tw) {
            int64_t uSecs = twUsecs[tw];
            ssStats << " ";
            ssStats << twName(perfTimeWait_t(tw)) << " " << double(uSecs) * 1.0e-6 ;
        }
    // the secondary purpose of this function
    // NOCHECKIN . at checkin must be _TRACE
    LOG4CXX_TRACE (perfLogger, "QueryTimingDetail " << query.getInstanceID() << " " << query.getQueryID() << " " << ssStats.str() << " " << query.getQueryString());

}



} // namespace
