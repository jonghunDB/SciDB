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
 * @file Mutex.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Mutex class for synchronization
 */

#ifndef MUTEX_H_
#define MUTEX_H_

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <pthread.h>

#include <sys/time.h>             // linux specific
#include <sys/resource.h>         // linux specific

#include <util/Platform.h>
#include <util/PerfTime.h>

#include <system/Exceptions.h>


namespace scidb
{

class Event;

class Mutex
{
friend class Event;
friend class Semaphore;
friend class Condition;
private:
 class PAttrEraser
 {
 public:
 PAttrEraser(pthread_mutexattr_t *attrPtr) : _attrPtr(attrPtr)
     {
         assert(_attrPtr!=NULL);
     }
     ~PAttrEraser()
     {
         pthread_mutexattr_destroy(_attrPtr);
     }
 private:
     pthread_mutexattr_t *_attrPtr;
 };

    pthread_mutex_t _mutex;
    static const int _mutexType = PTHREAD_MUTEX_RECURSIVE;

  public:
    void checkForDeadlock() {
        assert(_mutex.__data.__count == 1);
    }

    Mutex()
    {
        pthread_mutexattr_t mutexAttr;
        if (int rc = pthread_mutexattr_init(&mutexAttr)) {
            std::stringstream ss;
            ss << "pthread_mutexattr_init errno="<<rc;
            throw std::runtime_error(ss.str());
        }
        PAttrEraser onStack(&mutexAttr);

        if (int rc = pthread_mutexattr_settype(&mutexAttr, _mutexType)) {
            std::stringstream ss;
            ss << "pthread_mutexattr_settype errno="<<rc;
            throw std::runtime_error(ss.str());
        }
        if (int rc = pthread_mutex_init(&_mutex, &mutexAttr)) {
            std::stringstream ss;
            ss << "pthread_mutex_init errno="<<rc;
            throw std::runtime_error(ss.str());
        }
    }

    ~Mutex()
    {
        if (int rc = pthread_mutex_destroy(&_mutex)) {
            std::stringstream ss;
            ss << "pthread_mutex_destroy errno="<<rc;
            throw std::runtime_error(ss.str());
        }
    }

    void lock(const  WaitTimerParams& wtp)
    {
        {
            ScopedWaitTimer timer(wtp);     // destruction updates the timing of tw
                                           // note: we are purposely timing only the time until lock
                                           // *acquisition*, time passing until unlock() counts
                                           // as cpu time on the calling thread
            if (pthread_mutex_lock(&_mutex)) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_lock";
            }
        }
    }

    // alternate version for the common case
    void lock(perfTimeWait_t tw)
    {
        {
            ScopedWaitTimer timer(tw);     // destruction updates the timing of tw
                                           // note: we are purposely timing only the time until lock
                                           // *acquisition*, time passing until unlock() counts
                                           // as cpu time on the calling thread
            if (int rc = pthread_mutex_lock(&_mutex)) {
                std::stringstream ss;
                ss << "pthread_mutex_lock errno="<<rc;
                throw std::runtime_error(ss.str());
            }
        }
    }

    void unlock()
    {
        if (int rc = pthread_mutex_unlock(&_mutex)) {
            std::stringstream ss;
            ss << "pthread_mutex_unlock errno="<<rc;
            throw std::runtime_error(ss.str());
        }
    }

    /// @return true if the mutex is locked by this thread; otherwise, false
    /// @note Works only in DEBUG mode
    /// @note Specific to Linux implementation of pthreads
    bool isLockedByThisThread()
    {
        bool result = false;
        if (isDebug())  {
            assert(_mutexType == PTHREAD_MUTEX_RECURSIVE);

            int locked = pthread_mutex_trylock(&_mutex);
            if (locked == 0) {
                result = (_mutex.__data.__count > 1);
                unlock();
            }
            result = result || (locked == EAGAIN) || (locked == EDEADLK);
        }
        return result;
    }
};


/***
 * RAII class for holding Mutex in object visible scope.
 */
class ScopedMutexLock
{
private:
	Mutex& _mutex;

public:
    /**
     * @parm mutex - object locked while this is in scope
     * @parm tw - wait enumeration
     *
     */
	ScopedMutexLock(Mutex& mutex, perfTimeWait_t tw = PTW_SML_DFLT): _mutex(mutex)
	{
		_mutex.lock(WaitTimerParams(tw,1,NULL));
	}

    /**
     * @parm mutex - object locked while this is in scope
     * @parm ptw - parameters for undersampled waiting
     */
	ScopedMutexLock(Mutex& mutex, const WaitTimerParams& wtp): _mutex(mutex)
	{
		_mutex.lock(wtp);
	}

	~ScopedMutexLock()
	{
		_mutex.unlock();
	}
};


} //namespace

#endif /* MUTEX_H_ */
