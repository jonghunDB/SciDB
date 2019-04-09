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
 * @file ThreadPool.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The ThreadPool class
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <vector>

#include <util/Semaphore.h>
#include <util/JobQueue.h>
#include <system/Sysinfo.h>
#include <util/InjectedError.h>

namespace scidb
{

class Thread;

/**
 * Pool of threads each of which will infinitely extract and execute jobs from a JobQueue.
 *
 */
class ThreadPool : public InjectedErrorListener
{
    friend class Thread;
private:
    std::vector<std::shared_ptr<Thread> > _threads;
    std::shared_ptr<JobQueue> _queue;
    Mutex _mutex;
    std::vector< std::shared_ptr<Job> > _currentJobs;
    bool _shutdown;
    size_t _threadCount;
    std::shared_ptr<Semaphore> _terminatedThreads;
    const std::string _name;      // for debug messages

public:

    class InvalidArgumentException: public SystemException
    {
    public:
        InvalidArgumentException(const char* file, const char* function, int32_t line)
        : SystemException(file, function, line, "scidb",
                          SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT,
                          "SCIDB_SE_INTERNAL", "SCIDB_LE_INVALID_FUNCTION_ARGUMENT",
                          INVALID_QUERY_ID)
        {
        }
        ~InvalidArgumentException() throw () {}
        void raise() const { throw *this; }
        virtual Exception::Pointer copy() const
        {
            std::shared_ptr<InvalidArgumentException> ep =
                    std::make_shared<InvalidArgumentException>(_file.c_str(),
                                                               _function.c_str(),
                                                               _line);
            ep->_what_str = _what_str;
            ep->_formatted_msg = _formatted_msg;
            ep->_formatter = _formatter;
            return ep;
        }
    };

    class AlreadyStoppedException: public SystemException
    {
    public:
        AlreadyStoppedException(const char* file, const char* function, int32_t line)
        : SystemException(file, function, line, "scidb",
                          SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR,
                          "SCIDB_SE_INTERNAL", "SCIDB_LE_UNKNOWN_ERROR",
                          INVALID_QUERY_ID)
       {
       }
       ~AlreadyStoppedException() throw () {}
       void raise() const { throw *this; }
       virtual Exception::Pointer copy() const
        {
            std::shared_ptr<AlreadyStoppedException> ep =
                    std::make_shared<AlreadyStoppedException>(_file.c_str(),
                                                              _function.c_str(),
                                                              _line);
            ep->_what_str = _what_str;
            ep->_formatted_msg = _formatted_msg;
            ep->_formatter = _formatter;
            return ep;
        }
    };

    class AlreadyStartedException: public SystemException
    {
    public:
        AlreadyStartedException(const char* file, const char* function, int32_t line)
        : SystemException(file, function, line, "scidb",
                          SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR,
                          "SCIDB_SE_INTERNAL", "SCIDB_LE_UNKNOWN_ERROR",
                          INVALID_QUERY_ID)
        {
        }

        ~AlreadyStartedException() throw () {}
        void raise() const { throw *this; }
        virtual Exception::Pointer copy() const
        {
            std::shared_ptr<AlreadyStartedException> ep =
                    std::make_shared<AlreadyStartedException>(_file.c_str(),
                                                              _function.c_str(),
                                                              _line);
            ep->_what_str = _what_str;
            ep->_formatted_msg = _formatted_msg;
            ep->_formatter = _formatter;
            return ep;
        }
    };

    /**
     * Constructor of ThreadPool.
     *
     * @param threadCount the number of threads that will process jobs from queue.
     * @param queue       the job queue from which threads in the pool will pop jobs from.
     * @throws scidb::ThreadPool::InvalidArgumentException if threadCount == 0.
     */
	ThreadPool(size_t threadCount, std::shared_ptr<JobQueue> queue, const std::string& name);

    /**
     * Start the threads in the pool. It can be called only once.
     * @throws scidb::ThreadPool::AlreadyStoppedException if it has been stopped
     * @throws scidb::ThreadPool::AlreadyStartedException if it has been started
     */
    void start();

    /**
     * Try to force the threads to exit and wait for all of them to exit.
     */
	void stop();

    ~ThreadPool() {
        stop();
    }

    std::shared_ptr<JobQueue> getQueue() const
    {
        return _queue;
    }

    const std::string& name() const { return _name; }

    /**
     * @return true if start() was called in the lifetime of the object. False otherwise.
     */
    bool isStarted();

    static void startInjectedErrorListener()
    {
        s_injectedErrorListener.start();
    }

    static InjectedErrorListener&
    getInjectedErrorListener()
    {
        return s_injectedErrorListener;
    }

 private:
    static InjectedErrorListener s_injectedErrorListener;

};

} //namespace

#endif /* THREADPOOL_H_ */
