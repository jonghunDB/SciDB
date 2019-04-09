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
 * @file Query.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of query context methods
 */

#include <time.h>
#include <iostream>
#include <iomanip>

#include <boost/archive/text_iarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/string.hpp>

#include <log4cxx/logger.h>

#include <array/DBArray.h>
#include <array/NewReplicationMgr.h>
#include <query/Query.h>
#include <query/QueryPlan.h>
#include <query/RemoteArray.h>
#include <malloc.h>
#include <memory>
#include <monitor/MonitorCommunicator.h>
#include <monitor/QueryStats.h>
#include <network/MessageDesc.h>
#include <network/MessageHandleJob.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <rbac/Rights.h>
#include <rbac/Session.h>

#include <system/BlockCyclic.h>
#include <system/Cluster.h>
#include <system/Config.h>
#include <system/Exceptions.h>
#include <system/SciDBConfigOptions.h>
#include <system/System.h>
#include <system/SystemCatalog.h>
#include <system/Warnings.h>

#include <rbac/NamespaceDesc.h>
#include <util/iqsort.h>
#include <util/LockManager.h>
#ifndef SCIDB_CLIENT
#include <util/MallocStats.h>
#endif
#include <util/PerfTime.h>
#include <util/PerfTimeLog.h>

namespace scidb
{

using namespace std;
using namespace arena;

// Query class implementation
Mutex Query::queriesMutex;
Query::Queries Query::_queries;
uint32_t Query::nextID = 0;
log4cxx::LoggerPtr replogger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr Query::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr UpdateErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr RemoveErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr BroadcastAbortErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");

boost::mt19937 Query::_rng;
std::atomic<uint64_t> Query::_numOutstandingQueries;

thread_local std::weak_ptr<Query> Query::_queryPerThread;

#ifdef COVERAGE
    extern "C" void __gcov_flush(void);
#endif

size_t Query::PendingRequests::increment()
{
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
    _nReqs += 1;
    return _nReqs;
}

bool Query::PendingRequests::decrement()
{
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
    if (--_nReqs == 0 && _sync) {
        _sync = false;
        return true;
    }
    return false;
}

bool Query::PendingRequests::test()
{
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
    if (_nReqs != 0) {
        _sync = true;
        return false;
    }
    return true;
}

std::shared_ptr<Query>
Query::createFakeQuery(InstanceID coordID,
                       InstanceID localInstanceID,
                       const InstLivenessPtr& liveness,
                       int32_t *longErrorCode)
{
    std::shared_ptr<Query> query = make_shared<Query>(QueryID::getFakeQueryId());
    try {
        query->init(coordID, localInstanceID, liveness);
    }
    catch (const scidb::Exception& e) {
        if (longErrorCode != NULL) {
            *longErrorCode = e.getLongErrorCode();
        } else {
            destroyFakeQuery(query.get());
            throw;
        }
    }
    catch (const std::exception& e) {
        destroyFakeQuery(query.get());
        throw;
    }
    return query;
}

void Query::destroyFakeQuery(Query* q)
 {
     if (q!=NULL && q->getQueryID().isFake()) {
         try {
             arena::ScopedArenaTLS arenaTLS(q->getArena());
             q->handleAbort();
         } catch (scidb::Exception&) { }
     }
 }


Query::Query(const QueryID& queryID):
    _queryID(queryID),
    _livenessSubscriberID(0),
    _instanceID(INVALID_INSTANCE),
    _coordinatorID(INVALID_INSTANCE),
    _error(SYSTEM_EXCEPTION_SPTR(SCIDB_E_NO_ERROR, SCIDB_E_NO_ERROR)),
    _rights(new rbac::RightsMap()),
    _completionStatus(INIT),
    _commitState(UNKNOWN),
    _creationTime(time(NULL)),
    _useCounter(0),
    _doesExclusiveArrayAccess(false),
    _procGrid(NULL),
    _isAutoCommit(false),
    _usecElapsedStart(int64_t(perfTimeGetElapsed()*1.0e6)),
    _queryStats(this),
    //semSG,
    semResults(),
    syncSG()
{
    ++_numOutstandingQueries;

    for(unsigned i=0; i< PTW_NUM; ++i) {
        _twUsecs[i] = 0;
    }
    assert(_twUsecs[0].is_lock_free());
    memset(_twUsecs, 0, sizeof(_twUsecs));
}

Query::~Query()
{
    arena::ScopedArenaTLS arenaTLS(_arena);

    // Reset all data members that may have Value objects.
    // These objects were allocated from the query arena, and
    // must be deallocated when the query context is still in thread-local storage.
    logicalPlan.reset();
    _mergedArray.reset();
    _currentResultArray.reset();
    std::vector<std::shared_ptr<PhysicalPlan>>().swap(_physicalPlans);
    _operatorContext.reset();

    LOG4CXX_TRACE(_logger, "Query::~Query() " << _queryID << " "<<(void*)this);
    if (_arena) {
        LOG4CXX_DEBUG(_logger, "Query._arena:" << *_arena);
    }

    scidb::perfTimeLog(_usecElapsedStart, _twUsecs, *this);

    delete _procGrid ; _procGrid = NULL ;
    --_numOutstandingQueries;
}

void Query::init(InstanceID coordID,
                 InstanceID localInstanceID,
                 const InstLivenessPtr& liveness)
{
   assert(liveness);
   assert(localInstanceID != INVALID_INSTANCE);
   {
      ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

      validate();

      assert( _queryID != INVALID_QUERY_ID);

   /* Install a special arena within the query that all local operator arenas
      should delagate to; we're now using a LeaArena here -  an adaptation of
      Doug Lea's design with a tunable set of bin sizes - because it not only
      supports recycling but also suballocates all of the blocks it hands out
      from large - currently 64 MiB - slabs that are given back to the system
      en masse no later than when the query completes;  the hope here is that
      this reduces the overall fragmentation of the system heap...*/
      {
          assert(_arena == 0);
          stringstream ss ;
          ss << "query "<<_queryID;
          _arena = newArena(Options(ss.str().c_str()).lea(arena::getArena(),64*MiB));
      }

      assert(!_coordinatorLiveness);
      _coordinatorLiveness = liveness;
      assert(_coordinatorLiveness);

      size_t nInstances = _coordinatorLiveness->getNumLive();
      if (nInstances <= 0) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_LIVENESS_EMPTY);
      }
      assert(_liveInstances.size() == 0);
      _liveInstances.clear();
      _liveInstances.reserve(nInstances);

      const InstanceLiveness::LiveInstances& liveInstances =
         _coordinatorLiveness->getLiveInstances();
      assert(liveInstances.size() == nInstances);
      for ( InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
        iter != liveInstances.end(); ++iter) {
         _liveInstances.push_back((*iter).getInstanceId());
      }

      _physInstanceID = localInstanceID;
      _instanceID = mapPhysicalToLogical(localInstanceID);
      assert(_instanceID != INVALID_INSTANCE);
      assert(_instanceID < nInstances);

      _defaultArrResidency = //XXX TODO: use _liveInstances instead !
         std::make_shared<MapArrayResidency>(_liveInstances.begin(),
                                             _liveInstances.end());
        SCIDB_ASSERT(_defaultArrResidency->size() == _liveInstances.size());
        SCIDB_ASSERT(_defaultArrResidency->size() > 0);

      if (coordID == INVALID_INSTANCE) {
         _coordinatorID = INVALID_INSTANCE;
         std::shared_ptr<Query::ErrorHandler> ptr(new BroadcastAbortErrorHandler());
         pushErrorHandler(ptr);
      } else {
         _coordinatorID = mapPhysicalToLogical(coordID);
         assert(_coordinatorID < nInstances);
      }

      _receiveSemaphores.resize(nInstances, Semaphore());
      _receiveMessages.resize(nInstances);
      chunkReqs.resize(nInstances);

      Finalizer f = bind(&Query::destroyFinalizer, _1);
      pushFinalizer(f);
      if (coordID == INVALID_INSTANCE) {
          f = bind(&Query::broadcastCommitFinalizer, _1);
          pushFinalizer(f);
      }
      f.clear();

      _errorQueue = NetworkManager::getInstance()->createWorkQueue("QueryErrorWorkQueue");
      assert(_errorQueue);
      _errorQueue->start();
      _bufferReceiveQueue = NetworkManager::getInstance()->createWorkQueue("QueryBufferReceiveWorkQueue");
      assert(_bufferReceiveQueue);
      _bufferReceiveQueue->start();
      _sgQueue = NetworkManager::getInstance()->createWorkQueue("QueryOperatorWorkQueue");
      _sgQueue->stop();
      assert(_sgQueue);
      _replicationCtx = std::make_shared<ReplicationContext>(shared_from_this(),
                                                             nInstances);
      assert(_replicationCtx);
   }

   // register for notifications
   Notification<InstanceLiveness>::Subscriber listener =
      boost::bind(&Query::handleLivenessNotification, shared_from_this(), _1);
   _livenessSubscriberID = Notification<InstanceLiveness>::subscribe(listener);

   LOG4CXX_DEBUG(_logger, "Initialized query (" << _queryID << ")");
}

void Query::broadcastCommitFinalizer(const std::shared_ptr<Query>& q)
{
    assert(q);
    if (q->wasCommitted()) {
        std::shared_ptr<MessageDesc>  msg(makeCommitMessage(q->getQueryID()));
        NetworkManager::getInstance()->broadcastPhysical(msg);
    }
}

std::shared_ptr<Query> Query::insert(const std::shared_ptr<Query>& query)
{
    assert(query);
    SCIDB_ASSERT(query->getQueryID().isValid());

    SCIDB_ASSERT(queriesMutex.isLockedByThisThread());

    pair<Queries::iterator,bool> res =
       _queries.insert( std::make_pair ( query->getQueryID(), query ) );

    if (res.second) {
        const uint32_t nRequests =
           std::max(Config::getInstance()->getOption<int>(CONFIG_REQUESTS),1);
        if (_queries.size() > nRequests) {
            _queries.erase(res.first);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_RESOURCE_BUSY)
                   << "too many queries");
        }
        assert(res.first->second == query);
        LOG4CXX_DEBUG(_logger, "Allocating query (" << query->getQueryID() << ")");
        LOG4CXX_DEBUG(_logger, "Number of allocated queries = " << _queries.size());

        SCIDB_ASSERT(Query::getQueryByID(query->getQueryID(), false) == query);
        return query;
    }
    return res.first->second;
}

QueryID Query::generateID()
{
   const QueryID queryID(Cluster::getInstance()->getLocalInstanceId(), getTimeInNanoSecs());
   LOG4CXX_DEBUG(_logger, "Generated queryID: instanceID="
                 << queryID.getCoordinatorId()
                 << ", time=" <<  queryID.getId()
                 << ", queryID=" << queryID);
   return queryID;
}

std::shared_ptr<Query> Query::create(QueryID queryID,
                                     InstanceID instanceId)
{
    SCIDB_ASSERT(queryID.isValid());

    std::shared_ptr<Query> query = make_shared<Query>(queryID);
    assert(query);
    assert(query->_queryID == queryID);

    std::shared_ptr<const scidb::InstanceLiveness> myLiveness =
       Cluster::getInstance()->getInstanceLiveness();
    assert(myLiveness);

    query->init(instanceId,
                Cluster::getInstance()->getLocalInstanceId(),
                myLiveness);
    bool isDuplicate = false;
    {

       ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY);
       isDuplicate = (insert(query) != query);
    }
    if (isDuplicate) {
        QueryID dup(query->_queryID);
        query->_queryID = QueryID::getFakeQueryId();
        destroyFakeQuery(query.get());
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DUPLICATE_QUERY_ID)
            << dup;
    }
    return query;
}

void Query::start()
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
    checkNoError();
    if (_completionStatus == INIT) {
        _completionStatus = START;
    }
}

void Query::stop()
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
    checkNoError();
    if (_completionStatus == START) {
        _completionStatus = INIT;
    }
}

void Query::setAutoCommit()
{
    SCIDB_ASSERT(isCoordinator());
    _isAutoCommit = true;
}

void Query::pushErrorHandler(const std::shared_ptr<ErrorHandler>& eh)
{
    assert(eh);
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
    checkNoError();
    _errorHandlers.push_back(eh);
}

void Query::pushFinalizer(const Finalizer& f)
{
    assert(f);
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
    checkNoError();
    _finalizers.push_back(f);
}

void Query::done()
{
    bool isCommit = false;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

        if (!_isAutoCommit &&
            SCIDB_E_NO_ERROR != _error->getLongErrorCode())
        {
            _completionStatus = ERROR;
            _error->raise();
        }
        _completionStatus = OK;
        if (_isAutoCommit) {
            _commitState = COMMITTED; // we have to commit now
            isCommit = true;
        }
    }
    if (isCommit) {
        handleComplete();
    }
}

void Query::done(const std::shared_ptr<Exception>& unwindException)
{
    bool isAbort = false;
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        if (SCIDB_E_NO_ERROR == _error->getLongErrorCode())
        {
            _error = unwindException;
            _error->setQueryId(_queryID);
            msg = _error;
        }
        _completionStatus = ERROR;
        isAbort = (_commitState != UNKNOWN);

        LOG4CXX_DEBUG(_logger, "Query::done: queryID=" << _queryID
                      << ", _commitState=" << _commitState
                      << ", errorCode=" << _error->getLongErrorCode());
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (isAbort) {
        handleAbort();
    }
}

bool Query::doesExclusiveArrayAccess()
{
    return _doesExclusiveArrayAccess;
}

std::shared_ptr<LockDesc>
Query::requestLock(std::shared_ptr<LockDesc>& requestedLock)
{
    assert(requestedLock);
    assert(!requestedLock->isLocked());
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

    if (requestedLock->getLockMode() > LockDesc::RD) {
        _doesExclusiveArrayAccess = true;
    }

    pair<QueryLocks::const_iterator, bool> res =
       _requestedLocks.insert(requestedLock);
    assert(requestedLock->getQueryId() == _queryID);
    if (res.second) {
        assert((*res.first).get() == requestedLock.get());
        LOG4CXX_DEBUG(_logger, "Requested lock: "
                      << (*res.first)->toString()
                      << " inserted");
        return requestedLock;
    }

    if ((*(res.first))->getLockMode() < requestedLock->getLockMode()) {
        _requestedLocks.erase(res.first);
        res = _requestedLocks.insert(requestedLock);
        assert(res.second);
        assert((*res.first).get() == requestedLock.get());
        LOG4CXX_DEBUG(_logger, "Promoted lock: " << (*res.first)->toString() << " inserted");
    }
    return (*(res.first));
}


void Query::handleError(const std::shared_ptr<Exception>& unwindException)
{
    assert(arena::Arena::getArenaTLS() == _arena);
    assert(unwindException);
    assert(unwindException->getLongErrorCode() != SCIDB_E_NO_ERROR);
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = unwindException;
            _error->setQueryId(_queryID);
            msg = _error;
        }
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
}

bool Query::checkFinalState()
{
   ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

   if (!_finalizers.empty()) {
       return false;
   }

   switch (_completionStatus) {
       case OK:
       case ERROR:
           return true;
       case INIT:
           return _error->getLongErrorCode() != SCIDB_E_NO_ERROR;
       case START:
           return false;
       default:
           SCIDB_ASSERT(false);
   }

   // dummy return
   return false;
}

void Query::invokeFinalizers(deque<Finalizer>& finalizers)
{
    assert(arena::Arena::getArenaTLS() == _arena);

    assert(finalizers.empty() || checkFinalState());
    for (deque<Finalizer>::reverse_iterator riter = finalizers.rbegin();
        riter != finalizers.rend(); riter++)
    {
        Finalizer& fin = *riter;
        if (!fin) {
           continue;
        }
        try {
           fin(shared_from_this());
        } catch (const std::exception& e) {
           LOG4CXX_FATAL(_logger, "Query (" << _queryID
                         << ") finalizer failed:"
                         << e.what()
                         << "Aborting!");
           abort();
        }
    }
}

void
Query::invokeErrorHandlers(std::deque<std::shared_ptr<ErrorHandler> >& errorHandlers)
{
    for (deque<std::shared_ptr<ErrorHandler> >::reverse_iterator riter = errorHandlers.rbegin();
         riter != errorHandlers.rend(); riter++) {
        std::shared_ptr<ErrorHandler>& eh = *riter;
        try {
            eh->handleError(shared_from_this());
        } catch (const std::exception& e) {
            LOG4CXX_FATAL(_logger, "Query (" << _queryID
                          << ") error handler failed:"
                          << e.what()
                          << "Aborting!");
            abort();
        }
    }
}

void Query::handleAbort()
{
    assert(arena::Arena::getArenaTLS() == _arena);

    QueryID queryId = INVALID_QUERY_ID;
    deque<Finalizer> finalizersOnStack;
    deque<std::shared_ptr<ErrorHandler> > errorHandlersOnStack;
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

        queryId = _queryID;
        LOG4CXX_DEBUG(_logger, "Query (" << queryId << ") is being aborted");

        if(_commitState == COMMITTED) {
            LOG4CXX_ERROR(_logger, "Query (" << queryId
                          << ") cannot be aborted after commit."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "abort");
        }

        if(_isAutoCommit && _completionStatus == START) {
            LOG4CXX_ERROR(_logger, "Query (" << queryId
                          << ") cannot be aborted when in autoCommit state."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "abort");
        }

        _commitState = ABORTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_CANCELLED)
                      << queryId);
            _error->setQueryId(queryId);
            msg = _error;
        }
        if (_completionStatus == START)
        {
            LOG4CXX_DEBUG(_logger, "Query (" << queryId << ") is still in progress");
            return;
        }
        errorHandlersOnStack.swap(_errorHandlers);
        finalizersOnStack.swap(_finalizers);
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (!errorHandlersOnStack.empty()) {
        LOG4CXX_ERROR(_logger, "Query (" << queryId << ") error handlers ("
                     << errorHandlersOnStack.size() << ") are being executed");
        invokeErrorHandlers(errorHandlersOnStack);
        errorHandlersOnStack.clear();
    }
    invokeFinalizers(finalizersOnStack);
}

void Query::handleCommit()
{
    assert(arena::Arena::getArenaTLS() == _arena);

    QueryID queryId = INVALID_QUERY_ID;
    deque<Finalizer> finalizersOnStack;
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

        queryId = _queryID;

        LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") is being committed");

        if (_completionStatus != OK || _commitState == ABORTED) {
            LOG4CXX_ERROR(_logger, "Query (" << _queryID
                          << ") cannot be committed after abort."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "commit");
        }

        _errorHandlers.clear();

        _commitState = COMMITTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_ALREADY_COMMITED);
            (*static_cast<scidb::SystemException*>(_error.get())) << queryId;
            _error->setQueryId(queryId);
            msg = _error;
        }
        finalizersOnStack.swap(_finalizers);
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    assert(queryId != INVALID_QUERY_ID);
    invokeFinalizers(finalizersOnStack);
}

void Query::handleComplete()
{
    assert(arena::Arena::getArenaTLS() == _arena);
    handleCommit();
#ifdef COVERAGE
    __gcov_flush();
#endif
}

void Query::handleCancel()
{
    assert(arena::Arena::getArenaTLS() == _arena);
    handleAbort();
}

void Query::handleLivenessNotification(std::shared_ptr<const InstanceLiveness>& newLiveness)
{
    QueryID thisQueryId;
    InstanceID coordPhysId = INVALID_INSTANCE;
    std::shared_ptr<const scidb::Exception> msg;
    bool isAbort = false;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

        assert(newLiveness->getVersion() >= _coordinatorLiveness->getVersion());

        if (newLiveness->getVersion() == _coordinatorLiveness->getVersion()) {
            assert(newLiveness->isEqual(*_coordinatorLiveness));
            return;
        }

        LOG4CXX_ERROR(_logger, "Query " << _queryID << " is aborted on changed liveness");

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
            _error->setQueryId(_queryID);
            msg = _error;
        }

        if (_coordinatorID != INVALID_INSTANCE) {
            coordPhysId = getPhysicalCoordinatorID();

            InstanceLiveness::InstancePtr newCoordState = newLiveness->find(coordPhysId);
            isAbort = newCoordState->isDead();
            if (!isAbort) {
                InstanceLiveness::InstancePtr oldCoordState = _coordinatorLiveness->find(coordPhysId);
                isAbort = (*newCoordState != *oldCoordState);
            }
        }
        // If the coordinator is dead, we abort the query.
        // There is still a posibility that the coordinator actually has committed.
        // For read queries it does not matter.
        // For write queries UpdateErrorHandler::handleErrorOnWorker() will wait
        // (while holding its own array lock)
        // until the coordinator array lock is released and decide whether to really abort
        // based on the state of the catalog (i.e. if the new version is recorded).

        if (!_errorQueue) {
            LOG4CXX_TRACE(_logger,
                          "Liveness change will not be handled for a deallocated query ("
                          << _queryID << ")");
            isAbort = false;
        }
        thisQueryId = _queryID;
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (!isAbort) {
        return;
    }
    try {
        std::shared_ptr<MessageDesc> msg = makeAbortMessage(thisQueryId);

        // HACK (somewhat): set sourceid to coordinator, because only it can issue an abort
        assert(coordPhysId != INVALID_INSTANCE);
        msg->setSourceInstanceID(coordPhysId);

        std::shared_ptr<MessageHandleJob> job = make_shared<ServerMessageHandleJob>(msg);
        job->dispatch(NetworkManager::getInstance());

    } catch (const scidb::Exception& e) {
        LOG4CXX_WARN(_logger, "Failed to abort queryID=" << thisQueryId
                      << " on coordinator liveness change because: " << e.what());
        if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND) {
            SCIDB_ASSERT(false);
            throw;
        } else {
            LOG4CXX_TRACE(_logger,
                          "Liveness change will not be handled for a deallocated query ("
                          << thisQueryId << ")");
        }
    }
}

InstanceID Query::getPhysicalCoordinatorID(bool resolveLocalInstanceId)
{
    InstanceID coord = _coordinatorID;
    if (_coordinatorID == INVALID_INSTANCE) {
        if (!resolveLocalInstanceId) {
            return INVALID_INSTANCE;
        }
        coord = _instanceID;
    }
    assert(_liveInstances.size() > 0);
    assert(_liveInstances.size() > coord);
    return _liveInstances[coord];
}

InstanceID Query::mapLogicalToPhysical(InstanceID instance)
{
   if (instance == INVALID_INSTANCE) {
      return instance;
   }
   ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);  //XXX TODO: remove lock ?
   assert(_liveInstances.size() > 0);
   if (instance >= _liveInstances.size()) {
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instance;
   }
   checkNoError();
   instance = _liveInstances[instance];
   return instance;
}

InstanceID Query::mapPhysicalToLogical(InstanceID instanceID)
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY); //XXX TODO: remove lock ?
    assert(_liveInstances.size() > 0);
    size_t index=0;
    bool found = bsearch(_liveInstances, instanceID, index);
    if (!found) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instanceID;
    }
    return index;
}

bool Query::isPhysicalInstanceDead(InstanceID instance)
{
   ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);  //XXX TODO: remove lock ?
   checkNoError();
   InstanceLiveness::InstancePtr instEntry = _coordinatorLiveness->find(instance);
   // non-existent == dead
   return (instEntry==NULL || instEntry->isDead());
}

bool Query::isDistributionDegraded(const ArrayDesc& desc, size_t redundancy)
{
    // Arrays are allowed to exist on different instances.
    ArrayResPtr res = desc.getResidency();
    ArrayDistPtr dist = desc.getDistribution();

    Cluster* cluster = Cluster::getInstance();
    SCIDB_ASSERT(cluster);
    InstMembershipPtr membership = /* Make sure the membership has not changed from under us */
            cluster->getMatchingInstanceMembership(getCoordinatorLiveness()->getMembershipId());

    SCIDB_ASSERT(membership);

    const InstanceLiveness::LiveInstances& liveInstances =
       getCoordinatorLiveness()->getLiveInstances();

    size_t numLiveServers = 0;
    if (isDebug()) {
        // Count the total number of *live* servers.
        // A server is considered live if all of its instances are live
        // (i.e. not marked as dead in the query liveness set)
        ServerCounter scT;
        membership->visitInstances(scT);

        const InstanceLiveness::LiveInstances& deadInstances =
                getCoordinatorLiveness()->getDeadInstances();

        ServerCounter scD;
        for (InstanceLiveness::DeadInstances::const_iterator iter = deadInstances.begin();
             iter != deadInstances.end();
             ++iter) {
            const InstanceID iId = (*iter).getInstanceId();
            scD(iId);
        }
        SCIDB_ASSERT(scT.getCount() >= scD.getCount());
        // live = total - dead
        numLiveServers = scT.getCount() - scD.getCount();
    }

    std::set<InstanceID> liveSet; //XXX TODO: use _liveInstances instead !
    for (InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
         iter != liveInstances.end();
         ++iter) {
        const InstanceID iId = (*iter).getInstanceId();
        bool inserted = liveSet.insert(iId).second;
        SCIDB_ASSERT(inserted);
    }

    if (res->isEqual(liveSet.begin(),liveSet.end())) {
        // query liveness is the same as array residency
        return false;
    }

    const size_t residencySize = res->size();
    size_t numLiveInRes = 0;
    size_t numResidencyServers = 0;
    size_t numLiveServersInResidency = 0;
    {
        ServerCounter scR;
        ServerCounter scD;
        for (size_t i=0; i < residencySize; ++i) { //XXX TODO: can we make it O(n) ?
            const InstanceID id = res->getPhysicalInstanceAt(i);
            scR(id);
            const size_t nFound = liveSet.count(id);
            SCIDB_ASSERT(nFound<2);
            numLiveInRes += nFound;
            if (nFound==0) { scD(id); }
        }
        numResidencyServers = scR.getCount();
        numLiveServersInResidency = numResidencyServers - scD.getCount();
    }

    LOG4CXX_TRACE(_logger, "Query::isDistributionDegraded: "
                  << "residencySize=" << residencySize
                  << " numLiveInRes=" << numLiveInRes
                  << " numLiveServers(debug build only)=" << numLiveServers
                  << " numResidencyServers=" << numResidencyServers
                  << " numLiveServersInResidency=" << numLiveServersInResidency
                  << " queryID=" << _queryID );

    // The number of live servers in residency may be higher or lower
    // than the number of servers with dead instances because a particular
    // array residency may span an arbitrary set of instances
    // (and say not include the dead instances).
    // So, numLiveServers >=< numLiveServersInResidency

    if (numLiveInRes == residencySize) {
        // all instances in the residency are alive
        return false;
    }

    SCIDB_ASSERT(numLiveInRes < residencySize);

    if ((numLiveServersInResidency + redundancy) < numResidencyServers) {
        // not enough redundancy, too many servers are down
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
    }

    return true;
}

bool Query::isDistributionDegradedForRead(const ArrayDesc& desc)
{
    const size_t redundancy = desc.getDistribution()->getRedundancy();
    return isDistributionDegraded(desc, redundancy);
}

bool Query::isDistributionDegradedForWrite(const ArrayDesc& desc)
{
    return isDistributionDegraded(desc, 0);
}

void Query::checkDistributionForRemove(const ArrayDesc& desc)
{
    // We need to make sure that the array residency was determined
    // strictly before the query membership. That will allow us to determine
    // if we have enough instances on-line to remove the array.
    InstMembershipPtr membership =
            Cluster::getInstance()->getInstanceMembership(MAX_MEMBERSHIP_ID);
    if (getCoordinatorLiveness()->getMembershipId() != membership->getId()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                               SCIDB_LE_LIVENESS_MISMATCH);
    }

    //XXX TODO: if the entire residency set is alive, it should be OK to remove as well
    if (getCoordinatorLiveness()->getNumDead()>0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                               SCIDB_LE_NO_QUORUM);
    }
}

namespace
{
    class ResidencyConstructor
    {
    public:
        ResidencyConstructor(std::vector<InstanceID>& instances, bool isRandom=false)
        :  _instances(instances), _isRandom(isRandom)
        {
        }
        void operator() (const InstanceDesc& i)
        {
            if (isDebug()) {
                if (!_isRandom || Query::getRandom()%2 == 1) {
                    _instances.push_back(i.getInstanceId());
                }
            } else {
                // normal production case
                _instances.push_back(i.getInstanceId());
            }
        }
    private:
        std::vector<InstanceID>& _instances;
        bool _isRandom;
    };
}

ArrayResPtr Query::getDefaultArrayResidencyForWrite()
{
    Cluster* cluster = Cluster::getInstance();
    SCIDB_ASSERT(cluster);

    InstMembershipPtr membership = /* Make sure the membership has not changed from under us */
            cluster->getMatchingInstanceMembership(getCoordinatorLiveness()->getMembershipId());

    SCIDB_ASSERT(membership);
    SCIDB_ASSERT(membership->getNumInstances() == getCoordinatorLiveness()->getNumInstances());

    bool isRandomRes(false);
    if (isDebug()) {
        // for testing only
        isRandomRes = (Config::getInstance()->getOption<std::string>(CONFIG_PERTURB_ARR_RES) == "random");
    }

    ArrayResPtr res;
    if (!isRandomRes && membership->getNumInstances() == getCoordinatorLiveness()->getNumLive()) {
        SCIDB_ASSERT(getCoordinatorLiveness()->getNumDead()==0);
        return getDefaultArrayResidency();
    }

    std::vector<InstanceID> resInstances;
    resInstances.reserve(membership->getNumInstances());
    ResidencyConstructor rc(resInstances, isRandomRes);
    membership->visitInstances(rc);
    if (!isRandomRes) {
        SCIDB_ASSERT(resInstances.size() == membership->getNumInstances());
    } else if (resInstances.empty()) {
        resInstances.push_back(getPhysicalInstanceID());
    }
    res = std::make_shared<MapArrayResidency>(resInstances);

    return res;
}

ArrayResPtr Query::getDefaultArrayResidency()
{
    SCIDB_ASSERT(_defaultArrResidency);
    return _defaultArrResidency;
}

std::shared_ptr<Query> Query::getQueryByID(QueryID queryID, bool raise)
{
    std::shared_ptr<Query> query;

    // The active query map holds only valid query ids.
    if (queryID.isValid()) {
        ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY);

        Queries::const_iterator q = _queries.find(queryID);
        if (q != _queries.end()) {
            return q->second;
        }
    }

    if (raise) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND)
            << queryID;
    }
    assert(!query);
    return query;
}

void Query::freeQueries(const Finalizer& onFreeQuery)
{
    Queries queries;
    {
        ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY);
        queries.swap(_queries);
    }
    for (auto const& q : queries) {
        LOG4CXX_DEBUG(_logger, "Deallocating queries: (" << q.second->getQueryID() << ")");
        try {
            arena::ScopedArenaTLS arenaTLS(q.second->getArena());
            onFreeQuery(q.second);
            q.second->handleAbort();
        } catch (Exception&) {
        }
    }
}

void Query::freeQueriesWithWarning(const Warning& warn)
{
    freeQueries([&warn](const std::shared_ptr<Query>& q) {
        SCIDB_ASSERT(q);
        q->postWarning(warn);
    });
}

size_t Query::visitQueries(const Visitor& visit)
{
    ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY);

    if (visit)
    {
        for (auto & i : _queries)
        {
            visit(i.second);
        }
    }

    return _queries.size();
}

std::string Query::getCompletionStatusStr() const
{
    switch(_completionStatus)
    {
        case INIT:    return "0 - pending";
        case START:   return "1 - active";
        case OK:      return "2 - completed";
        case ERROR:   return "3 - errors";
    }

    std::stringstream ss;
    ss << "4 - unknown=" << _completionStatus;
    return ss.str();
}

QueryStats & Query::getStats(bool updateArena)
{
    if(updateArena)
    {
        // find a better place to do this
        _queryStats.setArenaInfo(_arena);
    }
    return _queryStats;
}

void dumpMemoryUsage(const QueryID queryId)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_OUTPUT_PROC_STATS)) {
        const size_t* mstats = getMallocStats();
        LOG4CXX_DEBUG(Query::_logger,
                      "Stats after query ID ("<<queryId<<"): "
                      <<"Allocated size for PersistentChunks: " << StorageMan::getInstance().getUsedMemSize()
                      <<", allocated size for network messages: " << NetworkManager::getInstance()->getUsedMemSize()
                      <<", number of mallocs: " << (mstats ? mstats[0] : 0)
                      <<", number of mallocs: " << (mstats ? mstats[0] : 0)
                      <<", number of frees: "   << (mstats ? mstats[1] : 0)
                      <<", number of outstanding queries: " << Query::_numOutstandingQueries.load()-1);
    }
#endif
}

/**
 * @brief Collect @c OperatorContext smart pointers from a physical plan tree.
 *
 * @details The various OperatorContexts in the physical plan can hold
 * smart pointer references that should be cleaned up when the Query
 * is finalized in @c Query::destroy() below.  In particular, any
 * PullSGContext in the tree will hang on to intermediate input
 * arrays, which may be (or may contain) large materialized MemArrays.
 * Those ought to be released when the query is finalized.
 *
 * @p We walk the tree and collect the smart pointers, so that when
 * this @c OpContextReaper object goes out of scope, the smart pointer
 * references will go away.  As with other smart pointers cleared by
 * @c Query::destroy(), the pointers are gathered under @c errorMutex,
 * but actual destruction of pointed-at objects occurs at scope-exit,
 * when the lock is not held.  We don't want to hold the lock because
 * these destructors can be quite heavyweight---for example,
 * destroying a MemArray may involve discarding dirty buffers and
 * removing an on-disk DataStore.
 */
class OpContextReaper
    : public PhysicalQueryPlanNode::Visitor
{
    using OpCtxPtr = std::shared_ptr<OperatorContext>;
    vector<OpCtxPtr> _ptrs;
public:
    void operator()(PhysicalQueryPlanNode& node)
    {
        SCIDB_ASSERT(node.getPhysicalOperator());
        PhysicalOperator& phyOp = *node.getPhysicalOperator();
        OpCtxPtr opCtx = phyOp.getOperatorContext();
        if (opCtx) {
            _ptrs.push_back(opCtx);
            phyOp.unsetOperatorContext();
        }
    }
};

void Query::destroy()
{
    freeQuery(getQueryID());

    // Any members explicitly destroyed in this method
    // should have an atomic query validating getter method
    // (and setter ?)
    std::shared_ptr<Array> resultArray;
    std::shared_ptr<RemoteMergedArray> mergedArray;
    std::shared_ptr<WorkQueue> bufferQueue;
    std::shared_ptr<WorkQueue> errQueue;
    std::shared_ptr<WorkQueue> opQueue;
    Continuation continuation;
    std::shared_ptr<ReplicationContext> replicationCtx;
    std::shared_ptr<OperatorContext> opCtx;
    OpContextReaper reaper;

    // Swap pointers into local copies under mutex.  Pointed-at
    // objects are destroyed on scope-exit.  (Destructors may involve
    // lots of processing and should not run with the lock held.)
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);

        LOG4CXX_TRACE(_logger, "Cleaning up query (" << getQueryID() << ")");

        // Drop all unprocessed messages and cut any circular references
        // (from MessageHandleJob to Query).
        // This should be OK because we broadcast either
        // the error or abort before dropping the messages

        _bufferReceiveQueue.swap(bufferQueue);
        _errorQueue.swap(errQueue);
        _sgQueue.swap(opQueue);
        _replicationCtx.swap(replicationCtx);

        // One day there may be more than one physical plan, but at
        // this point there's just the current one (or none, if the
        // query failed before one could be generated).
        auto plan = getCurrentPhysicalPlan();
        if (plan) {
            plan->getRoot()->visitDepthFirst(reaper);
        }

        // Unregister this query from liveness notifications
        Notification<InstanceLiveness>::unsubscribe(_livenessSubscriberID);

        // The result array may also have references to this query
        _currentResultArray.swap(resultArray);

        _mergedArray.swap(mergedArray);
        _continuation.swap(continuation);
        _operatorContext.swap(opCtx);
    }
    if (bufferQueue) { bufferQueue->stop(); }
    if (errQueue)    { errQueue->stop(); }
    if (opQueue)     { opQueue->stop(); }

    scidb::monitor::Communicator::addQueryInfoToHistory(this);
    dumpMemoryUsage(getQueryID());
}

void
BroadcastAbortErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    if (query->getQueryID().isFake()) {
        return;
    }
    if (!query->getQueryID().isValid()) {
        assert(false);
        return;
    }
    if (! query->isCoordinator()) {
        assert(false);
        return;
    }
    LOG4CXX_DEBUG(_logger, "Broadcast ABORT message to all instances for query "
                  << query->getQueryID());
    std::shared_ptr<MessageDesc> abortMessage = makeAbortMessage(query->getQueryID());
    // query may not have the instance map, so broadcast to all
    NetworkManager::getInstance()->broadcastPhysical(abortMessage);
}

void Query::freeQuery(const QueryID& queryID)
{
    ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY);
    Queries::iterator i = _queries.find(queryID);
    if (i != _queries.end()) {
        std::shared_ptr<Query>& q = i->second;
        LOG4CXX_DEBUG(_logger, "Deallocating query (" << q->getQueryID()
                      << ") with use_count=" << q.use_count());
        _queries.erase(i);
    }
}

bool Query::validate()
{
    bool isShutdown = NetworkManager::isShutdown();
    if (isShutdown) {
        arena::ScopedArenaTLS arenaTLS(_arena);
        handleAbort();
    }

    // called at high rates when chunks have low cell counts
    // requires statistically sampled timing to keep timing cost insignificant
    // good example: reshape an 8k x 8k (csize 1k) array to 16k x 4k (csize 1k)
    static thread_local uint64_t unsampledCount=0;      // per-thread sub-sampling memo
    const uint64_t SAMPLE_INTERVAL=1009;                // prime avoids some aliasing
                                                        // >1000 drops cost sufficiently for this case
    WaitTimerParams wtp(PTW_SML_QUERY, SAMPLE_INTERVAL, &unsampledCount);
    ScopedMutexLock cs(errorMutex, wtp);
    checkNoError();
    return true;
}

void Query::checkNoError() const
{
    SCIDB_ASSERT(const_cast<Mutex*>(&errorMutex)->isLockedByThisThread());

    if (_isAutoCommit && _commitState == COMMITTED &&
        _error->getLongErrorCode() == SCIDB_LE_QUERY_ALREADY_COMMITED) {
        SCIDB_ASSERT(!_currentResultArray);
        return;
    }

    if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
    {
        // note: error code can be SCIDB_LE_QUERY_ALREADY_COMMITED
        //       because ParallelAccumulatorArray is started
        //       regardless of whether the client pulls the data
        //       (even in case of mutating queries).
        //       So, the client can request a commit before PAA is done.
        //       An exception will force the backgound PAA threads to exit.
        _error->raise();
    }
}

std::shared_ptr<Array> Query::getCurrentResultArray()
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
    validate();
    ASSERT_EXCEPTION ( (!_isAutoCommit || !_currentResultArray),
                       "Auto-commit query cannot return data");
    return _currentResultArray;
}

void Query::setCurrentResultArray(const std::shared_ptr<Array>& array)
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
    validate();
    ASSERT_EXCEPTION ( (!_isAutoCommit || !array),
                       "Auto-commit query cannot return data");
    _currentResultArray = array;
}

// TODO: change to PV (=0)and eliminate this implementation?
OperatorContext::~OperatorContext()
{
}

void Query::startSGQueue(std::shared_ptr<OperatorContext> const& opContext,
                         std::shared_ptr<JobQueue> const& jobQueue)
{
    SCIDB_ASSERT(opContext);
    SCIDB_ASSERT(_sgQueue);

    ScopedMutexLock lock(errorMutex, PTW_SML_QUERY);
    assert(validate());

    // NOTE: Allthough we want to stop using Query's _operatorContext,
    // it's still used in some cases (Distributed Sort?)
    // and for the merging of REMOTE_ARRAYS onto the coordinator
    _operatorContext = opContext;
    _sgQueue->start(jobQueue);
}

void Query::stopSGQueue()
{
    SCIDB_ASSERT(_operatorContext);
    SCIDB_ASSERT(_sgQueue);

    ScopedMutexLock lock(errorMutex, PTW_SML_QUERY);
    SCIDB_ASSERT(validate());

    if(false) { // TODO: we can't actually reset it here yet, it breaks sort()
        _operatorContext.reset();
    }

    if(false) { // TODO: we can't actually stop it ourselves yet.
        // we'd have to count how many times it was started
        _sgQueue->stop();
    }
}

void Query::postWarning(const Warning& warn)
{
    ScopedMutexLock lock(_warningsMutex, PTW_SML_QUERY);
    _warnings.push_back(warn);
}

std::vector<Warning> Query::getWarnings()
{
    ScopedMutexLock lock(_warningsMutex, PTW_SML_QUERY);
    return _warnings;
}

void Query::clearWarnings()
{
    ScopedMutexLock lock(_warningsMutex, PTW_SML_QUERY);
    _warnings.clear();
}

void RemoveErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    boost::function<bool()> work = boost::bind(&RemoveErrorHandler::handleRemoveLock, _lock, true);
    Query::runRestartableWork<bool, Exception>(work);
}

bool RemoveErrorHandler::handleRemoveLock(const std::shared_ptr<LockDesc>& lock,
                                          bool forceLockCheck)
{
   assert(lock);
   assert(lock->getLockMode() == LockDesc::RM);
   SystemCatalog& sysCat = *SystemCatalog::getInstance();

   std::shared_ptr<LockDesc> coordLock;
   if (!forceLockCheck) {
      coordLock = lock;
   } else {
      coordLock = sysCat.checkForCoordinatorLock(
            lock->getNamespaceName(), lock->getArrayName(), lock->getQueryId());
   }
   if (!coordLock) {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
                     " lock does not exist. No action for query "
                     << lock->getQueryId());
       return false;
   }

   if (coordLock->getArrayId() == 0 ) {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
                     " lock is not initialized. No action for query "
                     << lock->getQueryId());
       return false;
   }

   bool rc;
   if (coordLock->getArrayVersion() == 0)
   {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
            << " lock queryID="       << coordLock->getQueryId()
            << " lock namespaceName=" << coordLock->getNamespaceName()
            << " lock arrayName="     << coordLock->getArrayName());
       rc = sysCat.deleteArray(coordLock->getNamespaceName(),
                               coordLock->getArrayName());
   }
   else
   {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
            << " lock queryID="       << coordLock->getQueryId()
            << " lock namespaceName=" << coordLock->getNamespaceName()
            << " lock arrayName="     << coordLock->getArrayName()
            << " lock arrayVersion="  << coordLock->getArrayVersion());
       rc = sysCat.deleteArrayVersions(coordLock->getNamespaceName(),
                                       coordLock->getArrayName(),
                                       coordLock->getArrayVersion());
   }
   return rc;
}

void UpdateErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    boost::function<void()> work = boost::bind(&UpdateErrorHandler::_handleError, this, query);
    Query::runRestartableWork<void, Exception>(work);
}

void UpdateErrorHandler::_handleError(const std::shared_ptr<Query>& query)
{
   assert(query);
   if (!_lock) {
      assert(false);
      LOG4CXX_TRACE(_logger,
                    "Update error handler has nothing to do for query ("
                    << query->getQueryID() << ")");
      return;
   }
   assert(_lock->getInstanceId() == Cluster::getInstance()->getLocalInstanceId());
   assert( (_lock->getLockMode() == LockDesc::CRT)
           || (_lock->getLockMode() == LockDesc::WR) );
   assert(query->getQueryID() == _lock->getQueryId());

   LOG4CXX_DEBUG(_logger,
                 "Update error handler is invoked for query ("
                 << query->getQueryID() << ")");

   if (_lock->getInstanceRole() == LockDesc::COORD) {
      handleErrorOnCoordinator(_lock, true);
   } else {
      assert(_lock->getInstanceRole() == LockDesc::WORKER);
      handleErrorOnWorker(_lock, query->isForceCancelled(), true);
   }
}

void UpdateErrorHandler::releaseLock(const std::shared_ptr<LockDesc>& lock,
                                     const std::shared_ptr<Query>& query)
{
   assert(lock);
   assert(query);
   boost::function<bool()> work = boost::bind(&SystemCatalog::unlockArray,
                                             SystemCatalog::getInstance(),
                                             lock);
   bool rc = Query::runRestartableWork<bool, Exception>(work);
   if (!rc) {
      LOG4CXX_WARN(_logger, "Failed to release the lock for query ("
                   << query->getQueryID() << ")");
   }
}

static bool isTransientArray(const std::shared_ptr<LockDesc> & lock)
{
    return ( lock->getArrayId() > 0 &&
             lock->getArrayId() == lock->getArrayVersionId() &&
             lock->getArrayVersion() == 0 );
}


void UpdateErrorHandler::handleErrorOnCoordinator(const std::shared_ptr<LockDesc> & lock,
                                                  bool rollback)
{
   assert(lock);
   assert(lock->getInstanceRole() == LockDesc::COORD);

   string const& namespaceName = lock->getNamespaceName();
   string const& arrayName = lock->getArrayName();

   std::shared_ptr<LockDesc> coordLock =
      SystemCatalog::getInstance()->checkForCoordinatorLock(
            namespaceName, arrayName, lock->getQueryId());
   if (!coordLock) {
      LOG4CXX_DEBUG(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                    " coordinator lock does not exist. No abort action for query "
                    << lock->getQueryId());
      return;
   }

   if (isTransientArray(coordLock)) {
       SCIDB_ASSERT(false);
       // no rollback for transient arrays
       return;
   }

   const ArrayID unversionedArrayId  = coordLock->getArrayId();
   const VersionID newVersion      = coordLock->getArrayVersion();
   const ArrayID newArrayVersionId = coordLock->getArrayVersionId();

   if (unversionedArrayId == 0) {
       SCIDB_ASSERT(newVersion == 0);
       SCIDB_ASSERT(newArrayVersionId == 0);
       // the query has not done much progress, nothing to rollback
       return;
   }

   ASSERT_EXCEPTION(newVersion > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent newVersion<=0"));
   ASSERT_EXCEPTION(unversionedArrayId > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent unversionedArrayId<=0"));
   ASSERT_EXCEPTION(newArrayVersionId > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent newArrayVersionId<=0"));

   const VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(unversionedArrayId);

   if (lastVersion == newVersion) {
       // we are done, the version is committed
       return;
   }
   SCIDB_ASSERT(lastVersion < newVersion);
   SCIDB_ASSERT(lastVersion == (newVersion-1));

   if (rollback) {

       LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                     " the new version "<< newVersion
                     <<" of array " << ArrayDesc::makeQualifiedArrayName(namespaceName, arrayName)
                     <<" (arrId="<< newArrayVersionId <<")"
                     <<" is being rolled back for query ("
                     << lock->getQueryId() << ")");

       doRollback(lastVersion, unversionedArrayId, newArrayVersionId);
   }
}

void UpdateErrorHandler::handleErrorOnWorker(const std::shared_ptr<LockDesc>& lock,
                                             bool forceCoordLockCheck,
                                             bool rollback)
{
   assert(lock);
   assert(lock->getInstanceRole() == LockDesc::WORKER);

   string const& namespaceName  = lock->getNamespaceName();
   string const& arrayName      = lock->getArrayName();
   VersionID newVersion         = lock->getArrayVersion();
   ArrayID newArrayVersionId    = lock->getArrayVersionId();

   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                 << " forceLockCheck = "<< forceCoordLockCheck
                 << " arrayName = "<< ArrayDesc::makeQualifiedArrayName(namespaceName, arrayName)
                 << " newVersion = "<< newVersion
                 << " newArrayVersionId = "<< newArrayVersionId);

   if (newVersion != 0) {

       if (forceCoordLockCheck) {
           std::shared_ptr<LockDesc> coordLock;
           do {  //XXX TODO: fix the wait, possibly with batching the checks
               coordLock = SystemCatalog::getInstance()->checkForCoordinatorLock(
                    namespaceName, arrayName, lock->getQueryId());
               Query::waitForSystemCatalogLock();
           } while (coordLock);
       }
       ArrayID arrayId = lock->getArrayId();
       if(arrayId == 0) {
           LOG4CXX_WARN(_logger, "Invalid update lock for query ("
                        << lock->getQueryId()
                        << ") Lock:" << lock->toString()
                        << " No rollback is possible.");
       }
       VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(arrayId);
       assert(lastVersion <= newVersion);

       // if we checked the coordinator lock, then lastVersion == newVersion implies
       // that the commit succeeded, and we should not rollback.
       // if we are not checking the coordinator lock, then something failed locally
       // and it should not be possible that the coordinator committed---we should
       // definitely rollback.
       assert(forceCoordLockCheck || lastVersion < newVersion);

       if (lastVersion < newVersion && newArrayVersionId > 0 && rollback) {

           LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                         " the new version "<< newVersion
                         <<" of array " << ArrayDesc::makeQualifiedArrayName(namespaceName, arrayName)
                         <<" (arrId="<< newArrayVersionId <<")"
                         <<" is being rolled back for query ("
                         << lock->getQueryId() << ")");

           doRollback(lastVersion, arrayId, newArrayVersionId);
       }
   }
   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                     << " exit");
}

void UpdateErrorHandler::doRollback(VersionID lastVersion,
                                    ArrayID baseArrayId,
                                    ArrayID newArrayId)
{

    LOG4CXX_TRACE(_logger, "UpdateErrorHandler::doRollback:"
                  << " baseArrayId = "<< baseArrayId
                  << " newArrayId = "<< newArrayId);

   // if a query stopped before the coordinator recorded the new array
   // version id there is no rollback to do
   assert(newArrayId>0);
   assert(baseArrayId>0);

   try {
       NewDBArray::rollbackVersion(lastVersion,
                                   baseArrayId,
                                   newArrayId);
   } catch (const scidb::Exception& e) {
       LOG4CXX_ERROR(_logger, "UpdateErrorHandler::doRollback:"
                     << " baseArrayId = "<< baseArrayId
                     << " newArrayId = "<< newArrayId
                     << ". Error: "<< e.what());
       throw;
   }
}

void Query::registerPhysicalOperator(const std::weak_ptr<PhysicalOperator>& phyOpWeak)
{
    auto phyOp = phyOpWeak.lock();
    OperatorID opID = phyOp->getOperatorID();

    if(opID.isValid()) {
        if (_physicalOperators.size() < opID.getValue()+1) {
            _physicalOperators.resize(opID.getValue());  // default-insert missing entries
            _physicalOperators.push_back(phyOpWeak); // add the specified one
        } else {
            _physicalOperators[opID.getValue()] = phyOpWeak;
        }
        LOG4CXX_TRACE(_logger, "Query::registerPhysOp(): operatorID valid, no change");
    } else {
        _physicalOperators.push_back(std::weak_ptr<PhysicalOperator>(phyOpWeak));
        phyOp->setOperatorID(OperatorID(_physicalOperators.size()-1));
        LOG4CXX_TRACE(_logger, "Query::registerPhysOp(): (coordinator case) new operaterID generated");
    }
}

std::shared_ptr<PhysicalOperator> Query::getPhysicalOperatorByID(const OperatorID& operatorID) const
{
    SCIDB_ASSERT(_physicalOperators.size() > 0);

    SCIDB_ASSERT(operatorID.isValid());                           // by looking only at the number
    SCIDB_ASSERT(operatorID.getValue() < _physicalOperators.size()); // that its contained in the table

    auto weakPtr =  _physicalOperators[operatorID.getValue()];
    auto result = weakPtr.lock();

    SCIDB_ASSERT(result->getOperatorID() == operatorID); // matches the original

    return result;
}
void Query::releaseLocks(const std::shared_ptr<Query>& q)
{
    assert(q);
    LOG4CXX_DEBUG(_logger, "Releasing locks for query " << q->getQueryID());

    boost::function<uint32_t()> work = boost::bind(&SystemCatalog::deleteArrayLocks,
                                                   SystemCatalog::getInstance(),
                                                   Cluster::getInstance()->getLocalInstanceId(),
                                                   q->getQueryID(),
                                                   LockDesc::INVALID_ROLE);
    runRestartableWork<uint32_t, Exception>(work);
}

void Query::acquireLocks()
{
    QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        validate();
        Query::Finalizer f = bind(&Query::releaseLocks, _1);
        pushFinalizer(f);
        assert(_finalizers.size() > 1);
        locks = _requestedLocks;
    }
    acquireLocksInternal(locks);
}

void Query::retryAcquireLocks()
{
    QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        // try to assert that the lock release finalizer is in place
        assert(_finalizers.size() > 1);
        validate();
        locks = _requestedLocks;
    }
    if (locks.empty())
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "Query::retryAcquireLocks";
    }
    acquireLocksInternal(locks);
}

void Query::acquireLocksInternal(QueryLocks& locks)
{
    LOG4CXX_TRACE(_logger, "Acquiring "<< locks.size()
                  << " array locks for query " << _queryID);

    // If we don't have quorum and we're requesting anything other than
    // a read-only lock, then that's an error.
    if (_coordinatorLiveness->getNumDead() > 0) {
        for (const auto& lock : locks) {
            // TODO: if the array residency does not include the dead
            // instance(s), we will still fail but should not.
            if (lock->getLockMode() > LockDesc::RD) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
            }
        }
    }

    try {
        SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, this);
        SystemCatalog::getInstance()->lockArrays(locks, errorChecker);

        validate();

        // get the array metadata catalog version, i.e. 'timestamp' the arrays in use by this query
        if (!locks.empty()) {
            SystemCatalog::getInstance()->getCurrentVersion(locks);
        }
    } catch (const scidb::LockBusyException& e) {
        throw;
    } catch (std::exception&) {
        releaseLocks(shared_from_this());
        throw;
    }
    if (_logger->isDebugEnabled()) {
        LOG4CXX_DEBUG(_logger, "Acquired "<< locks.size() << " array locks for query " << _queryID);
        for (auto & lock : locks)
        {
            LOG4CXX_DEBUG(_logger, "Acquired lock: " << lock->toString());
        }
    }
}

ArrayID
Query::getCatalogVersion(
    const std::string& namespaceName,
    const std::string& arrayName,
    bool allowMissing) const
{
    // Currently synchronization is not used because this is called
    // either strictly before or strictly after the query array lock
    // acquisition on the coordinator.  -- tigor
    assert(isCoordinator());

    if (_requestedLocks.empty() ) {
        // we have not acquired the locks yet
        return SystemCatalog::ANY_VERSION;
    }
    const std::string* unversionedNamePtr(&arrayName);
    std::string unversionedName;
    if (!ArrayDesc::isNameUnversioned(arrayName) ) {
        unversionedName = ArrayDesc::makeUnversionedName(arrayName);
        unversionedNamePtr = &unversionedName;
    }

    // Look up lock for this array by name, and return its catalogVersion.  It's OK to use
    // INVALID_MODE because this LockDesc object is only for lookup purposes, and the mode isn't
    // used by comparator.
    std::shared_ptr<LockDesc> key(
        make_shared<LockDesc>(
            namespaceName,
            (*unversionedNamePtr),
            getQueryID(),
            Cluster::getInstance()->getLocalInstanceId(),
            LockDesc::COORD,
            LockDesc::INVALID_MODE));

    QueryLocks::const_iterator iter = _requestedLocks.find(key);
    if (iter == _requestedLocks.end() && allowMissing) {
        return SystemCatalog::ANY_VERSION;
    }
    ASSERT_EXCEPTION(iter!=_requestedLocks.end(),
                     string("Query::getCatalogVersion: unlocked array: ")+arrayName);
    const std::shared_ptr<LockDesc>& lock = (*iter);
    assert(lock->isLocked());
    return lock->getArrayCatalogId();
}

ArrayID Query::getCatalogVersion(
    const std::string&      arrayName,
    bool                    allowMissing) const
{
    string ns, arr;
    ArrayDesc::splitQualifiedArrayName(arrayName, ns, arr);
    if (ns.empty()) {
        ns = getNamespaceName();
    }
    return getCatalogVersion(ns, arr, allowMissing);
}


uint64_t Query::getLockTimeoutNanoSec()
{
    static const uint64_t WAIT_LOCK_TIMEOUT_MSEC = 2000;
    const uint64_t msec = _rng()%WAIT_LOCK_TIMEOUT_MSEC + 1;
    const uint64_t nanosec = msec*1000000;
    return nanosec;
}

void Query::waitForSystemCatalogLock()
{
    Thread::nanoSleep(getLockTimeoutNanoSec());
}

void Query::setQueryPerThread(const std::shared_ptr<Query>& query)
{
    _queryPerThread = query;
}

std::shared_ptr<Query> Query::getQueryPerThread()
{
    return _queryPerThread.lock();
}

void  Query::resetQueryPerThread()
{
    _queryPerThread.reset();
}

void Query::perfTimeAdd(const perfTimeWait_t tw, const double sec)
{
    assert(tw >= 0);
    assert(tw < PTW_NUM);
    _twUsecs[tw] += int64_t(sec*1.0e6);                   // NB: can record separate from execute
}

uint64_t Query::getActiveTimeMicroseconds() const
{
    SCIDB_ASSERT(_twUsecs[PTW_SPCL_ACTIVE] >= 0);
    return static_cast<uint64_t>(_twUsecs[PTW_SPCL_ACTIVE]);
}

const ProcGrid* Query::getProcGrid() const
{
    // locking to ensure a single allocation
    // XXX TODO: consider always calling Query::getProcGrid() in MpiManager::checkAndSetCtx
    //           that should guarantee an atomic creation of _procGrid
    ScopedMutexLock lock(const_cast<Mutex&>(errorMutex), PTW_SML_QUERY);
    // logically const, but we made _procGrid mutable to allow the caching
    // NOTE: Tigor may wish to push this down into the MPI context when
    //       that code is further along.  But for now, Query is a fine object
    //       on which to cache the generated procGrid
    if (!_procGrid) {
        _procGrid = new ProcGrid(safe_static_cast<procNum_t>(getInstancesCount()));
    }
    return _procGrid;
}

void Query::listLiveInstances(InstanceVisitor& func)
{
    assert(func);
    ScopedMutexLock lock(errorMutex, PTW_SML_QUERY);  //XXX TODO: remove lock ?

    for (vector<InstanceID>::const_iterator iter = _liveInstances.begin();
         iter != _liveInstances.end(); ++iter) {
        std::shared_ptr<Query> thisQuery(shared_from_this());
        func(thisQuery, (*iter));
    }
}

void Query::attachSession(const std::shared_ptr<Session> &session)
{
    SCIDB_ASSERT(session);
    _session = session;
}

string Query::getNamespaceName() const
{
    if (_session) {
        const string& ns = _session->getNamespace().getName();
        if (!ns.empty()) {
            return ns;
        }
    }
    return rbac::PUBLIC_NS_NAME;
}

void Query::getNamespaceArrayNames(
    const std::string &         qualifiedName,
    std::string &               namespaceName,
    std::string &               arrayName) const
{
    namespaceName = getNamespaceName();
    scidb::ArrayDesc::splitQualifiedArrayName(qualifiedName, namespaceName, arrayName);
}



ReplicationContext::ReplicationContext(const std::shared_ptr<Query>& query, size_t nInstances)
: _query(query)
#ifndef NDEBUG // for debugging
,_chunkReplicasReqs(nInstances)
#endif
{
    // ReplicatonManager singleton is initialized at startup time
    if (_replicationMngr == NULL) {
        _replicationMngr = NewReplicationManager::getInstance();
    }
}

ReplicationContext::QueueInfoPtr ReplicationContext::getQueueInfo(ArrayID id)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    QueueInfoPtr& qInfo = _inboundQueues[id];
    if (!qInfo) {
        int size = Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE);
        assert(size>0);
        size = (size<1) ? 4 : size+4; // allow some minimal extra space to tolerate mild overflows
        NetworkManager& netMgr = *NetworkManager::getInstance();
        qInfo = std::make_shared<QueueInfo>(netMgr.createWorkQueue("ReplicatonContextWorkQueue",
                                                                   1, static_cast<uint64_t>(size)));
        assert(!qInfo->getArray());
        assert(qInfo->getQueue());
        qInfo->getQueue()->stop();
    }
    assert(qInfo->getQueue());
    return qInfo;
}

void ReplicationContext::enableInboundQueue(ArrayID aId, const std::shared_ptr<Array>& array)
{
    assert(array);
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
    QueueInfoPtr qInfo = getQueueInfo(aId);
    assert(qInfo);
    std::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    qInfo->setArray(array);
    wq->start();
}

std::shared_ptr<scidb::WorkQueue> ReplicationContext::getInboundQueue(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
    QueueInfoPtr qInfo = getQueueInfo(aId);
    assert(qInfo);
    std::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    return wq;
}

std::shared_ptr<scidb::Array> ReplicationContext::getPersistentArray(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
    QueueInfoPtr qInfo = getQueueInfo(aId);
    assert(qInfo);
    std::shared_ptr<scidb::Array> array = qInfo->getArray();
    assert(array);
    assert(qInfo->getQueue());
    return array;
}

void ReplicationContext::removeInboundQueue(ArrayID aId)
{
    // tigor:
    // Currently, we dont remove the queue until the query is destroyed.
    // The reason for this was that we did not have a sync point, and
    // each instance was not waiting for the INCOMING replication to finish.
    // But we now have a sync point here, to coordinate the storage manager
    // fluhes.  So we may be able to implement queue removal in the future.

    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    syncBarrier(0, query);
    syncBarrier(1, query);
}

namespace {
void generateReplicationItems(std::shared_ptr<MessageDesc>& msg,
                              NewReplicationManager::ItemVector* replicaVec,
                              const std::shared_ptr<Query>& query,
                              InstanceID physInstanceId)
{
    if (physInstanceId == query->getPhysicalInstanceID()) {
        return;
    }
    std::shared_ptr<NewReplicationManager::Item> item(new NewReplicationManager::Item(physInstanceId, msg, query));
    replicaVec->push_back(item);
}
}

void ReplicationContext::replicationSync(ArrayID arrId)
{
    assert(arrId > 0);
    std::shared_ptr<MessageDesc> msg = std::make_shared<MessageDesc>(mtChunkReplica);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = msg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_array_id(arrId);
    // tell remote instances that we are done replicating
    chunkRecord->set_eof(true);

    assert(_replicationMngr);
    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    msg->setQueryID(query->getQueryID());

    NewReplicationManager::ItemVector replicasVec;
    Query::InstanceVisitor f =
        boost::bind(&generateReplicationItems, msg, &replicasVec, _1, _2);
    query->listLiveInstances(f);

    assert(replicasVec.size() == (query->getInstancesCount()-1));
    for (auto const& item : replicasVec) {
        _replicationMngr->send(item);
    }
    for (auto const& item : replicasVec) {
        _replicationMngr->wait(item);
        assert(item->isDone());
        ASSERT_EXCEPTION(!item->hasError(), "Error sending replica sync!");
    }

    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
        qInfo = getQueueInfo(arrId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // wait for all to ack our eof
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    qInfo->getSemaphore().enter(replicasVec.size(), ec, PTW_SEM_REP);
}

void ReplicationContext::replicationAck(InstanceID sourceId, ArrayID arrId)
{
    assert(arrId > 0);
    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex, PTW_SML_QUERY);
        qInfo = getQueueInfo(arrId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // sourceId acked our eof
    qInfo->getSemaphore().release();
}

/// cached pointer to the NewReplicationManager singeton
NewReplicationManager*  ReplicationContext::_replicationMngr;

void ReplicationContext::enqueueInbound(ArrayID arrId, std::shared_ptr<Job>& job)
{
    assert(job);
    assert(arrId>0);
    assert(job->getQuery());
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY);

    std::shared_ptr<WorkQueue> queryQ(getInboundQueue(arrId));

    if (Query::_logger->isTraceEnabled()) {
        std::shared_ptr<Query> query(job->getQuery());
        LOG4CXX_TRACE(Query::_logger, "ReplicationContext::enqueueInbound"
                      <<" job="<<job.get()
                      <<", queue="<<queryQ.get()
                      <<", arrId="<<arrId
                      << ", queryID="<<query->getQueryID());
    }
    assert(_replicationMngr);
    try {
        WorkQueue::WorkItem item = _replicationMngr->getInboundReplicationItem(job);
        queryQ->enqueue(item);
    } catch (const WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(Query::_logger, "ReplicationContext::enqueueInbound"
                      << ": Overflow exception from the message queue (" << queryQ.get()
                      << "): "<<e.what());
        std::shared_ptr<Query> query(job->getQuery());
        assert(query);
        assert(false);
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        query->handleError(e.copy());
        throw;
    }
}

} // namespace
