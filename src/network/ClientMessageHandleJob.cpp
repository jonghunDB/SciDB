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
 * ClientMessageHandleJob.cpp
 *
 *  Modified on: May 18, 2015
 *      Author: mcorbett@paradigm4.com
 *      Purpose:  Basic Security enhancements
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */
#include "ClientMessageHandleJob.h"

#include <memory>
#include <time.h>


#include <boost/date_time/posix_time/posix_time.hpp>
#include <log4cxx/logger.h>

#include <SciDBAPI.h>

#include <array/Metadata.h>
#include <array/CompressedBuffer.h>

#include <network/NetworkManager.h>
#include <network/MessageUtils.h>
#include <network/Connection.h>

#include <query/RemoteMergedArray.h>
#include <query/QueryProcessor.h>
#include <query/executor/ScopedQueryThread.h>
#include <query/executor/SciDBExecutor.h>
#include <query/Serialize.h>

#include <system/Exceptions.h>
#include <system/Warnings.h>

#include <util/Mutex.h>
#include <rbac/Session.h>

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network.msgs"));

ClientMessageHandleJob::ClientMessageHandleJob(
    const std::shared_ptr<Connection>  & connection,
    const std::shared_ptr<MessageDesc> & messageDesc)
    : MessageHandleJob(messageDesc)
    , _connection(connection)
{
    assert(connection);
    assert(messageDesc);
}

void ClientMessageHandleJob::run()
{
   assert(isScidbMessage(_messageDesc->getMessageType()));
   MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
   LOG4CXX_TRACE(logger, "Starting client message handling: type=" << messageType)

   ASSERT_EXCEPTION(_currHandler, "ClientMessageJob handler is not set");
   _currHandler();

   LOG4CXX_TRACE(logger, "Finishing client message handling: type=" << messageType)
}

void
ClientMessageHandleJob::executeSerially(std::shared_ptr<WorkQueue>& serialQueue,
                                        std::weak_ptr<WorkQueue>& initialQueue,
                                       const scidb::Exception* error)
{
    static const char *funcName="ClientMessageHandleJob::handleReschedule: ";

    if (dynamic_cast<const scidb::ClientMessageHandleJob::CancelChunkFetchException*>(error)) {
        serialQueue->stop();
        LOG4CXX_TRACE(logger, funcName << "Serial queue "<<serialQueue.get()<<" is stopped");
        serialQueue.reset();
        if (std::shared_ptr<WorkQueue> q = initialQueue.lock()) {
            q->unreserve();
        }
        return;
    }

    if (error) {
        LOG4CXX_ERROR(logger, funcName << "Error: "<<error);
        arena::ScopedArenaTLS arenaTLS(getQuery()->getArena());
        getQuery()->handleError(error->copy());
    }

    std::shared_ptr<Job> fetchJob(shared_from_this());
    WorkQueue::WorkItem work = boost::bind(&Job::executeOnQueue, fetchJob, _1, _2);
    assert(work);
    try
    {
        serialQueue->enqueue(work);
    }
    catch (const WorkQueue::OverflowException& e)
    {
        // as long as there is at least one item in the queue, we are OK
        LOG4CXX_TRACE(logger, funcName << "Serial queue is full, dropping request");
    }
}

ClientMessageHandleJob::RescheduleCallback
ClientMessageHandleJob::getSerializeCallback(std::shared_ptr<WorkQueue>& serialQueue)
{
    std::shared_ptr<WorkQueue> thisQ(_wq.lock());
    ASSERT_EXCEPTION(thisQ.get()!=nullptr, "ClientMessageHandleJob::getSerializeCallback: current work queue is deallocated");
    std::shared_ptr<ClientMessageHandleJob> thisJob(std::dynamic_pointer_cast<ClientMessageHandleJob>(shared_from_this()));

    const uint32_t cuncurrency = 1;
    const uint32_t depth = 2;
    serialQueue = NetworkManager::getInstance()->createWorkQueue("ClientMessageWorkQueue", cuncurrency, depth);
    serialQueue->stop();

    ClientMessageHandleJob::RescheduleCallback func =
       boost::bind(&ClientMessageHandleJob::executeSerially, thisJob,
                   serialQueue, _wq, _1);

    thisQ->reserve(thisQ);
    return func;
}

void
ClientMessageHandleJob::handleQueryError(RescheduleCallback& cb,
                                         Notification<scidb::Exception>::MessageTypePtr errPtr)
{
    assert(!dynamic_cast<const scidb::ClientMessageHandleJob::CancelChunkFetchException*>(errPtr.get()));
    assert(cb);
    if (errPtr->getQueryId() != _query->getQueryID()) {
        return;
    }
    cb(errPtr.get());
}

void
ClientMessageHandleJob::fetchChunk()
{
    static const char *funcName="ClientMessageHandleJob::fetchChunk: ";
    const QueryID queryID = _messageDesc->getQueryID();
    try
    {
        _query = Query::getQueryByID(queryID);
        ScopedActiveQueryThread saqt(_query); // _query is set appropriately
        arena::ScopedArenaTLS arenaTLS(_query->getArena());
        _query->validate();

        std::shared_ptr<scidb_msg::Fetch> fetchRecord = _messageDesc->getRecord<scidb_msg::Fetch>();

        ASSERT_EXCEPTION((fetchRecord->has_attribute_id()), funcName);
        AttributeID attributeId = fetchRecord->attribute_id();
        const string arrayName = fetchRecord->array_name();

        LOG4CXX_TRACE(logger, funcName << "Fetching chunk attId= " << attributeId << ", queryID=" << queryID );

        std::shared_ptr<Array> fetchArray = _query->getCurrentResultArray();

        const uint32_t invalidArrayType(~0);
        validateRemoteChunkInfo(fetchArray.get(),
                                _messageDesc->getMessageType(),
                                invalidArrayType,
                                attributeId,
                                CLIENT_INSTANCE);

        std::shared_ptr<RemoteMergedArray> mergedArray = std::dynamic_pointer_cast<RemoteMergedArray>(fetchArray);
        if (mergedArray != NULL) {
            std::shared_ptr<WorkQueue> serialQueue;
            Notification<scidb::Exception>::SubscriberID queryErrorSubscriberID(0);
            // Set up this job for async execution
            RemoteMergedArray::RescheduleCallback cb;
            try {
                // create a functor which serializes the execution(s) of this job
                cb = getSerializeCallback(serialQueue);
                assert(cb);
                assert(serialQueue);
                assert(!serialQueue->isStarted());

                // create and register a listener that will kick off this job if query error happens
                Notification<scidb::Exception>::Subscriber listener =
                   boost::bind(&ClientMessageHandleJob::handleQueryError, this, cb, _1);
                queryErrorSubscriberID = Notification<scidb::Exception>::subscribe(listener);
                _query->validate(); // to make sure we have not just missed the notification

                // prepare this job for the next execution
                Handler h = boost::bind(&ClientMessageHandleJob::fetchMergedChunk, this, mergedArray,
                                           attributeId, queryErrorSubscriberID);
                _currHandler.swap(h);
                assert(_currHandler);

                // register the functor with the array so that it can kick it off when remote messages arrive
                mergedArray->resetCallback(attributeId, cb);
                // finally enqueue & run this job ...
                cb(NULL);
                serialQueue->start();
            } catch (const Exception& e) {
                // well ... undo everything
                Notification<scidb::Exception>::unsubscribe(queryErrorSubscriberID);
                mergedArray->resetCallback(attributeId);
                if (cb) {
                    CancelChunkFetchException ccfe(REL_FILE, __FUNCTION__, __LINE__);
                    cb(&ccfe);
                }
                throw;
            }
            return;
        }

        std::shared_ptr<MessageDesc> chunkMsg;
        std::shared_ptr< ConstArrayIterator> iter = fetchArray->getConstIterator(attributeId);
        if (!iter->end()) {
            const ConstChunk* chunk = &iter->getChunk();
            assert(chunk);
            populateClientChunk(arrayName, attributeId, chunk, chunkMsg);
            ++(*iter);
        } else {
            populateClientChunk(arrayName, attributeId, NULL, chunkMsg);
        }

        _query->validate();
        _connection->sendMessage(chunkMsg);

        LOG4CXX_TRACE(logger, funcName << "Chunk of arrayName= "<< arrayName
                     <<", attId="<< attributeId
                     << " queryID=" << queryID << " sent to client");
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, funcName << "Client's fetchChunk failed to complete queryID="
                      <<queryID<<" : " << e.what()) ;
        if (_query) {
            arena::ScopedArenaTLS arenaTLS(_query->getArena());
            _query->handleError(e.copy());
        }
        std::shared_ptr<MessageDesc> msg(makeErrorMessageFromExceptionForClient(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::fetchMergedChunk(std::shared_ptr<RemoteMergedArray>& fetchArray,
                                              AttributeID attributeId,
                                              Notification<scidb::Exception>::SubscriberID queryErrorSubscriberID)
{
    SCIDB_ASSERT(_query);
    ScopedActiveQueryThread saqt(_query); // _query is set appropriately
    arena::ScopedArenaTLS arenaTLS(_query->getArena());

    static const char *funcName="ClientMessageHandleJob::fetchMergedChunk: ";
    const QueryID queryID = _messageDesc->getQueryID();
    RemoteMergedArray::RescheduleCallback cb;
    try
    {
        ASSERT_EXCEPTION((queryID == _query->getQueryID()),
                         "Query ID mismatch in fetchMergedChunk");
        _query->validate();

        const string arrayName = _messageDesc->getRecord<scidb_msg::Fetch>()->array_name();
        std::shared_ptr<MessageDesc> chunkMsg;

        LOG4CXX_TRACE(logger,
                      funcName << "Processing chunk of arrayName= " << arrayName
                      <<", attId="<< attributeId
                      << " queryID=" << queryID);
        try
        {
            std::shared_ptr< ConstArrayIterator> iter =
                fetchArray->getConstIterator(attributeId);
            if (!iter->end()) {
                const ConstChunk* chunk = &iter->getChunk();
                assert(chunk);
                populateClientChunk(arrayName, attributeId, chunk, chunkMsg);
            } else {
                populateClientChunk(arrayName, attributeId, NULL, chunkMsg);
            }
        }
        catch (const scidb::MultiStreamArray::RetryException& )
        {
            LOG4CXX_TRACE(logger,
                          funcName << " reschedule arrayName= " << arrayName
                          << ", attId="<<attributeId
                          <<" queryID="<<queryID);
            return;
        }

        // This is the last execution of this job, tear down the async execution setup
        CancelChunkFetchException e(REL_FILE, __FUNCTION__, __LINE__);
        Notification<scidb::Exception>::unsubscribe(queryErrorSubscriberID);
        cb = fetchArray->resetCallback(attributeId);
        assert(cb);
        cb(&e);
        cb.clear();

        _query->validate();
        _connection->sendMessage(chunkMsg);

        LOG4CXX_TRACE(logger, funcName << "Chunk of arrayName= "<< arrayName
                     <<", attId="<< attributeId
                     << " queryID=" << queryID
                     << " sent to client");
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, funcName << "Client's fetchChunk failed to complete for"
                      <<" queryID="<<queryID<<" : " << e.what()) ;

        // Async setup teardown
        Notification<scidb::Exception>::unsubscribe(queryErrorSubscriberID);
        if (!cb) {
            cb = fetchArray->resetCallback(attributeId);
        }
        if (cb) {
            CancelChunkFetchException ccfe(REL_FILE, __FUNCTION__, __LINE__);
            cb(&ccfe);
        }
        if (_query) {
            _query->handleError(e.copy());
        }
        std::shared_ptr<MessageDesc> msg(makeErrorMessageFromExceptionForClient(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::populateClientChunk(const std::string& arrayName,
                                                 AttributeID attributeId,
                                                 const ConstChunk* chunk,
                                                 std::shared_ptr<MessageDesc>& chunkMsg)
{
    // called from fetch chunk, do not reset times

    static const char *funcName="ClientMessageHandleJob::populateClientChunk: ";
    std::shared_ptr<scidb_msg::Chunk> chunkRecord;
    if (chunk)
    {
        checkChunkMagic(*chunk, __PRETTY_FUNCTION__);
        std::shared_ptr<CompressedBuffer> buffer = std::make_shared<CompressedBuffer>();
        std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        chunk->compress(*buffer, emptyBitmap);
        chunkMsg = std::make_shared<MessageDesc>(mtChunk, buffer);
        chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        chunkRecord->set_eof(false);
        chunkRecord->set_compression_method(static_cast<int32_t>(buffer->getCompressionMethod()));
        chunkRecord->set_attribute_id(chunk->getAttributeDesc().getId());
        chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
        chunkMsg->setQueryID(_query->getQueryID());
        chunkRecord->set_count(chunk->isCountKnown() ? chunk->count() : 0);
        const Coordinates& coordinates = chunk->getFirstPosition(false);
        for (size_t i = 0; i < coordinates.size(); i++) {
            chunkRecord->add_coordinates(coordinates[i]);
        }
        LOG4CXX_TRACE(logger, funcName << "Prepared message with chunk at postion " 
                      <<CoordsToStr(coordinates)
                      <<", arrayName= "<< arrayName
                      <<", attId="<< attributeId
                      <<", queryID="<<_query->getQueryID());
    }
    else
    {
        chunkMsg = std::make_shared<MessageDesc>(mtChunk);
        chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        chunkMsg->setQueryID(_query->getQueryID());
        chunkRecord->set_eof(true);
        LOG4CXX_TRACE(logger, funcName
                      << "Prepared message with information that there are "
                      << "no unread chunks (EOF)"
                      <<", arrayName= "<< arrayName
                      <<", attId="<< attributeId
                      <<", queryID="<<_query->getQueryID());
    }

    if (_query->getWarnings().size())
    {
        //Propagate warnings gathered on coordinator to client
        vector<Warning> v = _query->getWarnings();
        for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
        {
            ::scidb_msg::Chunk_Warning* warn = chunkRecord->add_warnings();
            warn->set_code(it->getCode());
            warn->set_file(it->getFile());
            warn->set_function(it->getFunction());
            warn->set_line(it->getLine());
            warn->set_what_str(it->msg());
            warn->set_strings_namespace(it->getStringsNamespace());
            warn->set_stringified_code(it->getStringifiedCode());
        }
        _query->clearWarnings();
    }
}

void ClientMessageHandleJob::prepareClientQuery()
{
    std::shared_ptr<Query> nullPtr;
    ScopedActiveQueryThread saqt(nullPtr);  // _query not set

    assert(_connection);
    ASSERT_EXCEPTION(_connection.get()!=nullptr, "NULL connection");

    scidb::QueryResult queryResult;
    scidb::SciDB& scidb = getSciDBExecutor();
    try
    {
        queryResult.queryID = Query::generateID();
        SCIDB_ASSERT(queryResult.queryID.isValid());
        _connection->attachQuery(queryResult.queryID);

        // Getting needed parameters for execution
        std::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
        const string queryString = record->query();
        bool afl = record->afl();
        const string programOptions = _connection->getRemoteEndpointName() + ' ' + record->program_options();

        SCIDB_ASSERT(queryResult.queryID.isValid());
        try
        {
            // create, parse, and prepare query
            scidb.prepareQuery(
                queryString,
                afl,
                programOptions,
                queryResult,
                &_connection);

            _query = Query::getQueryByID(queryResult.queryID);
            Query::setQueryPerThread(_query);  // now exists
        }
        catch (const scidb::LockBusyException& e)
        {
            Handler h = boost::bind(
                    &ClientMessageHandleJob::retryPrepareQuery,
                    this, queryResult/*copy*/);
            _currHandler.swap(h);
            assert(_currHandler);
            reschedule(Query::getLockTimeoutNanoSec()/1000);
            return;
        }
        postPrepareQuery(queryResult);
    }
    catch (const Exception& e)
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());
        LOG4CXX_ERROR(logger, "prepareClientQuery failed to complete for queryID="
                      << queryResult.queryID<< " : " << e.what());
        scidb::SciDB& scidb = getSciDBExecutor();
        handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}


void ClientMessageHandleJob::retryPrepareQuery(scidb::QueryResult& queryResult)
{
    std::shared_ptr<Query> nullPtr;
    ScopedActiveQueryThread saqt(nullPtr);  // _query not set

    SCIDB_ASSERT(queryResult.queryID.isValid());
    scidb::SciDB& scidb = getSciDBExecutor();
    try {
        // Getting needed parameters for execution
        std::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
        const string queryString = record->query();
        bool afl = record->afl();
        const string programOptions = _connection->getRemoteEndpointName() + ' ' + record->program_options();
        try
        {
            scidb.retryPrepareQuery(queryString, afl, programOptions, queryResult);
            _query = Query::getQueryByID(queryResult.queryID);
            Query::setQueryPerThread(_query); // now exists
        }
        catch (const scidb::LockBusyException& e)
        {
            Handler h = boost::bind(&ClientMessageHandleJob::retryPrepareQuery, this, queryResult/*copy*/);
            _currHandler.swap(h);
            assert(_currHandler);
            assert(_timer);
            reschedule(Query::getLockTimeoutNanoSec()/1000);
            return;
        }
        postPrepareQuery(queryResult);
    }
    catch (const Exception& e)
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());
        LOG4CXX_ERROR(logger, "retryPrepareClientQuery failed to complete for queryID="
                      << queryResult.queryID << " : " << e.what());
        scidb::SciDB& scidb = getSciDBExecutor();
        handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::postPrepareQuery(scidb::QueryResult& queryResult)
{
    SCIDB_ASSERT(queryResult.queryID.isValid());
    _timer.reset();

    // Creating message with result for sending to client
    std::shared_ptr<MessageDesc> resultMessage = make_shared<MessageDesc>(mtQueryResult);
    std::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
    resultMessage->setQueryID(queryResult.queryID);
    queryResultRecord->set_explain_logical(queryResult.explainLogical);
    queryResultRecord->set_selective(queryResult.selective);
    queryResultRecord->set_exclusive_array_access(queryResult.requiresExclusiveArrayAccess);

    SCIDB_ASSERT(getQuery());
    SCIDB_ASSERT(getQuery()->getQueryID() == queryResult.queryID);
    vector<Warning> v = getQuery()->getWarnings();
    for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
    {
        ::scidb_msg::QueryResult_Warning* warn = queryResultRecord->add_warnings();

        cout << "Propagate warning during prepare" << endl;
        warn->set_code(it->getCode());
        warn->set_file(it->getFile());
        warn->set_function(it->getFunction());
        warn->set_line(it->getLine());
        warn->set_what_str(it->msg());
        warn->set_strings_namespace(it->getStringsNamespace());
        warn->set_stringified_code(it->getStringifiedCode());
    }
    getQuery()->clearWarnings();

    for (vector<string>::const_iterator it = queryResult.plugins.begin();
         it != queryResult.plugins.end(); ++it)
    {
        queryResultRecord->add_plugins(*it);
    }
    sendMessageToClient(resultMessage);
    LOG4CXX_DEBUG(logger, "The result preparation of query is sent to the client")
}

void ClientMessageHandleJob::handleExecuteOrPrepareError(const Exception& err,
                                                         const scidb::QueryResult& queryResult,
                                                         scidb::SciDB& scidb)
{
    SCIDB_ASSERT(_connection);
    try {
        if (queryResult.queryID.isValid()) {
            try {
                scidb.cancelQuery(queryResult.queryID);
                _connection->detachQuery(queryResult.queryID);
            } catch (const scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND
                    && e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND2) {
                    throw;
                }
            }
        }
        reportErrorToClient(err);
    } catch (const scidb::Exception& e) {
        try { _connection->disconnect(); } catch (...) {}
        throw;
    }
}

void ClientMessageHandleJob::reportErrorToClient(const Exception& err)
{
    std::shared_ptr<MessageDesc> msg(makeErrorMessageFromExceptionForClient(err,INVALID_QUERY_ID));
    sendMessageToClient(msg);
}

void ClientMessageHandleJob::sendMessageToClient(std::shared_ptr<MessageDesc>& msg)
{
    assert(_connection);
    assert(msg);
    _connection->sendMessageDisconnectOnError(msg);
}

namespace {

/// This is used as a query continuation routine,
/// which runs on the networking thread (ServerMessage::dispatch()).
void
handlePhysPlanOnCoordinator(std::shared_ptr<Job>& job,
                            std::shared_ptr<WorkQueue>& toQueue,
                            std::shared_ptr<SerializationCtx>& sCtx,
                            Notification<scidb::Exception>::SubscriberID& queryErrorSubscriberID,
                            const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(query->isCoordinator());
    // unregister query error notification listener
    Notification<scidb::Exception>::unsubscribe(queryErrorSubscriberID);
    WorkQueue::scheduleReserved(job, toQueue, sCtx);
}

/// This is used as a query error-handling routine,
/// which runs on the networking thread
void
handlePhysPlanQueryError(const std::shared_ptr<Query>& query,
                         Notification<scidb::Exception>::MessageTypePtr errPtr)
{
    if (errPtr->getQueryId() != query->getQueryID()) {
        return;
    }
    SCIDB_ASSERT(query->isCoordinator());
    Query::Continuation cont;
    query->swapContinuation(cont);
    if (cont) {
        cont(query);
    }
}

void noOp() {}

} //namespace

void ClientMessageHandleJob::executeClientQuery()
{
    std::shared_ptr<Query> nullPtr;
    ScopedActiveQueryThread saqt(nullPtr);  // _query not set

    // TODO: calling the executor class "SciDB" is not helpful, rename it Executor
    scidb::SciDB& scidb = getSciDBExecutor();
    std::shared_ptr<scidb::QueryResult> queryResultPtr = std::make_shared<scidb::QueryResult>();
    scidb::QueryResult& queryResult = *queryResultPtr;
    try
    {
        ASSERT_EXCEPTION(_connection.get()!=nullptr, "NULL connection");

        if (!_connection->getSession()) {
            _connection->disconnect();
            return;
        }

        std::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();

        const string queryString = record->query();
        bool afl = record->afl();
        queryResult.queryID = _messageDesc->getQueryID();

        if (!queryResult.queryID.isValid()) {
            // make a query object
            const string programOptions = _connection->getRemoteEndpointName() + ' ' + record->program_options();
            queryResult.queryID = Query::generateID();
            SCIDB_ASSERT(queryResult.queryID.isValid());
            _connection->attachQuery(queryResult.queryID);
            try
            {
                // creates the query
                scidb.prepareQuery(
                    queryString,
                    afl,
                    programOptions,
                    queryResult,
                    &_connection);

                _query = Query::getQueryByID(queryResult.queryID);
                Query::setQueryPerThread(_query); // now exists

                ASSERT_EXCEPTION(_query.get()!=nullptr, "NULL query");
                ASSERT_EXCEPTION(
                    _query->isCoordinator(),
                    "NULL query->isCoordinator()");
            }
            catch (const scidb::LockBusyException& e)
            {
                Handler h = boost::bind(
                        &ClientMessageHandleJob::retryExecuteQuery,
                        this, queryResult/*copy*/);
                _currHandler.swap(h);
                assert(_currHandler);
                reschedule(Query::getLockTimeoutNanoSec()/1000);
                return;
            }
        } else {
            _query = Query::getQueryByID(queryResult.queryID);
            Query::setQueryPerThread(_query); // now exists
        }

        SCIDB_ASSERT(queryResult.queryID.isValid());
        SCIDB_ASSERT(getQuery()->getQueryID() == queryResult.queryID);
        SCIDB_ASSERT(getQuery()->queryString == queryString);
        SCIDB_ASSERT(Query::getQueryPerThread() == getQuery());

        // prepare for a callback
        SCIDB_ASSERT(_currStateMutex.isLockedByThisThread());
        // no swap is ok because 'this' is pinned by the execution queue
        // and we dont take any arguments
        _currHandler = &noOp;
        setPhysPlanContinuation(getQuery());

        scidb.startExecuteQuery(queryString, afl, queryResult);

        Handler h = boost::bind(
                &ClientMessageHandleJob::completeExecuteQuery,
                this, queryResultPtr);
        _currHandler.swap(h);
        SCIDB_ASSERT(_currHandler);
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, "executeClientQuery failed to complete for queryID="
                      << queryResult.queryID << " : " << e.what());
        handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void
ClientMessageHandleJob::setPhysPlanContinuation(const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(_currStateMutex.isLockedByThisThread());

    std::shared_ptr<WorkQueue> toQ(_wq.lock());
    assert(toQ);
    std::shared_ptr<SerializationCtx> sCtx(_wqSCtx.lock());
    assert(sCtx);
    std::shared_ptr<Job> thisJob(shared_from_this());

    Notification<scidb::Exception>::SubscriberID queryErrorSubscriberID;
    // create and register a listener that will kick off this job if query error happens
    Notification<scidb::Exception>::Subscriber listener =
            boost::bind(&handlePhysPlanQueryError, query, _1);

    bool unreserveOnError = true;
    // continue on the same queue
    toQ->reserve(toQ);
    try
    {
       queryErrorSubscriberID = Notification<scidb::Exception>::subscribe(listener);

       Query::Continuation func =
               boost::bind(&handlePhysPlanOnCoordinator,
                           thisJob,
                           toQ,
                           sCtx,
                           queryErrorSubscriberID,
                           _1);

       query->swapContinuation(func);
       SCIDB_ASSERT(!func);
       unreserveOnError = false;

       query->validate(); // to make sure we have not just missed the notification
    }
    catch (const scidb::Exception& e)
    {
        // undo everything
        Notification<scidb::Exception>::unsubscribe(queryErrorSubscriberID);
        Query::Continuation cont;
        query->swapContinuation(cont);
        SCIDB_ASSERT(!cont || (cont && !unreserveOnError));
        if (cont || unreserveOnError) {
            toQ->unreserve();
        }
        throw;
    }
}

void ClientMessageHandleJob::completeExecuteQuery(const std::shared_ptr<scidb::QueryResult>& queryResultPtr)
{
    std::shared_ptr<Query> nullPtr;
    ScopedActiveQueryThread saqt(nullPtr);  // _query not set

    SCIDB_ASSERT(queryResultPtr);
    scidb::QueryResult& queryResult = *queryResultPtr;

    // TODO: calling the executor class "SciDB" is not helpful, rename it Executor
    scidb::SciDB& scidb = getSciDBExecutor();
    try
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());
        SCIDB_ASSERT(getQuery());
        SCIDB_ASSERT(getQuery()->getQueryID() == queryResult.queryID);
        Query::setQueryPerThread(getQuery());  // now exists

        getQuery()->validate();

        ASSERT_EXCEPTION(getQuery()->isCoordinator() ,
                         "Non-coordinator must not run ClientMessageHandleJob::completeExecuteQuery");
        ASSERT_EXCEPTION(_connection.get()!=nullptr, "NULL connection");

        if (!_connection->getSession()) {
            _connection->disconnect();
            ASSERT_EXCEPTION_FALSE("Session must be already authenticated");
        }

        if (isDebug()) {
            std::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
            const string queryString = record->query();
            SCIDB_ASSERT(!queryString.empty());
            SCIDB_ASSERT(getQuery()->queryString == queryString);
        }

        scidb.completeExecuteQuery(queryResult, getQuery());

        postExecuteQueryInternal(queryResult, getQuery());
    }
    catch (const Exception& e)
    {
       LOG4CXX_ERROR(logger, "completeExecuteClientQuery failed to complete for queryID="
                     << queryResult.queryID<< " : " << e.what());
       handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::retryExecuteQuery(scidb::QueryResult& queryResult)
{
    std::shared_ptr<Query> nullPtr;
    ScopedActiveQueryThread saqt(nullPtr);  // _query not set

    SCIDB_ASSERT(queryResult.queryID.isValid());
    scidb::SciDB& scidb = getSciDBExecutor();
    Handler tmpHandler = &noOp;
    try
    {
        std::shared_ptr<scidb_msg::Query> record = _messageDesc->getRecord<scidb_msg::Query>();
        const string queryString = record->query();
        bool afl = record->afl();
        const string programOptions = _connection->getRemoteEndpointName() + ' ' + record->program_options();
        try
        {
            scidb.retryPrepareQuery(queryString, afl, programOptions, queryResult);
            _query = Query::getQueryByID(queryResult.queryID);
            Query::setQueryPerThread(_query); // now exists
        }
        catch (const scidb::LockBusyException& e)
        {
            Handler h = boost::bind(&ClientMessageHandleJob::retryExecuteQuery, this, queryResult/*copy*/);
            _currHandler.swap(h);
            assert(_currHandler);
            assert(_timer);
            reschedule(Query::getLockTimeoutNanoSec()/1000);
            return;
        }

        SCIDB_ASSERT(queryResult.queryID.isValid());
        SCIDB_ASSERT(getQuery()->getQueryID() == queryResult.queryID);
        SCIDB_ASSERT(getQuery()->queryString == queryString);
        SCIDB_ASSERT(Query::getQueryPerThread() == getQuery());

        // prepare for a callback
        SCIDB_ASSERT(_currStateMutex.isLockedByThisThread());
        std::shared_ptr<QueryResult> queryResultPtr = std::make_shared<QueryResult>();

        (*queryResultPtr) = queryResult;
        SCIDB_ASSERT(queryResultPtr->queryID == queryResult.queryID);
        SCIDB_ASSERT(queryResult.queryID.isValid());

        _currHandler.swap(tmpHandler); // queryResult is held by tmpHandler now
        SCIDB_ASSERT(queryResultPtr->queryID == queryResult.queryID);
        SCIDB_ASSERT(queryResult.queryID.isValid());

        setPhysPlanContinuation(getQuery());

        scidb.startExecuteQuery(queryString, afl, *queryResultPtr);

        Handler h = boost::bind(
                &ClientMessageHandleJob::completeExecuteQuery,
                this, queryResultPtr);
        _currHandler.swap(h);
        SCIDB_ASSERT(_currHandler);
    }
    catch (const Exception& e)
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());
        LOG4CXX_ERROR(logger, "retryExecuteClient failed to complete for queryID="
                      << queryResult.queryID << " : " << e.what());
        handleExecuteOrPrepareError(e, queryResult, scidb);
    }
}

void ClientMessageHandleJob::postExecuteQueryInternal(scidb::QueryResult& queryResult,
                                                      const std::shared_ptr<Query>& query)

{
    _timer.reset();

    SCIDB_ASSERT(queryResult.queryID.isValid());

    // Creating message with result for sending to client
    std::shared_ptr<MessageDesc> resultMessage = std::make_shared<MessageDesc>(mtQueryResult);
    std::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
    resultMessage->setQueryID(queryResult.queryID);
    queryResultRecord->set_execution_time(queryResult.executionTime);
    queryResultRecord->set_explain_logical(queryResult.explainLogical);
    queryResultRecord->set_explain_physical(queryResult.explainPhysical);
    queryResultRecord->set_selective(queryResult.selective);
    queryResultRecord->set_auto_commit(queryResult.autoCommit);

    if (queryResult.selective)
    {
        const ArrayDesc& arrayDesc = queryResult.array->getArrayDesc();
        queryResultRecord->set_array_name(arrayDesc.getName());

        const Attributes& attributes = arrayDesc.getAttributes();
        for (size_t i = 0; i < attributes.size(); i++)
        {
            ::scidb_msg::QueryResult_AttributeDesc* attribute = queryResultRecord->add_attributes();

            attribute->set_id(attributes[i].getId());
            attribute->set_name(attributes[i].getName());
            attribute->set_type(attributes[i].getType());
            attribute->set_flags(attributes[i].getFlags());
            attribute->set_default_compression_method(attributes[i].getDefaultCompressionMethod());
            attribute->set_default_missing_reason(attributes[i].getDefaultValue().getMissingReason());
            attribute->set_default_value(string((char*)attributes[i].getDefaultValue().data(), attributes[i].getDefaultValue().size()));
        }

        const Dimensions& dimensions = arrayDesc.getDimensions();
        for (size_t i = 0; i < dimensions.size(); i++)
        {
            ::scidb_msg::QueryResult_DimensionDesc* dimension = queryResultRecord->add_dimensions();

            dimension->set_name(dimensions[i].getBaseName());
            dimension->set_start_min(dimensions[i].getStartMin());
            dimension->set_curr_start(dimensions[i].getCurrStart());
            dimension->set_curr_end(dimensions[i].getCurrEnd());
            dimension->set_end_max(dimensions[i].getEndMax());
            dimension->set_chunk_interval(dimensions[i].getRawChunkInterval());
            dimension->set_chunk_overlap(dimensions[i].getChunkOverlap());
        }
    }

    vector<Warning> v = query->getWarnings();
    for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
    {
        ::scidb_msg::QueryResult_Warning* warn = queryResultRecord->add_warnings();

        warn->set_code(it->getCode());
        warn->set_file(it->getFile());
        warn->set_function(it->getFunction());
        warn->set_line(it->getLine());
        warn->set_what_str(it->msg());
        warn->set_strings_namespace(it->getStringsNamespace());
        warn->set_stringified_code(it->getStringifiedCode());
    }
    query->clearWarnings();

    for (vector<string>::const_iterator it = queryResult.plugins.begin();
         it != queryResult.plugins.end(); ++it)
    {
        queryResultRecord->add_plugins(*it);
    }

    queryResult.array.reset();

    query->validate();

    sendMessageToClient(resultMessage);
    LOG4CXX_DEBUG(logger, "The result of query is sent to the client")
}

void ClientMessageHandleJob::cancelQuery()
{
    scidb::SciDB& scidb = getSciDBExecutor();

    const QueryID queryID = _messageDesc->getQueryID();
    try
    {
        scidb.cancelQuery(queryID);
        _connection->detachQuery(queryID);
        std::shared_ptr<MessageDesc> msg(makeOkMessage(queryID));
        sendMessageToClient(msg);
        LOG4CXX_TRACE(logger, "The query " << queryID << " execution was canceled")
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, "cancelQuery failed for queryID="<< queryID << " : "<< e.what());
        std::shared_ptr<MessageDesc> msg(makeErrorMessageFromExceptionForClient(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::completeQuery()
{
    scidb::SciDB& scidb = getSciDBExecutor();

    const QueryID queryID = _messageDesc->getQueryID();
    auto query = Query::getQueryByID(queryID);
    try
    {
        // ScopedActiveQueryThread must be destroyed
        // prior to query->perfTimeLog()
        ScopedActiveQueryThread saqt(query); // _query is not set

        scidb.completeQuery(queryID);
        _connection->detachQuery(queryID);
        std::shared_ptr<MessageDesc> msg(makeOkMessage(queryID));
        sendMessageToClient(msg);
        LOG4CXX_TRACE(logger, "The query " << queryID << " execution was completed")
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger,"completeQuery failed for queryID="<< queryID << " : "<< e.what());
        std::shared_ptr<MessageDesc> msg(makeErrorMessageFromExceptionForClient(e, queryID));
        sendMessageToClient(msg);
    }
    // saqt should be destroyed at this point
    assert(!Query::getQueryPerThread().get());

    // so we know that query will be up-to-date w.r.t. time logging
    // when it is destroyed, which is when it logs
}

void ClientMessageHandleJob::dispatch(NetworkManager* nm)
{
    assert(isScidbMessage(_messageDesc->getMessageType()));
    MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_TRACE(logger, "Dispatching client message " << strMsgType(messageType)
                  << " on conn=" << hex << _connection.get() << dec);
    const QueryID queryID = _messageDesc->getQueryID();

    try {
        ASSERT_EXCEPTION(_connection, "NULL connection");
        std::shared_ptr<Session> session = _connection->getSession();
        ASSERT_EXCEPTION(session, "No session, connection was not authenticated");

        LOG4CXX_TRACE(logger, "ClientMessageHandleJob::dispatch session priority="<<session->getPriority());
        setPriority(session->getPriority());

        switch (messageType)
        {
        case mtPrepareQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::prepareClientQuery, this);
            // can potentially block
            enqueue(nm->getRequestQueue(getPriority()));
        }
        break;
        case mtExecuteQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::executeClientQuery, this);
            // can potentially block
            enqueue(nm->getRequestQueue(getPriority()));
        }
        break;
        case mtFetch:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::fetchChunk, this);
            // can potentially block
            enqueue(nm->getRequestQueue(getPriority()));
        }
        break;
        case mtCompleteQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::completeQuery, this);
            enqueueOnErrorQueue(queryID);
        }
        break;
        case mtCancelQuery:
        {
            _currHandler=boost::bind(&ClientMessageHandleJob::cancelQuery, this);
            enqueueOnErrorQueue(queryID);
            break;
        }
        break;
        default:
        {
            LOG4CXX_ERROR(logger, "Unknown message type " << messageType);
            throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
        }
        }
        LOG4CXX_TRACE(logger, "Client message type=" << messageType <<" dispatched");
    }
    catch (const Exception& e)
    {
        LOG4CXX_ERROR(logger, "Dropping " << strMsgType(_messageDesc->getMessageType())
                      << " for queryID=" << _messageDesc->getQueryID()
                      << ", from CLIENT"
                      << " because " << e.what());
        std::shared_ptr<MessageDesc> msg(makeErrorMessageFromExceptionForClient(e, queryID));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::enqueue(const std::shared_ptr<WorkQueue>& q)
{
   LOG4CXX_TRACE(logger, "ClientMessageHandleJob::enqueue message of type="
                  <<  _messageDesc->getMessageType()
                  << ", for queryID=" << _messageDesc->getQueryID()
                  << ", from CLIENT");

    std::shared_ptr<Job> thisJob(shared_from_this());
    WorkQueue::WorkItem work = boost::bind(&Job::executeOnQueue, thisJob, _1, _2);

    try {
        q->enqueue(work);
    }
    catch (WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(logger, "Overflow exception from the message queue ("
                      << q.get() << "): " << e.what());
        std::shared_ptr<MessageDesc> msg(makeErrorMessageFromExceptionForClient(e, _messageDesc->getQueryID()));
        sendMessageToClient(msg);
    }
}

void ClientMessageHandleJob::enqueueOnErrorQueue(QueryID queryID)
{
    std::shared_ptr<Query> query = Query::getQueryByID(queryID);
    std::shared_ptr<WorkQueue> q = query->getErrorQueue();
    if (!q) {
        // if errorQueue is gone, the query must be deallocated at this point
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND) << queryID;
    }
    LOG4CXX_TRACE(logger, "Error queue size=" << q->size()
                  << " for queryID="<< queryID);
    enqueue(q);
}

} // namespace scidb
