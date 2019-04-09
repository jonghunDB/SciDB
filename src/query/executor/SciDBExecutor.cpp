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
 * @file SciDBExecutor.cpp
 *
 * @author  roman.simakov@gmail.com
 *
 * @brief SciDB API internal implementation to coordinate query execution.
 *
 * This implementation is used by server side of remote protocol and
 * can be loaded directly to user process and transform it into scidb instance.
 * Maybe useful for debugging and embedding scidb into users applications.
 */

#include <query/executor/SciDBExecutor.h>

#include <memory>
#include <string>
#include <boost/bind.hpp>
#include <boost/serialization/serialization.hpp>
#include <log4cxx/logger.h>

#include <SciDBAPI.h>
#include <network/Connection.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <network/MessageHandleJob.h>
#include <network/OrderedBcast.h>
#include <query/QueryPlan.h>
#include <query/QueryProcessor.h>
#include <query/Serialize.h>
#include <query/optimizer/Optimizer.h>
#include <system/Cluster.h>
#include <system/Config.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Session.h>
#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>

using namespace std;
using NsComm = scidb::namespaces::Communicator;

namespace scidb
{

class SessionProperties;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.executor"));

namespace syncUtils
{
        /// Validate query until timeout expires
        bool validateQueryWithTimeout(uint64_t startTime,
                                      uint64_t timeout,
                                      std::shared_ptr<Query>& query)
        {
            bool rc = query->validate();
            assert(rc);
            if (hasExpired(startTime, timeout)) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_RESOURCE_BUSY)
                        << "remote query processor not ready to execute, try again later";
            }
            return rc;
        }

        /// Notify the workers about the query
        void notify(std::shared_ptr<Query>& query)
        {
            ASSERT_EXCEPTION(query->isCoordinator(), "Only coordinator is supposed to call syncUtils::notify()");
            {
                LOG4CXX_DEBUG(logger, "Send message from coordinator for waiting instances in queryID: "
                              << query->getQueryID());

                std::shared_ptr<MessageDesc> msg = std::make_shared<Connection::ServerMessageDesc>();
                msg->initRecord(mtNotify);
                std::shared_ptr<scidb_msg::Liveness> record = msg->getRecord<scidb_msg::Liveness>();
                bool res = serializeLiveness(query->getCoordinatorLiveness(), record.get());
                SCIDB_ASSERT(res);
                msg->setQueryID(query->getQueryID());
                NetworkManager::getInstance()->broadcastLogical(msg);
            }
        }

        /// Wait for confirmations the workers about the query
        void wait(const std::shared_ptr<Query>& query, uint64_t timeoutNanoSec)
        {
            ASSERT_EXCEPTION(query->isCoordinator(), "Only coordinator is supposed to call syncUtils::wait()");
            {
                const size_t instancesCount = query->getInstancesCount() - 1;
                LOG4CXX_DEBUG(logger, "Waiting for notification in queryID from " << instancesCount << " instances");

                Semaphore::ErrorChecker errorChecker;
                if (timeoutNanoSec > 0) {
                    errorChecker = boost::bind(&validateQueryWithTimeout, getTimeInNanoSecs(), timeoutNanoSec, query);
                } else {
                    errorChecker = boost::bind(&Query::validate, query);
                }
                query->semResults.enter(instancesCount, errorChecker, PTW_SEM_RESULTS_QP);
            }
        }
}

/**
 * Engine implementation of the SciDBAPI interface
 */
class SciDBExecutor: public scidb::SciDB, public InjectedErrorListener
{
    public:

    SciDBExecutor()
    :
        InjectedErrorListener(InjectErrCode::QUERY_BROADCAST)
    {
    }
    virtual ~SciDBExecutor()
    {
        // no need to call InjectedErrorListener::stop();
    }

    void* connect(SessionProperties const& sessionProperties,
                  const std::string& connectionString,
                  uint16_t port) override
    {
        ASSERT_EXCEPTION_FALSE(
            "connect - not needed, to implement in engine");

        // Shutting down warning
        return NULL;
    }

    void disconnect(void* connection = NULL)
    {
        ASSERT_EXCEPTION(
            false,
            "disconnect - not needed, to implement in engine");
    }

    void fillUsedPlugins(const ArrayDesc& desc, vector<string>& plugins)
    {
        for (size_t i = 0; i < desc.getAttributes().size(); i++) {
            const string& libName = TypeLibrary::getTypeLibraries().getObjectLibrary(desc.getAttributes()[i].getType());
            if (libName != "scidb")
                plugins.push_back(libName);
        }
    }

    void prepareQuery(const std::string& queryString,
                      bool afl,
                      const std::string& programOptions,
                      QueryResult& queryResult,
                      void* connection)
    {
        ASSERT_EXCEPTION(connection, "NULL connection");

        // Chosen Query ID should *not* be already in use!
        if (Query::getQueryByID(queryResult.queryID, false)) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "SciDBExecutor::prepareQuery";
        }

        // Query string must be of reasonable length!
        size_t querySize = queryString.size();
        size_t maxSize = Config::getInstance()->getOption<size_t>(CONFIG_QUERY_MAX_SIZE);
        if (querySize > maxSize) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_TOO_BIG) << querySize << maxSize;
        }

        std::shared_ptr<Connection> &scidb_connection =
            *reinterpret_cast<std::shared_ptr<Connection> *>(connection);
        ASSERT_EXCEPTION(scidb_connection, "NULL scidb_connection");
        ASSERT_EXCEPTION(scidb_connection->getSession(), "Connection has no session");

        // Create local query object, tie it to our session!
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
        std::shared_ptr<Query> query = queryProcessor->createQuery(
            queryString,
            queryResult.queryID,
            scidb_connection->getSession());
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        ASSERT_EXCEPTION(
            queryResult.queryID == query->getQueryID(),
            "queryResult.queryID == query->getQueryID()");

        // register the query on the thread
        // so that the performance of everything after this can be tracked
        // its ugly that this can't be done by the caller
        // maybe the query should be created before its prepared
        // then we wouldn't have to assume the caller is the client thread (ugly)
        Query::setQueryPerThread(query);

        LOG4CXX_DEBUG(logger, "Parsing query(" << query->getQueryID() << "): "
            << " user_id=" << query->getSession()->getUser().getId()
            << " " << queryString << "");

        try {
            prepareQueryBeforeLocking(query, queryProcessor, afl, programOptions);
            query->acquireLocks(); //can throw "try-again", i.e. LockBusyException
            prepareQueryAfterLocking(query, queryProcessor, afl, queryResult);
        } catch (const scidb::LockBusyException& e) {
            e.raise();

        } catch (const Exception& e) {
            query->done(e.copy());
            e.raise();
        }
        LOG4CXX_DEBUG(logger, "Prepared query(" << query->getQueryID() << "): " << queryString << "");
    }

    virtual void retryPrepareQuery(const std::string& queryString,
                                 bool afl,
                                 const std::string& programOptions,
                                   QueryResult& queryResult)
    {
        std::shared_ptr<Query>  query = Query::getQueryByID(queryResult.queryID);
        arena::ScopedArenaTLS arenaTLS(query->getArena());

        assert(queryResult.queryID == query->getQueryID());
        try {

            query->retryAcquireLocks();  //can throw "try-again", i.e. LockBusyException

            std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

            prepareQueryAfterLocking(query, queryProcessor, afl, queryResult);

        } catch (const scidb::LockBusyException& e) {
            e.raise();

        } catch (const Exception& e) {
            query->done(e.copy());
            e.raise();
        }
        LOG4CXX_DEBUG(logger, "Prepared query(" << query->getQueryID() << "): " << queryString << "");
   }

    void prepareQueryBeforeLocking(std::shared_ptr<Query>& query,
                                   std::shared_ptr<QueryProcessor>& queryProcessor,
                                   bool afl,
                                   const std::string& programOptions)
    {
       query->validate();
       query->programOptions = programOptions;
       query->start();

       // Install the query on the workers
       syncUtils::notify(query);
       InjectedErrorListener::throwif(__LINE__, __FILE__);

       // first pass to collect the array names in the query
       queryProcessor->parseLogical(query, afl);

       // Collect lock requests and requests for access rights.
       queryProcessor->inferAccess(query);

       // Authorization: check inferred access rights.
       NsComm::checkAccess(query->getSession().get(), query->getRights());

       // Wait for the workers to install the query
       const uint64_t indefiniteTimeout=0;
       syncUtils::wait(query, indefiniteTimeout);
   }

    void prepareQueryAfterLocking(std::shared_ptr<Query>& query,
                                  std::shared_ptr<QueryProcessor>& queryProcessor,
                                  bool afl,
                                  QueryResult& queryResult)
    {
        query->validate();

        // second pass under the array locks
        queryProcessor->parseLogical(query, afl);
        LOG4CXX_TRACE(logger, "Query is parsed");

        const ArrayDesc& desc = queryProcessor->inferTypes(query);
        fillUsedPlugins(desc, queryResult.plugins);
        LOG4CXX_TRACE(logger, "Types of query are inferred");

        std::ostringstream planString;
        query->logicalPlan->toString(planString);
        queryResult.explainLogical = planString.str();

        queryResult.selective = !query->logicalPlan->getRoot()->isDdl();
        queryResult.requiresExclusiveArrayAccess = query->doesExclusiveArrayAccess();

        query->stop();
        LOG4CXX_DEBUG(logger, "The query is prepared");
   }

    /// @note DEPRECATED as of 16.x
    /// @see start/completeExecuteQuery(...)
    /// retained as a tested backup plan for at least one more release
    void executeQuery(const std::string& queryString, bool afl,
                      QueryResult& queryResult,
                      void* connection)
    {
        const clock_t startClock = clock();
        SCIDB_ASSERT(queryResult.queryID.isValid());

        // Executing query string
        std::shared_ptr<Query> query = Query::getQueryByID(queryResult.queryID);
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

        assert(query->getQueryID() == queryResult.queryID);

        if (!query->logicalPlan->getRoot()) {
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_WAS_EXECUTED);
        }
        std::ostringstream planString;
        query->logicalPlan->toString(planString);
        queryResult.explainLogical = planString.str();
        // Note: Optimization will be performed while execution
        std::shared_ptr<Optimizer> optimizer =  Optimizer::create();
        try {
            query->start();

            while (queryProcessor->optimize(optimizer, query))
            {
                LOG4CXX_DEBUG(logger, "Query is optimized");

                const bool isDdl = query->getCurrentPhysicalPlan()->isDdl();
                LOG4CXX_DEBUG(logger, "The physical plan is detected as "
                              << (isDdl ? "DDL" : "DML") );
                if (logger->isDebugEnabled())
                {
                    std::ostringstream planString;
                    query->getCurrentPhysicalPlan()->toString(planString);
                    LOG4CXX_DEBUG(logger, "\n" + planString.str());
                }

                // Execution of single part of physical plan
                queryProcessor->preSingleExecute(query);
                NetworkManager* networkManager = NetworkManager::getInstance();
                const size_t instancesCount = query->getInstancesCount();

                {
                    std::ostringstream planString;
                    query->getCurrentPhysicalPlan()->toString(planString);
                    query->explainPhysical += planString.str() + ";";

                    // Serialize physical plan and sending it out
                    const string physicalPlan = serializePhysicalPlan(query->getCurrentPhysicalPlan());
                    LOG4CXX_DEBUG(logger, "The query plan is: " << planString.str());
                    LOG4CXX_DEBUG(logger, "The serialized form of the physical plan: queryID="
                                  << queryResult.queryID << ", physicalPlan='" << physicalPlan << "'");
                    std::shared_ptr<MessageDesc> preparePhysicalPlanMsg =
                        std::make_shared<MessageDesc>(mtPreparePhysicalPlan);
                    std::shared_ptr<scidb_msg::PhysicalPlan> preparePhysicalPlanRecord =
                        preparePhysicalPlanMsg->getRecord<scidb_msg::PhysicalPlan>();
                    preparePhysicalPlanMsg->setQueryID(query->getQueryID());
                    preparePhysicalPlanRecord->set_physical_plan(physicalPlan);

                    int priority = static_cast<MessageHandleJob*>(
                        Job::getCurrentJobPerThread().get())->getPriority();
                    preparePhysicalPlanRecord->mutable_session_info()->set_job_priority(priority);
                    preparePhysicalPlanRecord->mutable_session_info()->set_session_json(
                        query->getSession()->toJson());

                    Cluster* cluster = Cluster::getInstance();
                    assert(cluster);
                    preparePhysicalPlanRecord->set_cluster_uuid(cluster->getUuid());
                    networkManager->broadcastLogical(preparePhysicalPlanMsg);
                    LOG4CXX_DEBUG(logger, "Prepare physical plan was sent out");
                    LOG4CXX_DEBUG(logger, "Waiting confirmation about preparing physical plan in queryID from "
                       << instancesCount - 1 << " instances")
                }

                // Make sure ALL instances are ready to run. If the coordinator does not hear
                // from the workers within a timeout, the query is aborted. This is done to prevent a deadlock
                // caused by thread starvation.
                int deadlockTimeoutSec = Config::getInstance()->getOption<int>(CONFIG_DEADLOCK_TIMEOUT);
                if (deadlockTimeoutSec <= 0) {
                    deadlockTimeoutSec = 10;
                }
                static const uint64_t NANOSEC_PER_SEC = 1000 * 1000 * 1000;
                syncUtils::wait(query, static_cast<uint64_t>(deadlockTimeoutSec)*NANOSEC_PER_SEC);

                try {
                    // Execution of local part of physical plan
                    queryProcessor->execute(query);
                }
                catch (const std::bad_alloc& e) {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
                }
                LOG4CXX_DEBUG(logger, "Query is executed locally");

                // Wait for results from every instance except itself
                Semaphore::ErrorChecker ec = boost::bind(&Query::validate, query);
                query->semResults.enter(instancesCount-1, ec, PTW_SEM_RESULTS_EX);
                LOG4CXX_DEBUG(logger, "The responses are received");
                /**
                 * Check error state
                 */
                query->validate();

                queryProcessor->postSingleExecute(query);
            }
            query->done();
        } catch (const Exception& e) {
            query->done(e.copy());
            e.raise();
        }

        const clock_t stopClock = clock();

        queryResult.executionTime = (stopClock - startClock) * 1000 / CLOCKS_PER_SEC;;
        queryResult.queryID = query->getQueryID();
        queryResult.explainPhysical = query->explainPhysical;
        queryResult.selective = query->getCurrentResultArray().get()!=nullptr;
        queryResult.autoCommit = query->isAutoCommit();
        if (queryResult.selective) {
            SCIDB_ASSERT(!queryResult.autoCommit);
            queryResult.array = query->getCurrentResultArray();
        }

        LOG4CXX_DEBUG(logger, "The result of query (autoCommit="
                      << queryResult.autoCommit
                      <<", selective="<<queryResult.selective
                      <<") is returned")
    }

    /// Create and optimize the physical query plan,
    /// broadcast the plan maintaining a global order
    // to avoid deadlocks caused by thread starvation.
    void startExecuteQuery(const std::string& queryString,
                           bool afl,
                           QueryResult& queryResult)
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());

        // Executing query string
        std::shared_ptr<Query> query = Query::getQueryByID(queryResult.queryID);
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

        SCIDB_ASSERT(query->getQueryID() == queryResult.queryID);
        SCIDB_ASSERT(query->getSession());

        if (!query->logicalPlan->getRoot()) {
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_WAS_EXECUTED);
        }
        std::ostringstream planString;
        query->logicalPlan->toString(planString);
        queryResult.explainLogical = planString.str();

        LOG4CXX_DEBUG(logger, "Begin optimizing queryID="<<queryResult.queryID);

        // Note: Optimization will be performed while execution
        std::shared_ptr<Optimizer> optimizer =  Optimizer::create();
        try
        {
            query->start();

            queryProcessor->optimize(optimizer, query);

            LOG4CXX_DEBUG(logger, "Query is optimized");

            const bool isDdl = query->getCurrentPhysicalPlan()->isDdl();
            LOG4CXX_DEBUG(logger, "The physical plan is detected as "
                          << (isDdl ? "DDL" : "DML") );
            if (logger->isDebugEnabled())
            {
                std::ostringstream planString;
                query->getCurrentPhysicalPlan()->toString(planString);
                LOG4CXX_DEBUG(logger, "\n" + planString.str());
            }

            // Execution of single part of physical plan
            queryProcessor->preSingleExecute(query);
            {
                std::ostringstream planString;
                query->getCurrentPhysicalPlan()->toString(planString);
                query->explainPhysical += planString.str() + ";";

                // Serialize physical plan and sending it out
                const string physicalPlan = serializePhysicalPlan(query->getCurrentPhysicalPlan());
                LOG4CXX_DEBUG(logger, "The query plan is: " << planString.str());
                LOG4CXX_DEBUG(logger, "The serialized form of the physical plan: queryID="
                              << queryResult.queryID << ", physicalPlan='" << physicalPlan << "'");
                std::shared_ptr<MessageDesc> preparePhysicalPlanMsg = std::make_shared<MessageDesc>(mtPreparePhysicalPlan);
                std::shared_ptr<scidb_msg::PhysicalPlan> preparePhysicalPlanRecord =
                        preparePhysicalPlanMsg->getRecord<scidb_msg::PhysicalPlan>();
                preparePhysicalPlanMsg->setQueryID(query->getQueryID());
                preparePhysicalPlanRecord->set_physical_plan(physicalPlan);

                int priority = static_cast<MessageHandleJob*>(
                    Job::getCurrentJobPerThread().get())->getPriority();
                preparePhysicalPlanRecord->mutable_session_info()->set_job_priority(priority);
                preparePhysicalPlanRecord->mutable_session_info()->set_session_json(query->getSession()->toJson());

                Cluster* cluster = Cluster::getInstance();
                assert(cluster);
                preparePhysicalPlanRecord->set_cluster_uuid(cluster->getUuid());

                const bool isDeadlockPossible = false; //XXX TODO: enable this option in config.ini
                if (isDeadlockPossible) {
                    NetworkManager::getInstance()->broadcastLogical(preparePhysicalPlanMsg);
                    LOG4CXX_DEBUG(logger, "Prepare physical plan was sent out");
                    LOG4CXX_DEBUG(logger, "Waiting confirmation about preparing physical plan in queryID from "
                                  << query->getInstancesCount() - 1 << " instances")
                } else {
                    OrderedBcastManager::getInstance()->broadcast(preparePhysicalPlanMsg);
                    LOG4CXX_DEBUG(logger, "Prepare physical plan was sent out");
                }
            }
            query->stop();
        }
        catch (const std::bad_alloc& e)
        {
            std::shared_ptr<SystemException> se =
                    SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR);
            se << e.what();
            query->done(se);
            se->raise();
        }
        catch (const Exception& e)
        {
            query->done(e.copy());
            e.raise();
        }
    }

    /// Execute the pre-created physical plan
    void completeExecuteQuery(QueryResult& queryResult,
                              const std::shared_ptr<Query>& query)
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());
        SCIDB_ASSERT(query);

        arena::ScopedArenaTLS arenaTLS(query->getArena());

        // Executing query string
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

        SCIDB_ASSERT(query->getQueryID() == queryResult.queryID);
        SCIDB_ASSERT(!queryResult.explainLogical.empty());

        ASSERT_EXCEPTION(!query->logicalPlan->getRoot(), "When executed, query must have logicalPlan");

        const bool isDeadlockPossible = false; //XXX TODO: enable this option in config.ini
        if (isDeadlockPossible) {
            LOG4CXX_DEBUG(logger, "Waiting confirmation about preparing physical plan in queryID from "
                          << query->getInstancesCount() - 1 << " instances");
            // Make sure ALL instances are ready to run. If the coordinator does not hear
            // from the workers within a timeout, the query is aborted. This is done to prevent a deadlock
            // caused by thread starvation.
            // The deadlock is only possible if OrderedBcastManager is not used.
            // XXX TODO: In a long term we should probably solve the problem of thread starvation
            // XXX TODO: using for asynchronous execution techniques rather than global ordering.
            int deadlockTimeoutSec = Config::getInstance()->getOption<int>(CONFIG_DEADLOCK_TIMEOUT);
            if (deadlockTimeoutSec <= 0) {
                deadlockTimeoutSec = 10;
            }
            static const uint64_t NANOSEC_PER_SEC = 1000 * 1000 * 1000;
            syncUtils::wait(query, static_cast<uint64_t>(deadlockTimeoutSec)*NANOSEC_PER_SEC);
        }

        try {
            query->start();

            const size_t instanceCount = query->getInstancesCount();

            // Execution of local part of physical plan
            queryProcessor->execute(query);

            LOG4CXX_DEBUG(logger, "Query " << query->getQueryID()
                          << " executed locally, awaiting remote responses");

            // Wait for results from every instance except itself
            Semaphore::ErrorChecker ec = boost::bind(&Query::validate, query);
            query->semResults.enter(instanceCount-1, ec, PTW_SEM_RESULTS_EX);

            LOG4CXX_DEBUG(logger, "Query " << query->getQueryID()
                          << " remote responses received");

            // Check error state
            query->validate();

            queryProcessor->postSingleExecute(query);

            query->done();
        }
        catch (const std::bad_alloc& e)
        {
            std::shared_ptr<SystemException> se =
                    SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR);
            se << e.what();
            query->done(se);
            se->raise();
        }
        catch (const Exception& e)
        {
            query->done(e.copy());
            e.raise();
        }
        queryResult.queryID = query->getQueryID();
        queryResult.explainPhysical = query->explainPhysical;
        queryResult.selective = query->getCurrentResultArray().get()!=nullptr;
        queryResult.autoCommit = query->isAutoCommit();
        if (queryResult.selective) {
            SCIDB_ASSERT(!queryResult.autoCommit);
            queryResult.array = query->getCurrentResultArray();
        }

        LOG4CXX_DEBUG(logger, "The result of query (autoCommit="
                      << queryResult.autoCommit
                      <<", selective="<<queryResult.selective
                      <<") is returned")
    }

    void cancelQuery(QueryID queryID, void* connection)
    {
        LOG4CXX_TRACE(logger, "Cancelling query " << queryID)
        std::shared_ptr<Query> query = Query::getQueryByID(queryID);
        arena::ScopedArenaTLS arenaTLS(query->getArena());

        query->handleCancel();
    }

    void completeQuery(QueryID queryID, void* connection)
    {
        LOG4CXX_TRACE(logger, "Completing query " << queryID)
        std::shared_ptr<Query> query = Query::getQueryByID(queryID);
        arena::ScopedArenaTLS arenaTLS(query->getArena());

        query->handleComplete();
    }

    // deprecated in 16.6
    virtual void newClientStart(void* connection,
                                const SessionProperties& sessionProperties,
                                const std::string& userInfoFileName)
    {
        ASSERT_EXCEPTION_FALSE(
            "newClientStart - not needed, to implement in engine");
    }

    // deprecated in 16.6
    virtual void newClientStart(void* connection,
                                const std::string& userInfoFileName)
    {
        ASSERT_EXCEPTION_FALSE(
            "newClientStart - not needed, to implement in engine");
    }
} _sciDBExecutor;


SciDB& getSciDBExecutor()
{
   _sciDBExecutor.InjectedErrorListener::start();
    return _sciDBExecutor;
}

} // namespace scidb
