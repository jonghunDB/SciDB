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
 * @file Query.h
 * @brief Query context
 */

#ifndef QUERY_H_
#define QUERY_H_

#include <atomic>
#include <memory>
#include <string>
#include <deque>
#include <list>

#include <boost/scoped_array.hpp>
#include <boost/random/mersenne_twister.hpp>

#include <log4cxx/logger.h>

#include <monitor/QueryStats.h>
#include <query/QueryID.h>
#include <query/OperatorContext.h>
#include <system/LockDesc.h>
#include <util/Arena.h>
#include <util/Job.h>
#include <util/Notification.h>
#include <util/Semaphore.h>
#include <util/Thread.h>
#include <util/WorkQueue.h>

namespace scidb
{
class Array;
class LogicalPlan;
class MessageDesc;
class OperatorID;
class PhysicalOperator;
class PhysicalPlan;
class ProcGrid;
class RemoteArray;
class RemoteMergedArray;
class ReplicationContext;
class NewReplicationManager;
class Session;
class Warning;
namespace rbac { class RightsMap; }

const size_t MAX_BARRIERS = 2;


/**
 * The query structure keeps track of query execution and manages the resources used by SciDB
 * in order to execute the query. The Query is a state of query processor to make
 * query processor stateless. The object lives while query is used including receiving results.
 */
class Query : public std::enable_shared_from_this<Query>
{
public:  // types

    class ErrorHandler
    {
      public:
        virtual void handleError(const std::shared_ptr<Query>& query) = 0;
        virtual ~ErrorHandler() {}
    };

    /**
     * This struct is used for proper accounting of outstanding requests/jobs.
     * The number of outstanding requests can be incremented when they arrive
     * and decremented when they have been processed.
     * test() indicates the arrival of the last request.
     * From that point on, when the count of outstanding requests drops to zero,
     * it is an indication that all of the requests have been processed.
     * This mechanism is used for the sync() method used in SG and (for debugging) in replication.
     */
    class PendingRequests
    {
    private:
        Mutex  _mutex;
        size_t _nReqs;
        bool   _sync;
    public:
        size_t increment();
        bool decrement();
        bool test();

        PendingRequests() : _nReqs(0), _sync(false) {}
    };

    /** execution of query completion status */
    enum CompletionStatus
    {
        INIT  = 0,  // query execute() has not started
        START = 1,  // query execute() has not completed
        OK    = 2,  // query execute() completed with no errors
        ERROR = 3   // query execute() completed with errors
    };

    /** Query commit state */
    enum CommitState
    {
        UNKNOWN,
        COMMITTED, // _completionStatus!=ERROR
        ABORTED
    };

    using Finalizer = boost::function<void(const std::shared_ptr<Query>&)>;
    using InstanceVisitor = boost::function<void(const std::shared_ptr<Query>&, InstanceID)>;
    /// Functor type to represent a continuation of query processing which has been previously stopped
    using Continuation = boost::function<void(const std::shared_ptr<Query>&)>;
    using Queries = std::map<QueryID, std::shared_ptr<Query>>;
    using Visitor = boost::function<void(const std::shared_ptr<Query>&)>;

    friend class ServerMessageHandleJob;
    friend class UpdateErrorHandler;

public:  // methods
    explicit Query(const QueryID& querID);
    ~Query();

    //disable copying
    Query(const Query&) = delete;
    Query& operator=(const Query&) = delete;

    bool isFake() const
    {
        return getQueryID().isFake();
    }

    /**
     * @return amount of time to wait before trying to acquire an array lock
     */
    static uint64_t getLockTimeoutNanoSec();

    /**
     * Put this thread to sleep for some time
     * (before trying to acquire a SystemCatalog lock again)
     */
    static void waitForSystemCatalogLock();

    /**
     * Generate unique(?) query ID
     */
    static QueryID generateID();

    /// @return a random value
    static uint32_t getRandom() { return _rng(); }

    /**
     * @return the number of queries currently in the system
     * @param class Query::Visitor, a function of a query pointer
     * It is not allowed to take any locks.
     */
    static size_t visitQueries(const Visitor&);

    /**
     * Retrieve class responsible for holding the statistics
     */
    class QueryStats & getStats(bool updateArena = true);

    /**
     * Retrieve the completion status of the query.
     * @returns one of { "0 - pending", "1 - active", "2 - done", "3 - errors", "4 - unknown=%d" }
     */
    std::string getCompletionStatusStr() const;

    /**
     * Get the time stamp for the start of the query.
     */
    double getStartTime() const;

    /**
     * Saves a pointer to the session object.  This method is normally
     * called by the connection object when the query is being initialized.
     *
     * @param session  pointer to the session information
     */
    void attachSession(const std::shared_ptr<Session> &session);

    /**
     * Retrieves the session pointer set by the connection object.
     *
     * Fake queries do not have attached sessions.  Normal queries
     * *eventually* do, but there are windows during Query object
     * initialization where the Query lives in the _queries map but
     * the session is not yet attached.  Be careful when walking the
     * map.  @see ListQueriesArrayBuilder
     *
     * @return pointer to the session information
     */
    std::shared_ptr<Session> getSession()
    {
         return _session;
    }

    /**
     * Retrieves the const session pointer set by the connection object.
     * See non-const method above.
     */
    std::shared_ptr<const Session> getSession() const
    {
         return _session;
    }

    /**
     * Retrieve map of needed access rights accumulated so far.
     */
    rbac::RightsMap* getRights()
    {
        SCIDB_ASSERT(_rights.get() != nullptr);
        return _rights.get();
    }

    /**
     * Retrieve the current namespace name.
     * @returns non-empty namespace name
     */
    std::string getNamespaceName() const;

   /**
     * Retrieve the namespaceName and arrayName from a query and a potentially qualified array name
     * @param[in] qualifiedName A potentially qualified array name
     * @param[out] namespaceName The resulting namespaceName
     * @param[out] arrayName The resulting arrayName
     **/
     void getNamespaceArrayNames(
        const std::string &         qualifiedName,
        std::string &               namespaceName,
        std::string &               arrayName) const;

    /// @brief Set the autoCommit property.
    /// @note This function may only be called by the coordinator.
    void setAutoCommit();

    /// @return true if the query is in autoCommit mode.
    /// @note This function may only be called by the coordinator.
    bool isAutoCommit() const
    {
        SCIDB_ASSERT(isCoordinator());
        return _isAutoCommit;
    }

    /**
     * Add an error handler to run after a query's "main" routine has completed
     * and the query needs to be aborted/rolled back
     * @param eh - the error handler
     */
    void pushErrorHandler(const std::shared_ptr<ErrorHandler>& eh);

    /**
     * Add a finalizer to run after a query's "main" routine has completed (with any status)
     * and the query is about to be removed from the system
     * @param f - the finalizer
     */
    void pushFinalizer(const Finalizer& f);

    /**
     * Handle a change in the local instance liveness. If the new livenes is different
     * from this query's coordinator liveness, the query is marked to be aborted.
     */
    void handleLivenessNotification(std::shared_ptr<const InstanceLiveness>& newLiveness);

    /**
     * Map a "logical" instance ID to a "physical" one using the coordinator liveness.
     * @param logicalInstanceID  a logical instanceID.
     * @return the matching physical instanceID (if logicalInstanceID is valid), or INVALID_INSTANCE (if logicalInstanceID is invalid).
     */
    InstanceID mapLogicalToPhysical(InstanceID logicalInstanceID);

    /**
     * Map a "physical" instance ID to a "logical" one using the coordinator liveness.
     * @param physicalInstanceID  a physicalInstanceID.
     * @return the matching logical instanceID (if physicalInstanceID is valid), or INVALID_INSTANCE (if physicalInstanceID is invalid).
     */
    InstanceID mapPhysicalToLogical(InstanceID physicalInstanceID);

    /**
     * @return true if a given instance is considered dead
     * @param instance physical ID of a instance
     * @throw scidb::SystemException if this.errorCode is not 0
     */
    bool isPhysicalInstanceDead(InstanceID instance);

    /**
     * Get the "physical" instance ID of the coordinator
     * @param resolveLocalInstanceId if the result must always be a valid instance ID
     * @return COORDINATOR_INSTANCE if this instance is the coordinator and !resolveLocal,
     * else the coordinator instance's physical ID
     */
    InstanceID getPhysicalCoordinatorID(bool resolveLocalInstanceId=false);

    /**
     * Get logical instance count
     */
    size_t getInstancesCount() const
    {
        return _liveInstances.size();
    }

    /**
     * Return the arena that is owned by this query and from which the various
     * resources it needs in order to execute should be allocated.
     */
    arena::ArenaPtr getArena() const
    {
        return _arena;
    }

    /**
     *  Return true if the query completed successfully and was committed.
     */
    bool wasCommitted() const
    {
        return _commitState == COMMITTED;
    }

    /**
     * Execute a given routine for every live instance
     * @param func routine to execute
     */
    void listLiveInstances(InstanceVisitor& func);

    /**
     * Info needed for ScaLAPACK-compatible chunk distributions
     * Redistribution code and ScaLAPACK-based plugins need this,
     * most operators do not.
     */
    const ProcGrid* getProcGrid() const;

    /**
     * Get logical instance ID
     */
    InstanceID getInstanceID() const
    {
        return _instanceID;
    }

    /**
     * Get physical instance ID
     */
    InstanceID getPhysicalInstanceID() const
    {
        return _physInstanceID;
    }

    /**
     * @return coordinator's logical instance ID (when called on a worker instance),
     *         or INVALID_INSTANCE (when called on the coordinator).
     */
    InstanceID getCoordinatorID() const
    {
        return _coordinatorID;
    }

    bool isCoordinator() const
    {
        return (_coordinatorID == INVALID_INSTANCE);
    }

    std::shared_ptr<const InstanceLiveness> getCoordinatorLiveness()
    {
       return _coordinatorLiveness;
    }

    /**
     * @return false if all the instances in the array residency participate in the query;
     *         true if some number of instances in the array residency do not participate,
     *         but the number is less than the redundancy
     * @throw scidb::SystemException if the number of missing instances is > redundancy
     */
    bool isDistributionDegradedForRead(const ArrayDesc& desc);

    /**
     * @return false if all the instances in the array residency participate in the query;
     *
     * @throw scidb::SystemException if the number of missing instances is > 0
     */
    bool isDistributionDegradedForWrite(const ArrayDesc& desc);

    /// Verify that we have enough live instances to perform an array removal
    /// @throws scidb::SystemException on failure
    void checkDistributionForRemove(const ArrayDesc& desc);

    /// @return a residency consisting of the instances participating in the query
    /// that set of instances is the same as the query liveness
    ArrayResPtr getDefaultArrayResidency();

    /**
     * @return a residency consisting of the instances used to store arrays by default
     * that set of instances is the same as the query membership (for now)
     * @throws scidb::SystemException if the instance membership
     *         no longer matches the one of this query
     */
    ArrayResPtr getDefaultArrayResidencyForWrite();

    /**
     * The string with query that user want to execute.
     */
    const std::string & getQueryString() const
    {
        return queryString;
    }

    std::shared_ptr<Array> getCurrentResultArray();

    void setCurrentResultArray(const std::shared_ptr<Array>& array);

    /// swaps the current query continuation with a given one
    /// @param cont [in/out] continuation to insert on input, the old continuation on output
    void swapContinuation(Continuation& cont)
    {
        ScopedMutexLock cs(errorMutex);
        _continuation.swap(cont);
    }

    std::shared_ptr<RemoteMergedArray> getMergedArray()
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        validate();
        return _mergedArray;
    }

    void setMergedArray(const std::shared_ptr<RemoteMergedArray>& array)
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        validate();
        _mergedArray = array;
    }

    /**
     * Request that a given array lock be acquired before the query execution starts
     * @param lock - the lock description
     * @return either the requested lock or the lock that has already been requested for the same array
     *         with a more exclusive mode (RD < WR,CRT,RM,RNF,RNT)
     * @see scidb::LockDesc
     */
    std::shared_ptr<LockDesc> requestLock(std::shared_ptr<LockDesc>& lock);

    void addPhysicalPlan(std::shared_ptr<PhysicalPlan> physicalPlan)
    {
        _physicalPlans.push_back(physicalPlan);
    }

    std::shared_ptr<PhysicalPlan> getCurrentPhysicalPlan()
    {
        return _physicalPlans.empty()
            ? std::shared_ptr<PhysicalPlan>()
            : _physicalPlans.back();
    }

    /**
     * Get the queue for delivering buffer-send (mtMPISend) messages
     * @return empty pointer if the query is no longer active
     */
    std::shared_ptr<scidb::WorkQueue> getBufferReceiveQueue()
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        validate();
        assert(_bufferReceiveQueue);
        return _bufferReceiveQueue;
    }

    /// @return the query error queue or NULL if the queue is already deallocated
    std::shared_ptr<scidb::WorkQueue> getErrorQueue()
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        // not validating because the query can be in error state
        return _errorQueue;
    }

    std::shared_ptr<scidb::WorkQueue> getSGQueue()
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        validate();
        assert(_sgQueue);
        return _sgQueue;
    }

    std::shared_ptr<scidb::ReplicationContext> getReplicationContext()
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
        validate();
        assert(_replicationCtx);
        return _replicationCtx;
    }

    /// Create a fake query that does not correspond to a user-generated request
    /// for internal purposes only
    static std::shared_ptr<Query> createFakeQuery(InstanceID coordID,
                                                  InstanceID localInstanceID,
                                                  const InstLivenessPtr& liveness,
                                                  int32_t *longErrorCode=NULL);

    /// Destroy a query generated by createFakeQuery()
    static void destroyFakeQuery(Query* q);

    /**
     * Creates new query object and generate new queryID
     */
    static std::shared_ptr<Query> create(QueryID queryId, InstanceID instanceId=INVALID_INSTANCE);

    /**
     * Find query with given queryID in the global query map
     * @throws scidb::SystemException if the query id is not found and 'raise' is true
     * @returns query object pointer (null if not found and 'raise' is false)
     */
    static std::shared_ptr<Query> getQueryByID(QueryID queryID, bool raise = true);

    /**
     * Validates the pointer and the query it points for errors
     * @throws scidb::SystemException if the pointer is empty or if the query is in error state
     * @return true if no exception is thrown
     */
    static bool validateQueryPtr(const std::shared_ptr<Query>& query)
    {
#ifndef SCIDB_CLIENT
        if (!query) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND2);
        }
        return query->validate();
#else
        return true;
#endif
    }

    /**
     * Creates and validates the pointer and the query it points to for errors
     * @throws scidb::SystemException if the pointer is dead or if the query is in error state
     * @return a live query pointer if no exception is thrown
     */
    static std::shared_ptr<Query> getValidQueryPtr(const std::weak_ptr<Query>& query)
    {
        std::shared_ptr<Query> q(query.lock());
        validateQueryPtr(q);
        return q;
    }

    ///A wrapper over getValidQueryPtr(), to return a boolean.
    static bool isValidQueryPtr(const std::weak_ptr<Query>& query)
    {
        return getValidQueryPtr(query).get() != nullptr;
    }

    /**
     * Destroys query contexts for every still existing query
     */
    static void freeQueries(const Finalizer& onFreeQuery =
                                [] (const std::shared_ptr<Query>&) {});

    /**
     * Behaves the same as freeQueries, and posts the given warning
     * to each one.
     */
    static void freeQueriesWithWarning(const Warning& warn);

    /**
     * Release all the locks previously acquired by acquireLocks()
     * @param query whose locks to release
     * @throws exceptions while releasing the lock
     */
    static void releaseLocks(const std::shared_ptr<Query>& query);

    /**
     * Register PhysicalOperator in query's operator vector so it can be found by index
     * @returns operator id
     * @param phyOp the PhysicalOperator for the SG
     */
    void registerPhysicalOperator(const std::weak_ptr<PhysicalOperator>& phyOp);

    /**
     * Find operator with given operatorID in the query's operator vector
     * @throws scidb::SystemException if the query id is not found and 'raise' is true
     * @returns query object pointer (null if not found and 'raise' is false)
     */
    std::shared_ptr<PhysicalOperator> getPhysicalOperatorByID(const OperatorID& operatorID) const ;

    /**
     * Associate temporay array with this query
     * @param tmpArray temporary array
     */
    void setTemporaryArray(std::shared_ptr<Array> const& tmpArray);

    /**
     * Repeatedly execute given work until it either succeeds
     * or throws an unrecoverable exception.
     * @param work to execute
     * @param tries count of tries, -1 to infinite
     * @return result of running work
     */
    template<typename T, typename E>
    static T runRestartableWork(boost::function<T()>& work, int tries = -1);

    /**
     * Acquire all locks requested via requestLock(),
     * @throw scidb::LockBusyException if any of the locks are already taken (by other queries).
     *        Any locks that have been successfully acquired remain in that state.
     *        Any subsequent attempts to acquire the remaining locks should be done using retryAcquireLocks()
     * @throws exceptions while acquiring the locks and the same exceptions as validate()
     */
    void acquireLocks();

    /**
     * Acquire all locks requested via requestLock(). This method should be invoked only if
     * a previous call to acquireLocks() has failed with scidb::LockBusyException
     * @throw scidb::LockBusyException if any of the locks are already taken (by other queries).
     *        Any locks that have been successfully acquired remain in that state.
     *        Any subsequent attempts to acquire the remaining locks should be done using retryAcquireLocks()
     * @throws exceptions while acquiring the locks and the same exceptions as validate()
     */
    void retryAcquireLocks();

    /**
     * @return  true if the query acquires exclusive locks
     */
    bool doesExclusiveArrayAccess();

    /**
     * Handle a query error.
     * May attempt to invoke error handlers
     */
    void handleError(const std::shared_ptr<Exception>& unwindException);

    /**
     * Handle a client complete request
     */
    void handleComplete();

    /**
     * Handle a client cancellation request
     */
    void handleCancel();

    /**
     * Handle a coordinator commit request
     */
    void handleCommit();

    /**
     * Handle a coordinator abort request
     */
    void handleAbort();

    /**
     * Returns query ID
     */
    QueryID getQueryID() const
    {
        return _queryID;
    }

    static uint64_t numActiveQueries()
    {
        ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY);
        return _queries.size();
    }

    /**
     * Get the maximum ArrayID, versioned or unversioned, currently recorded in the catalog for a given array name.
     * All the versioned array names result in the same value as the corresponding unversioned array name,
     * i.e. getCatalogVersion("X") == getCatalogVersion("X@Y").
     * @note THREAD-SAFETY: This method can be called either before all the query locks are acquired or strictly after.
     * Otherwise, the effect is undefined. Currently the code respects that protocol but
     * this method does not use any synchronization to enforce it.
     * @param [in] namespaceName The namespace the array belongs to
     * @param [in] arrayName (possibly an array version name like 'X@Y')
     * @param [in] allowMissing If it is true and arrayName is not known, SystemCatalog::ANY_VERSION is returned.
     *             The default value is false.
     * @return the catalog array version ID
     */
    ArrayID getCatalogVersion(  const std::string& namespaceName,
                                const std::string& arrayName,
                                bool allowMissing=false) const ;
    ArrayID getCatalogVersion(  const std::string& arrayName,
                                bool allowMissing=false) const ;

    /**
     * Set result SG context and start the queue. Thread safe.
     *
     * @note needed for "implicit" SG that bring data back to coordinator
     *    when query includes results returned to client
     */
    void startSGQueue(std::shared_ptr<OperatorContext> const& opContext,
                      std::shared_ptr<JobQueue> const& jobQueue = std::shared_ptr<JobQueue>());

    /**
     * Stop theq queue and clear the SG context.
     */
    void stopSGQueue();

    /**
     * Mark query as started
     */
    void start();

    /**
     * Suspend query processing: state will be INIT
     */
    void stop();

    /**
     * Mark query as completed
     */
    void done();

    /**
     * Mark query as completed with an error
     */
    void done(const std::shared_ptr<Exception>& unwindException);

    /**
     * Validates the query for errors
     * @throws scidb::SystemException if the query is in error state
     * @return true if no exception is thrown
     */
    bool validate();

    void postWarning(const class Warning& warn);

    std::vector<Warning> getWarnings();

    void clearWarnings();

    time_t getCreationTime() const
    {
        return _creationTime;
    }

    std::shared_ptr<Exception> getError()
    {
        ScopedMutexLock lock(errorMutex, PTW_SML_QUERY);
        return _error;
    }

    /**
     * @return true if the query has been executed,
     * but no processing is currently happening (i.e. the client is not fetching)
     */
    bool idle() const
    {
        ScopedMutexLock lock(errorMutex, PTW_SML_QUERY);
        return ((_completionStatus == OK ||
                 _completionStatus == ERROR) &&
                // one ref is in Query::_queries another is shared_from_this()
                // more refs indicate that some jobs/iterators are using the query
                shared_from_this().use_count() < 3);
    }

    /**
     * store a shared pointer to (this) query in thread-local storage
     * after using this, be careful to use resetQueryPerThread()
     * in catch blocks. (should make a StackAssociate RAII thing)
     * this is implemented as a member function only to add
     * the sanity check of the assert()
     * note that the shared_pointer must be passed in as a reference
     * as that must be external
     */
    static void setQueryPerThread(const std::shared_ptr<Query>& query);

    /**
     * read the per-thread query pointer
     */
    static std::shared_ptr<Query> getQueryPerThread();

    /**
     * reset the per-thread query pointer
     */
    static void resetQueryPerThread();

    /**
     * how to accumulate (or make adjustments to) time category aggregates
     */
    void perfTimeAdd(const perfTimeWait_t tw, const double sec);

    /**
     * Retrieve the time the query has been active
     */
    uint64_t getActiveTimeMicroseconds() const;

private:  // methods
    /**
     * Get/insert a query object from/to the global list of queries
     * @param query the query object to insert
     * @return the old query object if it is already on the list;
     *         otherwise, the newly inserted object specified by the argument
     */
    static std::shared_ptr<Query> insert(const std::shared_ptr<Query>& query);

    /**
     * Initialize a query
     * @param coordID the "physical" coordinator ID (or INVALID_INSTANCE if on coordinator)
     * @param localInstanceID  "physical" local instance ID
     * @param coordinatorLiveness coordinator liveness at the time of query creation
     */
    void init(InstanceID coordID,
              InstanceID localInstanceID,
              const InstLivenessPtr& coordinatorLiveness);

    void setCoordinatorLiveness(std::shared_ptr<const InstanceLiveness>& liveness)
    {
       ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
       _coordinatorLiveness = liveness;
    }

    /**
     * @return true if the set of instances participating in the query is NOT the same as
     *         or a superset of the set of instances to which a given array is distributed;
     *         false otherwise
     * @param desc array descriptor (which specifies the array distribution)
     * @param redundancy array redundancy
     * @throws scidb::SystemException if the instance membership
     *         no longer matches the one of this query OR
     *         if the number of live instances is not sufficient to run the query
     *         taking into account the redundancy
     */
    bool isDistributionDegraded(const ArrayDesc& desc, size_t redundancy);

    /**
     * This function should only be called if the query is (being) aborted.
     * @return true if the local query execution finished successfully AND the coordinator was notified;
     * false otherwise i.e. the local execution failed before notifying the coordinator,
     * which couldn't possibly commit this query.
     */
    bool isForceCancelled()
    {
       ScopedMutexLock cs(errorMutex, PTW_SML_QUERY);
       assert (_commitState==ABORTED);
       bool result = (_completionStatus == OK);
       return result;
    }
    bool checkFinalState();

    /**
     *  Helper to invoke the finalizers with exception handling
     */
    void invokeFinalizers(std::deque<Finalizer>& finalizers);

    /**
     *  Helper to invoke the finalizers with exception handling
     */
    void invokeErrorHandlers(std::deque< std::shared_ptr<ErrorHandler> >& errorHandlers);

    void destroy();
    static void destroyFinalizer(const std::shared_ptr<Query>& q)
    {
        assert(q);
        q->destroy();
    }

    static void broadcastCommitFinalizer(const std::shared_ptr<Query>& q);

    /**
     * Destroy specified query context
     */
    static void freeQuery(const QueryID& queryID);

    /**
     * Acquire a set of SystemCatalog locks
     */
    void acquireLocksInternal(QueryLocks& locks);

    void checkNoError() const ;

private:  // data

    /**
     * Hold next value for generation query ID
     */
    static uint32_t nextID;

   /**
    * A dedicated arena from which this query can allocate the various resources that
    * it needs to execute.
    */
    arena::ArenaPtr _arena;

    /**
     * Query identifier to find the query during asynchronous message exchanging.
     */
    QueryID _queryID;

    /**
     * The global list of queries present in the system
     */
    static  Queries _queries;

    /**
     * currently THE operator context
     * in the future: the one "implicit" operator context for SGs that return data to client
     * even later: eliminated
     */
    std::shared_ptr<OperatorContext> _operatorContext;

    /**
     * vector of operators so that they can be looked up from remote
     * chunk-fetching messages.  (In practice, the index into this vector is the
     * OperatorID)
     */
    std::vector< std::weak_ptr<PhysicalOperator> > _physicalOperators;

    /**
     * The physical plan of query. Optimizer generates it for current step of incremental execution
     * from current logical plan. This plan is generated on coordinator and sent out to every instance for execution.
     */
    std::vector< std::shared_ptr<PhysicalPlan> > _physicalPlans;

    /**
     * Snapshot of the liveness information on the coordiantor
     * The worker instances must fail the query if their liveness membership
     * is/becomes different any time during the query execution.
     */
    std::shared_ptr<const InstanceLiveness> _coordinatorLiveness;

    /// Registration ID for liveness notifications
    Notification<InstanceLiveness>::SubscriberID _livenessSubscriberID;

    /**
     * The list of physical instances considered alive for the purposes
     * of this query. It is initialized to the liveness of
     * the coordinator when it starts the query. If any instance
     * detects a discrepancy in its current liveness and
     * this query liveness, it causes the query to abort.
     */
    std::vector<InstanceID> _liveInstances;

    /**
     * A "logical" instance ID of the local instance
     * for the purposes of this query.
     * It is obtained from the "physical" instance ID using
     * the coordinator liveness as the map.
     * Currently, it is the index of the local instance into
     * the sorted list of live instance IDs.
     */
    InstanceID _instanceID;

    /**
     * A "physical" instance ID of the local instance, which never changes
     */
    InstanceID _physInstanceID;

    /**
     * The "logical" instance ID of the instance responsible for coordination of query.
     * In case this instance is the coordinator instance, _coordinatorID is INVALID_INSTANCE.
     */
    InstanceID _coordinatorID;

    std::vector<Warning> _warnings;

    /**
     * Error state
     */
    mutable Mutex errorMutex;

    std::shared_ptr<Exception> _error;

    // RNG
    static boost::mt19937 _rng;

    Mutex _warningsMutex;

    /// Query array locks requested by the operators
    QueryLocks _requestedLocks;
    std::deque< std::shared_ptr<ErrorHandler> > _errorHandlers;
    std::deque<Finalizer> _finalizers; // last minute actions
    Continuation _continuation;

    /// @brief Access rights requested by operators.
    std::unique_ptr<rbac::RightsMap> _rights;

    CompletionStatus _completionStatus;
    CommitState _commitState;

    /**
     * Queue for MPI-style buffer messages
     */
    std::shared_ptr<scidb::WorkQueue> _bufferReceiveQueue;

    /**
     * FIFO queue for error messages
     */
    std::shared_ptr<scidb::WorkQueue> _errorQueue;

    /**
     * FIFO queue for SG messages
     */
    std::shared_ptr<scidb::WorkQueue> _sgQueue;

    /**
     * The state required to perform replication during execution
     */
    std::shared_ptr<ReplicationContext> _replicationCtx;

    /**
     * The result of query execution. It lives while the client connection is established.
     * In future we can develop more useful policy of result keeping with multiple
     * re-connections to query.
     */
    std::shared_ptr<Array> _currentResultArray;

    /**
     * TODO: XXX
     */
    std::shared_ptr<RemoteMergedArray> _mergedArray;

    /**
     * Time of query creation;
     */
    time_t _creationTime;

    /**
     * Used counter - increased for every handler which process Query and decreased after.
     * 0 means that client did not fetching data and query was executed.
     */
    int _useCounter;

    /**
     * true if the query acquires exclusive locks
     */
    bool _doesExclusiveArrayAccess;

    /**
    * cache for the ProGrid, which depends only on numInstances
    */
    mutable ProcGrid* _procGrid; // only access via getProcGrid()

    /**
     * cache for the array residency corresponding to the query live set
     */
    ArrayResPtr _defaultArrResidency; // only access via getDefaultArrayResidency()

    /**
     * The mutex to serialize access to _queries map.
     */
    static Mutex queriesMutex;

    /**
     *  A pointer to the session object used with this query
     */
    std::shared_ptr<Session> _session;

    /// If set, the query is committed immediately after a successful execution on the coordinator
    /// even before the client gets the QueryResult structure
    bool _isAutoCommit;

    /**
     * storage of perfTimeCategory_t statistics
     */
    static thread_local std::weak_ptr<Query> _queryPerThread;  // to find query from a client-handling thread
    std::atomic_int_fast64_t    _twUsecs[PTW_NUM];          // fast lock-free updates on x86_64
    int64_t                     _usecElapsedStart;           // starting timestamp, the only non-duration statistic
    static uint64_t             _perfTimeLogCount;           // just good enough for determining when to print

    QueryStats                  _queryStats;                 // Statistics for the query's usage

public:  // data
    /// @brief Logger for query processor.
    static log4cxx::LoggerPtr _logger;

    Mutex resultCS; /** @todo XXX this should not be necessary any more */

    /**
     * Program options which is used to run query
     */
    std::string programOptions;

    std::string queryString;

    std::string explainPhysical; /**< Every executed physical plan separated by ';' */

    /**
     * The logical plan of query. QueryProcessor generates it by parser only at coordinator instance.
     * Since we use incremental optimization this is the rest of logical plan to be executed.
     */
    std::shared_ptr<LogicalPlan> logicalPlan;

    /**
     *  Context variables to control thread
     */
    Semaphore semResults;

    /**
     * Semaphores for synchronization SG operations on remote instances
     */
    Semaphore semSG[MAX_BARRIERS];
    Semaphore syncSG;

    std::vector<PendingRequests> chunkReqs;

    /**
     * This section describe member fields needed for implementing send/receive functions.
     */
    Mutex _receiveMutex; //< Mutex for serialization access to _receiveXXX fields.

    /**
     * This vector holds send/receive messages for current query at this instance.
     * index in vector is source instance number.
     */
    std::vector<std::list<std::shared_ptr< MessageDesc>>> _receiveMessages;

    /**
     * This vector holds semaphores for working with messages queue. One semaphore for every source instance.
     */
    std::vector<Semaphore> _receiveSemaphores;

    /**
     * Incremented in Query constructor and decremented in Query destructor.
     */
    static std::atomic<uint64_t> _numOutstandingQueries;
};  // class Query


class UpdateErrorHandler : public Query::ErrorHandler
{
public:
    typedef boost::function< void(VersionID,ArrayID,ArrayID) > RollbackWork;

    explicit UpdateErrorHandler(const std::shared_ptr<LockDesc> & lock)
    : _lock(lock)
    {
        assert(_lock);
    }

    virtual ~UpdateErrorHandler() {}
    virtual void handleError(const std::shared_ptr<Query>& query);

    static void releaseLock(const std::shared_ptr<LockDesc>& lock,
                            const std::shared_ptr<Query>& query);

    static void handleErrorOnCoordinator(const std::shared_ptr<LockDesc>& lock,
                                         bool rollback);
    static void handleErrorOnWorker(const std::shared_ptr<LockDesc>& lock,
                                    bool forceCoordLockCheck,
                                    bool rollback);
private:
    static void doRollback(VersionID lastVersion,
                           ArrayID   baseArrayId,
                           ArrayID   newArrayId);
    void _handleError(const std::shared_ptr<Query>& query);

    UpdateErrorHandler(const UpdateErrorHandler&);
    UpdateErrorHandler& operator=(const UpdateErrorHandler&);

private:
    const std::shared_ptr<LockDesc> _lock;
    static log4cxx::LoggerPtr _logger;
};

class RemoveErrorHandler : public Query::ErrorHandler
{
public:
    explicit RemoveErrorHandler(const std::shared_ptr<LockDesc> & lock)
    : _lock(lock)
    {
        assert(_lock);
    }

    virtual ~RemoveErrorHandler() {}
    virtual void handleError(const std::shared_ptr<Query>& query);

    static bool handleRemoveLock(const std::shared_ptr<LockDesc>& lock,
                                 bool forceLockCheck);
private:
    RemoveErrorHandler(const RemoveErrorHandler&);
    RemoveErrorHandler& operator=(const RemoveErrorHandler&);

private:
    const std::shared_ptr<LockDesc> _lock;
    static log4cxx::LoggerPtr _logger;
};

class BroadcastAbortErrorHandler : public Query::ErrorHandler
{
 public:
    virtual void handleError(const std::shared_ptr<Query>& query);
    virtual ~BroadcastAbortErrorHandler() {}
 private:
    static log4cxx::LoggerPtr _logger;
};

class NewReplicationManager;

/**
 * The necessary context to perform replication during the execution of a query.
 */
class ReplicationContext
{
private:

    /**
     * Internal triplet container class.
     * It is used to hold the info needed for replication:
     *  - WorkQueue where incoming replication messages inserted
     *  - Array where the replicas are to be written
     *  - Semaphore for signaling when all replicas sent
     *    from this instance to all other instances are written
     */
    class QueueInfo
    {
    public:
        explicit QueueInfo(const std::shared_ptr<scidb::WorkQueue>& q)
            : _wq(q)
        {
            assert(q);
        }

        ~QueueInfo()
        {
            if (_wq) {
                _wq->stop();
            }
        }

        QueueInfo(const QueueInfo&) = delete;
        QueueInfo& operator=(const QueueInfo&) = delete;

        std::shared_ptr<scidb::WorkQueue> getQueue()     { return _wq; }
        std::shared_ptr<scidb::Array>     getArray()     { return _array; }
        scidb::Semaphore&                 getSemaphore() { return _replicaSem; }
        void setArray(const std::shared_ptr<Array>& arr) { _array = arr; }

    private:
        std::shared_ptr<scidb::WorkQueue> _wq;
        std::shared_ptr<scidb::Array>     _array;
        Semaphore _replicaSem;
    };

    typedef std::shared_ptr<QueueInfo> QueueInfoPtr;
    typedef std::map<ArrayID, QueueInfoPtr>  QueueMap;

    /**
     * Get inbound replication queue information for an array id
     * @param arrId array id
     */
    QueueInfoPtr getQueueInfo(ArrayID arrId);

    /**
     * Get inbound replication queue for a given ArrayID
     * @param arrId array ID
     * @return WorkQueue for enqueing replication jobs
     */
    std::shared_ptr<scidb::WorkQueue> getInboundQueue(ArrayID arrId);

private:

    Mutex _mutex;
    QueueMap _inboundQueues;
    std::weak_ptr<Query> _query;
    static NewReplicationManager* _replicationMngr;

public:
    /**
     * Constructor
     * @param query
     * @param nInstaneces
     */
    explicit ReplicationContext(const std::shared_ptr<Query>& query, size_t nInstances);

    /// Destructor
    virtual ~ReplicationContext() {}

    /**
     * Set up and start an inbound replication queue
     * @param arrId array ID of arr
     * @param arr array to which write replicas
     */
    void enableInboundQueue(ArrayID arrId, const std::shared_ptr<scidb::Array>& arr);

    /**
     * Enqueue a job to write a remote instance replica locally
     * @param arrId array ID to locate the appropriate queue
     * @param job replication job to enqueue
     */
    void enqueueInbound(ArrayID arrId, std::shared_ptr<Job>& job);

    /**
     * Wait until all replicas originated on THIS instance have been written
     * to the REMOTE instances
     * @param arrId array ID to identify the replicas
     */
    void replicationSync(ArrayID arrId);

    /**
     * Acknowledge processing of the last replication job from this instance on sourceId
     * @param sourceId instance ID where the replicas originated on this instance
     *        have been processed
     * @param arrId array ID to identify the replicas
     */
    void replicationAck(InstanceID sourceId, ArrayID arrId);

    /**
     * Remove the inbound replication queue and any related state
     * @param arrId array ID to locate the appropriate queue
     * @note It is the undo of enableInboundQueue()
     * @note currently NOOP
     */
    void removeInboundQueue(ArrayID arrId);

    /**
     * Get the persistent array for writing replicas
     * @param arrId array ID to locate the appropriate queue
     */
    std::shared_ptr<scidb::Array> getPersistentArray(ArrayID arrId);

public:

#ifndef NDEBUG // for debugging
    std::vector<Query::PendingRequests> _chunkReplicasReqs;
#endif
};

template<typename T, typename E>
T Query::runRestartableWork(boost::function<T()>& work, int tries)
{
    assert(work);
    int counter = tries;
    while (true)
    {
        //Run work
        try {
            return work();
        }
        //Detect recoverable exception
        catch (const E& e)
        {
            if (counter >= 0)
            {
                counter--;

                if (counter < 0)
                {
                    LOG4CXX_ERROR(_logger,
                                  "Query::runRestartableWork: Unable to restart work after "
                                  << tries << " tries");
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                           SCIDB_LE_CANNOT_RECOVER_RESTARTABLE_WORK);
                }
            }

            LOG4CXX_ERROR(_logger, "Query::runRestartableWork:"
                          << " Exception: "<< e.what()
                          << " will attempt to restart the operation");
            Thread::nanoSleep(getLockTimeoutNanoSec());
        }
    }
    ASSERT_EXCEPTION_FALSE("Unreachable return from Query::runRestartableWork");
    return T();
}

} // namespace

#endif /* QUERY_H_ */
