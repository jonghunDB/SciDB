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
 * @file SciDBAPI.h
 * @author roman.simakov@gmail.com
 * @author smirnoffjr@gmail.com
 *
 * @brief SciDB API
 *
 * This file describes common SciDB API accessible by users.
 * Currently we provide two implementations of this API: remote and embedded.
 *
 * Remote is a client shared library that can be linked into client process
 * and communicate with coordinator of cluster by message exchanging.
 *
 * Embedded version is linked with scidb implementation and can be loaded into client process.
 * In this case client process becomes a instance of scidb cluster.
 */

#ifndef SCIDBAPI_H_
#define SCIDBAPI_H_

#include <stdint.h>
#include <queue>

#include <query/QueryID.h>
#include <array/Array.h>
#include <system/Warnings.h>

#define EXPORTED_FUNCTION extern "C" __attribute__((visibility("default")))

namespace scidb
{

class Query;
class SessionProperties;
typedef std::queue<Warning> WarningsQueue;

/**
 * Query execution statistic
 */
class QueryResult
{
public:
    QueryResult()
    : selective(false), autoCommit(false),
      requiresExclusiveArrayAccess(false), executionTime(0)
    {
    }

#ifdef SCIDB_CLIENT
    ~QueryResult();
#endif

    // Query result fields
    QueryID queryID;
    bool selective;
    bool autoCommit;
    bool requiresExclusiveArrayAccess;
    std::shared_ptr<Array> array;

    // Statistics fields
    uint64_t executionTime;  // In milliseconds
    std::string explainLogical;
    std::string explainPhysical; // Every executed physical plan separated by ';'

    std::vector<std::string> plugins; /**< a list of plugins containing UDT in result array */
    std::vector< std::shared_ptr<Array> > mappingArrays;

#ifdef SCIDB_CLIENT
    bool hasWarnings();
    Warning nextWarning();

private:
    void _postWarning(const Warning &warning);

    WarningsQueue _warnings;
    Mutex _warningsLock;

    friend class SciDBWarnings;
#endif
};

enum StatementType {
    AQL = 0,
    AFL = 1
};

/**
 * This class provides an abstract interface to scidb client library.
 * Use this class to connect, execute queries and get result.
 */
class SciDB
{
public:
    static const uint16_t DEFAULT_PORT=1239;

    /**
     * Destructor
     */
    virtual ~SciDB() {}

    /**
     * @brief Connect client to a scidb instance
     * @param[in] props properties to configure the session
     * @param[in] host hostname or address of scidb instance
     * @param[in] port TCP port of scidb instance
     * @return a handle for connection
     * @throw scidb::Exception on failure
     */
    virtual void* connect(SessionProperties const& props,
                          const std::string& host = "localhost",
                          uint16_t port = DEFAULT_PORT) = 0;

    /**
     * Disconnect client from a coordinator of scidb cluster.
     * @param connection is handle to connection returned by connect method.
     */
    virtual void disconnect(void* connection = NULL) = 0;

    /**
     * Prepare a query string. Throws exception if an error occurred.
     * @param queryString a string with query on scidb language.
     * @param queryResult a reference to QueryResult structure with description of query execution result.
     * @param connection is handle to connection returned by connect method.
     */
    virtual void prepareQuery(const std::string& queryString, bool afl,
                              const std::string& programOptions,
                              QueryResult& queryResult,
                              void* connection = NULL) = 0;

    /**
     * Execute a query string. Throws exception if an error occurred.
     * @param queryString a string with query on scidb language.
     * @param queryResult a reference to QueryResult structure with description of query execution result.
     * if queryResult has queryID > 0 query will not be prepared and queryString is ignored.
     * @param connection is handle to connection returned by connect method.
     */
    virtual void executeQuery(const std::string& queryString, bool afl,
                              QueryResult& queryResult,
                              void* connection = NULL) = 0;

    /**
     * @param connection is handle to connection returned by connect method
     * Cancel current query execution, rollback any changes on disk, free the query reqources
     */
    virtual void cancelQuery(QueryID queryID, void* connection = NULL) = 0;

    /**
     * @param connection is handle to connection returned by connect method
     * Commit and free resources if query already finished.
     */
    virtual void completeQuery(QueryID queryID, void* connection = NULL) = 0;

#ifndef SCIDB_CLIENT
    // Server-side functions.
    //
    // NOTE: All server functions MUST appear here at the end,
    //       otherwise the vtable pointers for methods used in common
    //       code (that is, code shared by clients and servers) will
    //       be messed up.  You'll get a segfault or worse.
    //
    // XXX TODO: the client API must NOT be the same as the server API
    //           the interfaces must be divorced
    virtual void retryPrepareQuery(const std::string& queryString,
                                   bool afl,
                                   const std::string& programOptions,
                                   QueryResult& queryResult) = 0;
    virtual void startExecuteQuery(const std::string& queryString,
                                   bool afl,
                                   QueryResult& queryResult) = 0;
    virtual void completeExecuteQuery(QueryResult& queryResult,
                                      const std::shared_ptr<Query>& query) = 0;
#endif
};


/**
 * E X P O R T E D   F U N C T I O N S
 */
/**
 *  A function for returning SciDB instance
 *  @return a reference to scidb API.
 */
#ifdef SWIG
SciDB& getSciDB();
#else
EXPORTED_FUNCTION SciDB& getSciDB();
#endif

} // namespace scidb
#endif /* SCIDBAPI_H_ */
