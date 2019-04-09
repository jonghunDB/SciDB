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
#include <malloc.h>
#include <string.h>
#include <sstream>

#include <log4cxx/logger.h>

#include <query/Parser.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <array/TupleArray.h>
#include <array/DelegateArray.h>
#include <array/TransientCache.h>
#include <system/SystemCatalog.h>
#include <query/TypeSystem.h>
#include <util/PluginManager.h>
#include "ListArrayBuilders.h"
#include <rbac/NamespacesCommunicator.h>
#include <rbac/NamespaceDesc.h>
#include <rbac/Rbac.h>
#include <rbac/Rights.h>
#include <rbac/RoleDesc.h>
#include <rbac/UserDesc.h>
#include <rbac/Session.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.list"));

using namespace std;
using NsComm = namespaces::Communicator;

struct PhysicalList : PhysicalOperator
{
    PhysicalList(const string& logicalName,
                 const string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    string getMainParameter() const
    {
        if (_parameters.empty())
        {
            return "arrays";
        }

        OperatorParamPhysicalExpression& exp =
            *(std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0];
        return exp.getExpression()->evaluate().getString();
    }

    bool getShowSysParameter() const
    {
        if (_parameters.size() < 2)
        {
            return false;
        }

        OperatorParamPhysicalExpression& exp =
            *(std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        return exp.getExpression()->evaluate().getBool();
    }

    NamespaceDesc getNsParameter(std::shared_ptr<Query> const& query) const
    {
        Parameter p1 = findKeyword(KW_NS);
        Parameter p2 = findKeyword(KW_NS_ABBREV);
        SCIDB_ASSERT(!p1 || !p2); // Never both.

        if (p1) {
            OperatorParamNamespaceReference const& ref =
                dynamic_cast<OperatorParamNamespaceReference const&>(*p1);
            return ref.getNamespace();
        }
        if (p2) {
            OperatorParamNamespaceReference const& ref =
                dynamic_cast<OperatorParamNamespaceReference const&>(*p2);
            return ref.getNamespace();
        }
        return query->getSession()->getNamespace();
    }

    bool coordinatorOnly() const
    {
        /* The operations NOT in this list run exclusively on the coordinator.
           Note, we use binary search on this list, it must be ordered
           These strings must be in lexicographical order.
         */
        static const char* const s[] =
        {
            "buffer stats",
            "chunk map",            // back compatible with new storage mgr
            "datastores",
            "disk indexes",
            "libraries",
            "meminfo",
            "queries",
        };

        return !std::binary_search(s,s+SCIDB_SIZE(s),getMainParameter().c_str(),less_strcmp());
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                      const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (coordinatorOnly()) {
            stringstream ss;

            std::shared_ptr<Query> query(_query);
            SCIDB_ASSERT(query);

            ss << query->getInstanceID();
            ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(psLocalInstance,
                                                                                        DEFAULT_REDUNDANCY,
                                                                                        ss.str());
            ArrayDesc* mySchema = const_cast<ArrayDesc*>(&_schema);
            mySchema->setDistribution(localDist);
        }
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        if (coordinatorOnly() && !query->isCoordinator())
        {
            LOG4CXX_TRACE(logger, "list physical,  what = "
                          << getMainParameter()
                          << " coordinator only, not coordinator");
            return make_shared<MemArray>(_schema,query);
        }

        vector<string> items;
        string  const  what = getMainParameter();
        bool           showSys = getShowSysParameter();

        LOG4CXX_TRACE(logger, "list physical,  what = " << what);

        if (what == "aggregates") {
            ListAggregatesArrayBuilder builder(
                AggregateLibrary::getInstance()->getNumAggregates(), showSys);
            builder.initialize(query);
            AggregateLibrary::getInstance()->visitPlugins(
                AggregateLibrary::Visitor(
                    boost::bind(
                        &ListAggregatesArrayBuilder::list,&builder,_1,_2,_3)));
            return builder.getArray();
        } else if (what == "arrays") {

            bool showAllArrays = false;
            if (_parameters.size() == 2)
            {
                showAllArrays = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
                    _parameters[1])->getExpression()->evaluate().getBool();
            }
            return listArrays(showAllArrays, query);

        } else if (what == "operators") {
            OperatorLibrary::getInstance()->getLogicalNames(items, showSys);
            std::shared_ptr<TupleArray> tuples(std::make_shared<TupleArray>(_schema, _arena));
            for (size_t i=0, n=items.size(); i!=n; ++i) {
                constexpr size_t kName = 0;
                constexpr size_t kLibrary = 1;
                constexpr size_t kInternal = 2;
                constexpr size_t kDdl = 3;
                constexpr size_t kNestable = 4;
                vector<Value> tuple(showSys ? 5 : 2);
                tuple[kName].setString(items[i]);
                tuple[kLibrary].setString(
                    OperatorLibrary::getInstance()->getOperatorLibraries().getObjectLibrary(items[i]));
                if (showSys) {
                    auto logicalOp = OperatorLibrary::getInstance()->createLogicalOperator(items[i]);
                    const bool ddl = logicalOp && logicalOp->getProperties().ddl;
                    const bool nestable = logicalOp && !logicalOp->getProperties().noNesting;
                    tuple[kInternal].setBool(OperatorLibrary::isHiddenOp(items[i]));
                    tuple[kDdl].setBool(ddl);
                    tuple[kNestable].setBool(nestable);
                }
                tuples->appendTuple(PointerRange<Value>(tuple.size(), &tuple[0]));
            }
            return tuples;
        } else if (what == "types") {
            items = TypeLibrary::typeIds();
            std::shared_ptr<TupleArray> tuples(std::make_shared<TupleArray>(_schema, _arena));
            for (size_t i=0, n=items.size(); i!=n; ++i) {
                Value tuple[2];
                tuple[0].setString(items[i]);
                tuple[1].setString(
                    TypeLibrary::getTypeLibraries().getObjectLibrary(items[i]));
                tuples->appendTuple(tuple);
            }
            return tuples;
        } else if (what == "functions") {
            std::shared_ptr<TupleArray> tuples(std::make_shared<TupleArray>(_schema, _arena));
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin();
                 i != funcs.end(); ++i)
            {
                for (funcDescTypesMap::const_iterator j = i->second.begin();
                     j != i->second.end(); ++j)
                {
                    Value tuple[4];
                    FunctionDescription const& func = j->second;
                    tuple[0].setString(func.getName());
                    tuple[1].setString(func.getMangleName());
                    tuple[2].setBool(func.isDeterministic());
                    tuple[3].setString(FunctionLibrary::getInstance()->getFunctionLibraries()
                                       .getObjectLibrary(func.getMangleName()));
                    tuples->appendTuple(tuple);
                }
            }
            Value tuple1[4];
            tuple1[0].setString("iif");
            tuple1[1].setString("<any> iif(bool, <any>, <any>)");
            tuple1[2].setBool(true);
            tuple1[3].setString("scidb");
            tuples->appendTuple(tuple1);

            Value tuple2[4];
            tuple2[0].setString("missing_reason");
            tuple2[1].setString("int32 missing_reason(<any>)");
            tuple2[2].setBool(true);
            tuple2[3].setString("scidb");
            tuples->appendTuple(tuple2);

            Value tuple3[4];
            tuple3[0].setString("sizeof");
            tuple3[1].setString("uint64 sizeof(<any>)");
            tuple3[2].setBool(true);
            tuple3[3].setString("scidb");
            tuples->appendTuple(tuple3);

            return tuples;
        } else if (what == "macros") {
            return physicalListMacros(_arena, query, showSys);
        } else if (what == "queries") {
            ListQueriesArrayBuilder builder(query);
            builder.initialize(query);
            Query::visitQueries(
                Query::Visitor(
                    boost::bind(
                        &ListQueriesArrayBuilder::list, &builder, _1)));
            return builder.getArray();
        } else if (what == "instances") {
            return listInstances(query);
        } else if (what == "users") {
            return listUsers(query);
        } else if (what == "roles") {
            return listRoles(query);
        } else if (what == "namespaces") {
            return listNamespaces(query);
        }
        else if (what == "chunk map") {
            ListChunkMapArrayBuilder builder;
            builder.initialize(query);
            IndexMgr<DbAddressMeta>::getInstance()->visitDiskIndexes(
                DiskIndex<DbAddressMeta>::DiskIndexVisitor(
                    boost::bind(&ListChunkMapArrayBuilder::list, &builder, _1, _2, _3)),
                /*residentOnly*/ false);
            return builder.getArray();
       } else if (what == "disk indexes") {
            ListDiskIndexArrayBuilder builder;
            builder.initialize(query);
            IndexMgr<DbAddressMeta>::getInstance()->visitDiskIndexes(
                DiskIndex<DbAddressMeta>::DiskIndexVisitor(
                    boost::bind(&ListDiskIndexArrayBuilder::list, &builder, _1, _2, _3)),
                /*residentOnly*/ true);
            return builder.getArray();
        } else if (what == "buffer stats") {
            ListBufferStatsArrayBuilder builder;
            builder.initialize(query);
            builder.list();
            return builder.getArray();
        } else if (what == "libraries") {
            ListLibrariesArrayBuilder builder;
            builder.initialize(query);
            PluginManager::getInstance()->visitPlugins(
                PluginManager::Visitor(
                    boost::bind(
                        &ListLibrariesArrayBuilder::list,&builder,_1)));
            return builder.getArray();
        } else if (what == "datastores") {
            ListDataStoresArrayBuilder builder;
            builder.initialize(query);
            DataStores::getInstance()->visitDataStores(
                DataStores::Visitor(
                    boost::bind(
                        &ListDataStoresArrayBuilder::list,&builder,_1)));
            return builder.getArray();
        } else if (what == "counters") {
            bool reset = false;
            if (_parameters.size() == 2)
            {
                reset = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
                         _parameters[1])->getExpression()->evaluate().getBool();
            }
            ListCounterArrayBuilder builder;
            builder.initialize(query);
            CounterState::getInstance()->visitCounters(
                CounterState::Visitor(
                    boost::bind(
                        &ListCounterArrayBuilder::list,&builder,_1)));
            if (reset)
            {
                CounterState::getInstance()->reset();
            }
            return builder.getArray();
        }
        else
        {
            SCIDB_UNREACHABLE();
        }

        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "PhysicalList::execute";
     }

    std::shared_ptr<Array> listInstances(
        const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<const InstanceLiveness> queryLiveness(
            query->getCoordinatorLiveness());

        Instances instances;
        SystemCatalog::getInstance()->getInstances(instances);

        assert(queryLiveness->getNumInstances() <= instances.size());

        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        for (   Instances::const_iterator iter = instances.begin();
                iter != instances.end();
                ++iter)
        {
            Value tuple[5];

            const InstanceDesc& instanceDesc = *iter;
            InstanceID instanceId = instanceDesc.getInstanceId();
            time_t t = static_cast<time_t>(instanceDesc.getOnlineSince());
            tuple[0].setString(instanceDesc.getHost());
            tuple[1].setUint16(instanceDesc.getPort());
            tuple[2].setUint64(instanceId);
            if ((t == (time_t)0) ||
                queryLiveness->isDead(instanceId) ||
                !queryLiveness->find(instanceId)) {
                tuple[3].setString("offline");
            } else {
                struct tm date;

                if (!(&date == gmtime_r(&t, &date)))
                {
                  throw SYSTEM_EXCEPTION(
                      SCIDB_SE_EXECUTION,
                      SCIDB_LE_CANT_GENERATE_UTC_TIME);
                }

                string out(boost::str(
                    boost::format("%04d-%02d-%02d %02d:%02d:%02d")
                        % (date.tm_year+1900)
                        % (date.tm_mon+1)
                        % date.tm_mday
                        % date.tm_hour
                        % date.tm_min
                        % date.tm_sec));
                tuple[3].setString(out);
            }
            tuple[4].setString(instanceDesc.getPath());
            tuples->appendTuple(tuple);
        }
        return tuples;
    }

    std::shared_ptr<Array> listUsers(
        const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        std::vector<scidb::UserDesc> users;
        rbac::listUsers(users);
        for (size_t i=0, n=users.size(); i!=n; ++i) {
            scidb::UserDesc &user = users[i];
            Value tuple[2];
            tuple[0].setString(user.getName());
            tuple[1].setUint64(user.getId());
            tuples->appendTuple(tuple);
        }

        return tuples;
    }

    std::shared_ptr<Array> listRoles(
        const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        std::vector<scidb::RoleDesc> roles;
        scidb::namespaces::Communicator::getRoles(roles);
        for (size_t i=0, n=roles.size(); i!=n; ++i) {
            scidb::RoleDesc &role = roles[i];
            Value tuple[2];
            tuple[0].setString(role.getName());
            tuple[1].setUint64(role.getId());
            tuples->appendTuple(tuple);
        }

        return tuples;
    }

     std::shared_ptr<Array> listNamespaces(
         const std::shared_ptr<Query>& query)
     {
        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        // Add only the namespaces that we have permission to list
        std::vector<NamespaceDesc> namespaces;
        SystemCatalog::getInstance()->getNamespaces(namespaces);
        for (auto const& nsDesc : namespaces) {
            rbac::RightsMap rights;
            rights.upsert(rbac::ET_NAMESPACE, nsDesc.getName(), rbac::P_NS_LIST);
            try {
                NsComm::checkAccess(query->getSession().get(), &rights);
            }
            catch (Exception&) {
                continue;
            }

            Value tuple[1];
            tuple[0].setString(nsDesc.getName());
            tuples->appendTuple(tuple);
        }

        return tuples;
    }


    std::shared_ptr<Array> listArrays(
        bool showAllVersions,
        const std::shared_ptr<Query>& query)
    {
        // Which namespace?  Empty string means "all".
        string nsName;
        NamespaceDesc requestedNs(getNsParameter(query));
        if (requestedNs.getId() != rbac::ALL_NS_ID) {
            SCIDB_ASSERT(requestedNs.isIdValid());
            nsName = requestedNs.getName();
        }

        // Collect entries.
        vector<ArrayDesc> arrayDescs;
        const bool ignoreOrphanAttributes = true;
        const bool orderByName = true;
        SystemCatalog::getInstance()->getArrays(nsName,
                                                arrayDescs,
                                                ignoreOrphanAttributes,
                                                !showAllVersions,
                                                orderByName);

        // Build list.
        ListArraysArrayBuilder builder;
        builder.initialize(query);
        for (ArrayDesc const& arrayDesc : arrayDescs) {
            // filter out metadata introduced after the catalog version of this query/txn
            // XXX TODO: this does not deal with the arrays not locked by this query
            // XXX TODO: (they can be added/updated/removed mid-flight, i.e. before list::execute() runs).
            // XXX TODO: Either make list() take an 'ALL' array lock or
            // XXX TODO: introduce a single serialized PG timestamp, or ...
            const ArrayID catVersion = query->getCatalogVersion(
                query->getNamespaceName(), arrayDesc.getName(), true);

            if (arrayDesc.getId() <= catVersion && arrayDesc.getUAId() <= catVersion)
            {
                builder.list(arrayDesc);
            }
        }

        return builder.getArray();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalList, "list", "physicalList")

/****************************************************************************/
}
/****************************************************************************/
