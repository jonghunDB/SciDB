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
 * PhysicalBuild.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "array/Metadata.h"
#include "BuildArray.h"
#include "query/ops/input/InputArray.h"

using namespace std;
using namespace boost;

namespace scidb {

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.build"));
#define debug(e) LOG4CXX_DEBUG(logger, "PhysicalBuild: " << e)
#define trace(e) LOG4CXX_TRACE(logger, "PhysicalBuild: " << e)

class PhysicalBuild: public PhysicalOperator
{
public:
    PhysicalBuild(const string& logicalName,
                  const string& physicalName,
                  const Parameters& parameters,
                  const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
        , _asArrayLiteral(false)
    {
        // Can't check keywords until setKeywordParameters() below.  Must allow for "build(schema,
        // from: string)" here: not an error if only one parameter given.
        if (_parameters.size() == 3) {
            // "build(schema, expr, bool)"
            _asArrayLiteral =
                ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2])->getExpression()->evaluate().getBool();
            _exprParam = _parameters[1];
        } else if (_parameters.size() == 2) {
            _exprParam = _parameters[1];
        } else {
            // "build(schema, from: string)" but haven't seen the keywords yet.
            // Could set _asArrayLiteral here if need be.
            SCIDB_ASSERT(_parameters.size() == 1);
        }
    }

    void setKeywordParameters(KeywordParameters& kwParams) override
    {
        _kwParameters.swap(kwParams);
        Parameter p = findKeyword("from");
        if (p) {
            SCIDB_ASSERT(!_exprParam);
            _exprParam = p;
            _asArrayLiteral = true;
        } else {
            SCIDB_ASSERT(_exprParam);
        }
    }

    PartitioningSchema getOutDist() const
    {
        if (_asArrayLiteral) {
            debug("getOutDist: returning psLocalInstance =" << psLocalInstance);
            return psLocalInstance;
        }
        debug("getOutputDistribution: returning defaultPartitioning =" << defaultPartitioning());
        return defaultPartitioning()->getPartitioningSchema();
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                      const std::vector< ArrayDesc> & inputSchemas) const
    {
        assert(inputDistributions.size() == 0);
        assert(getOutDist() == _schema.getDistribution()->getPartitioningSchema());
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /**
     * Build is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        debug("execute: _schema  distribution:  " << _schema.getDistribution() << ", getOutDist(): " << getOutDist());
        assert(_schema.getDistribution()->getPartitioningSchema() == getOutDist());

        std::shared_ptr<Array> result;

        std::shared_ptr<Expression> expr =
            ((std::shared_ptr<OperatorParamPhysicalExpression>&)_exprParam)->getExpression();
        if (_asArrayLiteral)
        {
            assert(_schema.getDistribution()->getPartitioningSchema() == psLocalInstance);   // cf getOutputDistribution
            //We will produce this array only on coordinator
            if (query->isCoordinator())
            {
                //InputArray is very access-restrictive, but we're building it from a string - so it's small!
                //So why don't we just materialize the whole literal array:
                static const bool dontEnforceDataIntegrity = false;
                static const bool notInEmptyMode = false;
                static const int64_t maxCnvErrors(0);
                static const bool notParallelLoad = false;
                InputArray* ary = new InputArray(_schema, "",
                                                 query,
                                                 getShared(),
                                                 notInEmptyMode,
                                                 dontEnforceDataIntegrity,
                                                 maxCnvErrors,
                                                 notParallelLoad);
                std::shared_ptr<Array> input(ary);
                ary->openString(expr->evaluate().getString());
                std::shared_ptr<Array> materializedInput(new MemArray(input->getArrayDesc(),query));
                materializedInput->appendHorizontal(input);
                return materializedInput;
            }
            else
            {
                result = make_shared<MemArray>(_schema,query);
            }
        }
        else
        {
            result = make_shared<BuildArray>(query, _schema, expr);
        }

        debug("execute: returning array with distribution:  " << result->getArrayDesc().getDistribution()
              << ", getOutDist(): " << getOutDist());
        assert(result->getArrayDesc().getDistribution()->getPartitioningSchema() == getOutDist());
        return result;
    }

private:
    bool _asArrayLiteral;
    Parameter _exprParam;
};

} // namespace

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalBuild, "build", "physicalBuild")

}  // namespace scidb
