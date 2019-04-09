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
 * @file PhysicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of INPUT operator for inputing data from text file
 * which is located on coordinator
 */

#include "InputArray.h"
#include "InputSettings.h"

#include <query/Operator.h>
#include <query/PhysicalUpdate.h>
#include <query/QueryProcessor.h>
#include <query/QueryPlan.h>
#include <system/Cluster.h>
#include <util/PathUtils.h>

#include <log4cxx/logger.h>
#include <string.h>

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.impl_input"));

class ParsingContext;
class PhysicalInput : public PhysicalOperator
{
public:
    PhysicalInput(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    bool isSingleThreaded() const override
    {
        return false;
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    void inspectLogicalOp(LogicalOperator const& lop) override
    {
        // Learn the InputSettings computed in our logical operator.  Putting this info into the
        // control cookie ensures that all workers can learn it too.  Finally, use it to init our
        // own _settings object so it's ready for preSingleExecute().

        string inspectable(lop.getInspectable());
        setControlCookie(inspectable);
        _settings.fromString(inspectable);
    }

    int64_t getSourceInstanceID() const
    {
        SCIDB_ASSERT(_settings.isValid());
        return _settings.getInstanceId();
    }

    bool canToggleStrict() const override
    {
        return true;            // Yes, optimizer can call getIsStrict().
    }

    bool getIsStrict() const override
    {
        SCIDB_ASSERT(_settings.isValid());
        return _settings.getIsStrict();
    }

    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const&,
            std::vector<ArrayDesc> const&) const
    {
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == ALL_INSTANCE_MASK) {
            //The file is loaded from multiple instances - the distribution could be possibly violated - assume the worst

            ArrayDistPtr undefDist = createDistribution(psUndefined);

            if (!_preferredInputDist) {
                _preferredInputDist = _schema.getDistribution();
                SCIDB_ASSERT(_preferredInputDist);
            }
            ArrayDesc* mySchema = const_cast<ArrayDesc*>(&_schema);
            mySchema->setDistribution(undefDist);
        } else {
            SCIDB_ASSERT(_schema.getDistribution()->getPartitioningSchema() == psLocalInstance);
        }
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                     std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() >= 2);
        if (!_settings.isValid()) {
            // Already valid on the coordinator (we did it in inspectLogicalOp() so it'd be ready
            // for preSingleExecute()).  For other workers, initialize _settings here.
            _settings.fromString(getControlCookie());
            SCIDB_ASSERT(_settings.isValid());
            LOG4CXX_DEBUG(oplogger, "Received input() settings: " << _settings);
        }

        assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        std::shared_ptr<OperatorParamPhysicalExpression> paramExpr =
            (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        assert(paramExpr->isConstant());
        const string fileName = _settings.getPath();

        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) {
            sourceInstanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());

        } else if (sourceInstanceID != ALL_INSTANCE_MASK) {
            SCIDB_ASSERT(_schema.getDistribution()->getPartitioningSchema() == psLocalInstance);
            SCIDB_ASSERT(isValidPhysicalInstance(sourceInstanceID));
            sourceInstanceID = safe_dynamic_cast<const LocalArrayDistribution*>(_schema.getDistribution().get())->getLogicalInstanceId();
        }

        InstanceID myInstanceID = query->getInstanceID();
        int64_t maxErrors = _settings.getMaxErrors();
        bool enforceDataIntegrity = _settings.getIsStrict();

        std::shared_ptr<Array> result;
        bool emptyArray = (sourceInstanceID != ALL_INSTANCE_MASK &&
                           sourceInstanceID != myInstanceID);
        if (_preferredInputDist) {
            _schema.setDistribution(_preferredInputDist);
        }
        InputArray* ary = new InputArray(_schema, _settings.getFormat(), query, getShared(),
                                         emptyArray,
                                         enforceDataIntegrity,
                                         maxErrors,
                                         sourceInstanceID == ALL_INSTANCE_MASK); // last bool is parallelLoad indicator
        result.reset(ary);

        if (emptyArray) {
            // No need to actually open the file.  (In fact, if the file is a pipe and
            // double-buffering is enabled, opening it would wrongly steal data intended for
            // some other instance!  See ticket #4466.)
            SCIDB_ASSERT(ary->inEmptyMode());
        } else {
            try
            {
                ary->openFile(fileName);
            }
            catch(const Exception& e)
            {
                if (e.getLongErrorCode() != SCIDB_LE_CANT_OPEN_FILE)
                {
                    // Only expecting an open failure, but whatever---pass it up.
                    throw;
                }

                if (sourceInstanceID == myInstanceID)
                {
                    // If mine is the one-and-only load instance, let
                    // callers see the open failure.
                    throw;
                }

                // No *local* file to load, so return the "inEmptyMode"
                // InputArray result. (Formerly that was needed because
                // the "shadow array" feature relied on it.  It currently
                // does not matter, but might if shadow array
                // functionality were re-implemented.)
                // The open failure itself has already been logged.

                assert(ary->inEmptyMode()); // ... regardless of emptyArray value above.
            }
        }

        SCIDB_ASSERT(!_preferredInputDist ||
                     result->getArrayDesc().getDistribution()->getPartitioningSchema() == psUndefined);

        return result;
    }

private:

    mutable ArrayDistPtr _preferredInputDist;
    mutable InputSettings _settings;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInput, "input", "impl_input")

} //namespace
