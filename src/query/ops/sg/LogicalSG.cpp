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
 * @file LogicalSG.cpp
 *
 * @author roman.simakov@gmail.com
 * @brief This file implement logical operator SCATTER/GATHER
 */

#include "SGParams.h"

#include <query/Operator.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: sg().
 *
 * @par Synopsis:
 *   sg( srcArray, partitionSchema, instanceId=-1, outputArray="", isStrict=false, isPullSG=false, offsetVector=null)
 *
 * @par Summary:
 *   SCATTER/GATHER distributes array chunks over the instances of a cluster.
 *   The result array is returned.
 *   It is the only operator that uses the network manager.
 *   Typically this operator is inserted by the optimizer into the physical plan.
 *
 * @par Input:
 *   -[0] srcArray: the source array, with srcAttrs and srcDims.
 *   -[1] distribution:<br>
 *     0 = psReplication, <br>
 *     1 = psHashPartitioned,<br>
 *     2 = psLocalInstance,<br>
 *     3 = psByRow,<br>
 *     4 = psByCol,<br>
 *     5 = psUndefined.<br>
 *   -[2] instanceId:<br>
 *     -2 = to coordinator (same with 0),<br>
 *     -1 = all instances participate,<br>
 *     0..#instances-1 = to a particular instance.<br>
 *     [TODO: The usage of instanceId, in calculating which instance a chunk should go to, requires further documentation.]
 *   -[3] isStrict if true, enables the data integrity checks such as for data collisions and out-of-order input chunks, defualt=false. <br>
 *   -[4] offsetVector: a vector of #dimensions values.<br>
 *     To calculate which instance a chunk belongs, the chunkPos is augmented with the offset vector before calculation.
 *     [TODO: offsetVector is not as important as some of the things it is inhibiting. support might get dropped.]
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class LogicalSG: public LogicalOperator
{
public:
    LogicalSG(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT(TID_UINT32);
        ADD_PARAM_VARIES();
    }

    Placeholders nextVaryParamPlaceholder(const vector<ArrayDesc> &schemas)
    {
        Placeholders res;

        // sanity check: #parameters is at least 1 (i.e. partitionSchema), but no more than #dims+4.
        if (_parameters.size()==0 || _parameters.size() > schemas[0].getDimensions().size() + SGPARAM_OFFSET_START) {
            SCIDB_ASSERT(false);
        }

        // the param is before the offset vector
        else if (_parameters.size() < SGPARAM_OFFSET_START) {
            res.push_back(END_OF_VARIES_PARAMS());

            switch (_parameters.size()) {
            case SGPARAM_INSTANCE_ID:
                res.push_back(PARAM_CONSTANT(TID_INT64)); // instanceID
                break;
            case SGPARAM_IS_STRICT:
                res.push_back(PARAM_CONSTANT(TID_BOOL));  // isStrict
                break;
            case SGPARAM_IS_PULL:
                res.push_back(PARAM_CONSTANT(TID_BOOL));  // isPull

                break;
            default:
                ASSERT_EXCEPTION(false, "LogicalSG::nextVaryParamPlaceholder");
                break;
            }
        }

        // the param is in the offset vector
        else if (_parameters.size() < schemas[0].getDimensions().size() + SGPARAM_OFFSET_START) {
            // along with the first value in the offset, we say the vector is optional
            if (_parameters.size()==SGPARAM_OFFSET_START) {
                res.push_back(END_OF_VARIES_PARAMS());
            }
            res.push_back(PARAM_CONSTANT(TID_INT64));
        }

        // after the offset vector
        else {
            assert(_parameters.size() == schemas[0].getDimensions().size() + SGPARAM_OFFSET_START);
            // ASSERT_EXCEPTION?
            res.push_back(END_OF_VARIES_PARAMS());
        }

        return res;
    }

private:

    /// @return  validated partitioning scheme specified by the user
    /// @throws scidb::SystemException if the scheme is unsupported
    /// @todo XXX TODO: change PartitioningSchema to DistributionId
    PartitioningSchema getPartitioningSchema(const std::shared_ptr<Query>& query) const
    {
        ASSERT_EXCEPTION(_parameters[0], "Partitioning schema is not specified by the user");
        OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[0].get());
        const PartitioningSchema ps = static_cast<PartitioningSchema>(
            evaluate(lExp->getExpression(), TID_INT32).getInt32());
        if (! isValidPartitioningSchema(ps, false)  && ps != psLocalInstance)
        {
            // Do not allow optional data associated with the partitioning schema
            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_REDISTRIBUTE_ERROR);
        }
        return ps;
    }

    /// @return logical instance ID specified by the user, or ALL_INSTANCE_MASK if not specified
    InstanceID getInstanceId(const std::shared_ptr<Query>& query) const
    {
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() >=2 )
        {
            OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[1].get());
            instanceId = static_cast<InstanceID>(
                evaluate(lExp->getExpression(), TID_INT64).getInt64());
        }
        instanceId = (instanceId==COORDINATOR_INSTANCE_MASK) ? query->getInstanceID() : instanceId;
        return instanceId;
    }

    /// @return the coordinate offset specified by the user, or empty offset if not specified
    /// @note support for offset vector might get dropped evenutally.
    DimensionVector getOffsetVector(const vector<ArrayDesc> & inputSchemas,
                                    const std::shared_ptr<Query>& query) const
    {
        if (_parameters.size() <= SGPARAM_OFFSET_START) {
            return DimensionVector();
        }

        const Dimensions&  dims = inputSchemas[0].getDimensions();
        DimensionVector result(dims.size());
        ASSERT_EXCEPTION(_parameters.size() == dims.size() + SGPARAM_OFFSET_START,
                         "Invalid coordinate offset is specified");

        for (size_t i = 0; i < result.numDimensions(); ++i) {
            OperatorParamLogicalExpression* lExp =
                static_cast<OperatorParamLogicalExpression*>(_parameters[i+SGPARAM_OFFSET_START].get());
            result[i] = static_cast<Coordinate>(
                evaluate(lExp->getExpression(), TID_INT64).getInt64());
        }
        return result;
    }

    public:

    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query) override
    {
        assert(inputSchemas.size() == 1);
        ArrayDesc const& desc = inputSchemas[0];

        //validate the partitioning schema
        const PartitioningSchema ps = getPartitioningSchema(query) ;
        InstanceID localInstance = getInstanceId(query);
        DimensionVector offset = getOffsetVector(inputSchemas,query);

        if (isDebug()) {
            if (_parameters.size() > SGPARAM_IS_STRICT) { // isStrict present
                assert(_parameters[SGPARAM_IS_STRICT]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[SGPARAM_IS_STRICT].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
            if (_parameters.size() > SGPARAM_IS_PULL) { // isPull present
                assert(_parameters[SGPARAM_IS_PULL]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[SGPARAM_IS_PULL].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
        }

        string distCtx;
        if (ps == psLocalInstance) {
            ASSERT_EXCEPTION((localInstance < query->getInstancesCount()),
                             "The specified instance is larger than total number of instances");
            stringstream ss;
            ss<<localInstance;
            distCtx = ss.str();
        }

        std::shared_ptr<CoordinateTranslator> translator;
        if (!offset.isEmpty()) {
            translator = OffsetCoordinateTranslator::createOffsetMapper(offset);
        }

        ArrayDistPtr arrDist = ArrayDistributionFactory::getInstance()->construct(ps,
                                                                                  DEFAULT_REDUNDANCY,
                                                                                  distCtx,
                                                                                  translator,
                                                                                  0);
        return ArrayDesc(desc.getName(),
                         desc.getAttributes(),
                         desc.getDimensions(),
                         arrDist,
                         query->getDefaultArrayResidency()); // use the query live set because we dont know better
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSG, "_sg")

} //namespace
