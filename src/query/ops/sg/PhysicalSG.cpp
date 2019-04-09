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

#include "SGParams.h"

#include <memory>
#include <log4cxx/logger.h>

#include <array/NewDBArray.h>
#include <array/DelegateArray.h>
#include <array/TransientCache.h>
#include <network/MessageUtils.h>
#include <query/QueryPlan.h>
#include <query/QueryProcessor.h>
#include <system/SystemCatalog.h>

using namespace std;
using namespace boost;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.sg"));

// main operator

class PhysicalSG : public PhysicalOperator
{
public:
    /// @see PhysicalOperator::PhysicalOperator()
    PhysicalSG(const string& logicalName, const string& physicalName,
               const Parameters& parameters, const ArrayDesc& schema);

    // PhysicalOperator::getDistributionRquirement() not overridden

    bool outputFullChunks(std::vector< ArrayDesc> const&) const override { return true; }

    /// @see PhysicalOperator::getOutputDistribution()
    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                              const std::vector< ArrayDesc> & inputSchemas) const override;

    /// @see PhysicalOperator::getOutputBoundaries()
    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector<ArrayDesc> & inputSchemas) const override
    { return inputBoundaries[0]; }

    // requiresRedimensionOrPartition


    // getOutputDistribution


    // isSingleThreaded

    // preSingleExecute

    /// @see PhysicalOperator::execute()
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override ;
private:
    PartitioningSchema getPartitioningSchema() const ;

    DimensionVector getOffsetVector(const vector<ArrayDesc> & inputSchemas) const ;

    /// @return logical instance ID specified by the user, or ALL_INSTANCE_MASK if not specified
    InstanceID getInstanceId(const std::shared_ptr<Query>& query) const ;

};

//
// begin implementation
//

// ctor
PhysicalSG::PhysicalSG(const string& logicalName, const string& physicalName,
                       const Parameters& parameters, const ArrayDesc& schema)
:
    PhysicalOperator(logicalName, physicalName, parameters, schema)
{}

// getDistributionRequirement

// getPartitioningSchema
PartitioningSchema PhysicalSG::getPartitioningSchema() const
{
        // The operator parameters at this point are not used to determine the behavior of SG.
        // It is _schema which determines behavior:
        // it should be correctly populated either by the optimizer or by LogicalSG.

        // Here, we just check _schema is consistent with the parameters.
        SCIDB_ASSERT(_parameters[SGPARAM_DISTRIBUTION]);
        OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_DISTRIBUTION].get());
        PartitioningSchema ps = static_cast<PartitioningSchema>(pExp->getExpression()->evaluate().getInt32());
        SCIDB_ASSERT(ps == _schema.getDistribution()->getPartitioningSchema());
        return ps;
}

/// @see PhysicalOperator::getOutputDistribution()
RedistributeContext
PhysicalSG::getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                  const std::vector< ArrayDesc> & inputSchemas) const
{
        if (isDebug()) {
            // Verify that _schema & _parameters are in sync
            ArrayDistPtr arrDist = _schema.getDistribution();
            SCIDB_ASSERT(arrDist);
            SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUninitialized);
            SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUndefined);
            std::shared_ptr<Query> query(_query);
            SCIDB_ASSERT(query);

            PartitioningSchema ps = getPartitioningSchema();
            SCIDB_ASSERT(arrDist->getPartitioningSchema() == ps);

            SCIDB_ASSERT(ps != psLocalInstance ||
                         getInstanceId(query) == InstanceID(atol(arrDist->getContext().c_str())));

            DimensionVector offset = getOffsetVector(inputSchemas);
            Coordinates schemaOffset;
            InstanceID  schemaShift;
            ArrayDistributionFactory::getTranslationInfo(_schema.getDistribution().get(),
                                                         schemaOffset,
                                                         schemaShift);
            SCIDB_ASSERT(offset == DimensionVector(schemaOffset));
        }
        RedistributeContext distro(_schema.getDistribution(),
                                   _schema.getResidency());
        LOG4CXX_TRACE(logger, "sg() output distro: "<< distro);
        return distro;
}

// getOutputBoundaries not overridden

// requiresRedimensionOrRepartition not overridden

// preSingleExecute not overridden

// execute
std::shared_ptr<Array>
PhysicalSG::execute(vector< std::shared_ptr<Array> >& inputArrays,
                    std::shared_ptr<Query> query)
{
        ArrayDistPtr arrDist = _schema.getDistribution();
        SCIDB_ASSERT(arrDist);

        std::shared_ptr<Array> srcArray = inputArrays[0];

        bool enforceDataIntegrity=false;
        if (_parameters.size() > SGPARAM_IS_STRICT) { // isStrict param present
            assert(_parameters[SGPARAM_IS_STRICT]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_IS_STRICT].get());
            enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_STRICT << " isStrict present: " << enforceDataIntegrity);
        } else {
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_STRICT << " isStrict not present!");
        }

        bool isPullSG=false;
        if (_parameters.size() > SGPARAM_IS_PULL) { // isPull param present
            LOG4CXX_TRACE(logger, "PhysicalSG::execute(): isPullSG parameter present");
            assert(_parameters[SGPARAM_IS_PULL]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_IS_PULL].get());
            isPullSG = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_PULL << " isPullSG present: " << isPullSG);
        } else {
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_PULL << " isPullSG not present!");
        }
        LOG4CXX_DEBUG(logger, "PhysicalSG::execute(): isPullSG: " << isPullSG);

        {   // to temporarily preserve existing indentation
            // despite elimination of storing-sg functionality

            std::shared_ptr<Array> outputArray;

            if (isPullSG) {  // to use pullRedistribute instead of the usual redistributeToRandomAccess
                LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1a. !storeResult, isPullSG, dist: "<<
                                      RedistributeContext(arrDist, query->getDefaultArrayResidency()));

                const ArrayDesc& inputArrayDesc = srcArray->getArrayDesc();
                const bool isOpaque = true; // disable the inner stuff that cares about EmptyIndicator's attr order
                {
                    // not sure if this way of converting attributes to an ordering is required
                    // when save (which is row-oriented) is not involved 
                    const Attributes& attribs = inputArrayDesc.getAttributes();
                    std::set<AttributeID> attributeOrdering;
                    for  ( Attributes::const_iterator a = attribs.begin(); a != attribs.end(); ++a) {
                        if (!a->isEmptyIndicator() || isOpaque) {
                            SCIDB_ASSERT(inputArrayDesc.getEmptyBitmapAttribute()==NULL ||
                                         isOpaque ||
                                         // if emptyable, make sure the attribute is not the last
                                         // the last must be the empty bitmap
                                         (a->getId()+1) != attribs.size());
                            attributeOrdering.insert(a->getId());
                        }
                    }

                    // arguments here taken from the else case below, note localDist (original) not needed here
                    LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1B, !store, isPullSG, "
                                       << " pullRedistributeInAttributeOrder, enforce=" << enforceDataIntegrity);
                    outputArray  = pullRedistributeInAttributeOrder(srcArray,
                                                                    attributeOrdering,
                                                                    arrDist,
                                                                    _schema.getResidency(), //default query residency
                                                                    query,
                                                                    getShared(),
                                                                    enforceDataIntegrity);
                }
                // TOOD: does the user of outputArray need to do an outputArray->sync() ?  who does it?

            } else {  // !isPullSG
                LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1B. !store, !pullSG pre redistributeToRandomAccess into distro: "<<
                                      RedistributeContext(arrDist, query->getDefaultArrayResidency()));

                outputArray = redistributeToRandomAccess(srcArray,
                                           arrDist,
                                           _schema.getResidency(),
                                           query,
                                           getShared(),
                                           enforceDataIntegrity);

            }
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1B, !storeResult executed SG dist: "
                                  << RedistributeContext(outputArray->getArrayDesc().getDistribution(),
                                                         outputArray->getArrayDesc().getResidency())
                                  << " enforce= " << enforceDataIntegrity);

            SCIDB_ASSERT(_schema.getResidency()->isEqual(outputArray->getArrayDesc().getResidency()));
            SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(outputArray->getArrayDesc().getDistribution()));
            inputArrays[0].reset(); // hopefully, drop the input array
            return outputArray;
        }
}
//
// end execute
//

InstanceID PhysicalSG::getInstanceId(const std::shared_ptr<Query>& query) const
{
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() > SGPARAM_INSTANCE_ID ) {
            OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_INSTANCE_ID].get());
            instanceId = static_cast<InstanceID>(pExp->getExpression()->evaluate().getInt64());
        }
        if (instanceId == COORDINATOR_INSTANCE_MASK) {
            instanceId = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        }
        return instanceId;
}

DimensionVector PhysicalSG::getOffsetVector(const vector<ArrayDesc> & inputSchemas) const
{
        if (_parameters.size() <= SGPARAM_OFFSET_START) { // parameter is not present
            return DimensionVector();
        } else {
            DimensionVector result(_schema.getDimensions().size());
            assert (_parameters.size() == _schema.getDimensions().size() + SGPARAM_OFFSET_START);
            for (size_t i = 0; i < result.numDimensions(); i++)
            {
                result[i] = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[SGPARAM_OFFSET_START+i])->getExpression()->evaluate().getInt64();
            }
            return result;
        }
}


DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSG, "_sg", "impl_sg")

} //namespace
