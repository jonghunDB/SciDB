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
 * @file HabilisOptimizer.cpp
 *
 * @brief Our first attempt at a halfway intelligent optimizer.
 * habilis (adj.) Latin: fit, easy, adaptable, apt, handy, well-adapted, inventive,..
 *
 * @author poliocough@gmail.com
 */

#include "query/optimizer/HabilisOptimizer.h"

#include <query/ops/sg/SGParams.h>
#include <query/ParsingContext.h>
#include <array/DelegateArray.h>
#include <query/Query.h>
#include <query/QueryPlan.h>
#include <query/QueryPlanUtilites.h>
#include <array/ArrayDistribution.h>
#include <array/Metadata.h>

#include <log4cxx/logger.h>

#include <fstream>
#include <memory>

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.optimizer"));

namespace {
// Adds semantics to code in this file, to avoid calling some_func(..., false, ...).
static constexpr bool TILE_MODE = true;
static constexpr bool CHILDREN = true;
static constexpr bool CONSTANT = true;
}

HabilisOptimizer::HabilisOptimizer()
:
    _root(),
    _featureMask(CONDENSE_SG
                 | INSERT_REDIM_OR_REPART
                 | INSERT_MATERIALIZATION
                )
{
    const char* path = "/tmp/scidb_optimizer_override";
    std::ifstream inFile (path);
    if (inFile && !inFile.eof())
    {
        inFile >> _featureMask;
        LOG4CXX_TRACE(logger, "Feature mask overridden to "<<_featureMask);
    }
    inFile.close();
}


PhysPlanPtr HabilisOptimizer::optimize(const std::shared_ptr<Query>& query,
                                       std::shared_ptr<LogicalPlan>& logicalPlan)
{
    assert(_root.get() == NULL);
    assert(_query.get() == NULL);

    Eraser onStack(*this);

    _query = query;
    assert(_query);

    _defaultArrRes = query->getDefaultArrayResidency();
    _defaultArrDist = defaultPartitioning();

    std::shared_ptr<LogicalQueryPlanNode> logicalRoot = logicalPlan->getRoot();
    if (!logicalRoot)
    {   return PhysPlanPtr(new PhysicalPlan(_root)); }

    bool tileMode = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE) > 1;
    _root = tw_createPhysicalTree(logicalRoot, tileMode);

    if (!logicalPlan->getRoot()->isDdl())
    {
        if (isFeatureEnabled(INSERT_REDIM_OR_REPART))
        {
            tw_insertRedimensionOrRepartitionNodes(_root);
        }

        tw_insertSgNodes(_root);

        if (isFeatureEnabled(CONDENSE_SG))
        {
            LOG4CXX_TRACE(logger, "CONDENSE_SG: begin");

            tw_collapseSgNodes(_root);

            while (tw_pushupJoinSgs(_root))
            {
                tw_collapseSgNodes(_root);
            }

            LOG4CXX_TRACE(logger, "CONDENSE_SG: end");
        }

        if (isFeatureEnabled(INSERT_MATERIALIZATION))
        {
            tw_insertChunkMaterializers(_root);
        }

        tw_updateSgStrictness(_root);
    }

    PhysPlanPtr result(new PhysicalPlan(_root));
    // null out the root
    logicalPlan->setRoot(std::shared_ptr<LogicalQueryPlanNode>());

    return result;
}

void HabilisOptimizer::printPlan(PhysNodePtr node, bool children)
{
    if (!node) {
        node = _root;
    }
    scidb::printPlan(node, 0, children);
}

void HabilisOptimizer::logPlanDebug(PhysNodePtr node, bool children)
{
    if (!node) {
        node = _root;
    }
    scidb::logPlanDebug(logger, node, 0, children);
}

void HabilisOptimizer::logPlanTrace(PhysNodePtr node, bool children)
{
    if (!node) {
        node = _root;
    }
    scidb::logPlanTrace(logger, node, 0, children);
}

void HabilisOptimizer::n_addParentNode(PhysNodePtr target, PhysNodePtr nodeToInsert)
{
    LOG4CXX_TRACE(logger, "[n_addParentNode] begin");
    LOG4CXX_TRACE(logger, "[n_addParentNode] node to insert:");
    logPlanTrace(nodeToInsert, false);
    LOG4CXX_TRACE(logger, "[n_addParentNode] target tree:");
    logPlanTrace(target);

    if (target->hasParent())
    {
        PhysNodePtr parent = target->getParent();
        parent->replaceChild(target, nodeToInsert);
    }
    else
    {
        assert(_root == target);
        _root = nodeToInsert;
        _root->resetParent();   // paranoid
    }

    nodeToInsert->addChild(target);

    LOG4CXX_TRACE(logger, "[n_addParentNode] done");
    logPlanTrace();
    LOG4CXX_TRACE(logger, "[n_addParentNode] end");
}

void HabilisOptimizer::n_cutOutNode(PhysNodePtr nodeToRemove)
{
    LOG4CXX_TRACE(logger, "[n_cutOutNode] begin");
    logPlanTrace(nodeToRemove, false);
    vector<PhysNodePtr> children = nodeToRemove->getChildren();
    assert(children.size()<=1);

    if (nodeToRemove->hasParent())
    {
        PhysNodePtr parent = nodeToRemove->getParent();
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            parent->replaceChild(nodeToRemove, child);
        }
        else
        {
            parent->removeChild(nodeToRemove);
        }
    }

    else
    {
        assert(_root == nodeToRemove);
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            _root = child;
            _root->resetParent();
        }
        else
        {
            _root.reset();
        }
    }
    LOG4CXX_TRACE(logger, "[n_cutOutNode] done");
    logPlanTrace();
    LOG4CXX_TRACE(logger, "[n_cutOutNode] end");
}

std::shared_ptr<OperatorParam> HabilisOptimizer::n_createPhysicalParameter(const std::shared_ptr<OperatorParam> & logicalParameter,
                                                                      const vector<ArrayDesc>& logicalInputSchemas,
                                                                      const ArrayDesc& logicalOutputSchema,
                                                                      bool tile)
{
    if (logicalParameter->getParamType() == PARAM_LOGICAL_EXPRESSION)
    {
        std::shared_ptr<Expression> physicalExpression = std::make_shared<Expression> ();
        std::shared_ptr<OperatorParamLogicalExpression>& logicalExpression = (std::shared_ptr<OperatorParamLogicalExpression>&) logicalParameter;
        try
        {
            if (logicalExpression->isConstant())
            {
               physicalExpression->compile(logicalExpression->getExpression(), tile, logicalExpression->getExpectedType().typeId());
            }
            else
            {
               physicalExpression->compile(logicalExpression->getExpression(), tile, logicalExpression->getExpectedType().typeId(), logicalInputSchemas,
                                            logicalOutputSchema);
            }
            if (tile && !physicalExpression->supportsTileMode()) {
                return std::shared_ptr<OperatorParam>();
            }
            return std::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(logicalParameter->getParsingContext(), physicalExpression,
                                                                                  logicalExpression->isConstant()));
        } catch (Exception &e)
        {
            if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR2)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_PARAMETER_TYPE_ERROR, logicalExpression->getParsingContext())
                    << logicalExpression->getExpectedType().name() << TypeLibrary::getType(physicalExpression->getType()).name();
            }
            else
            {
                throw;
            }
        }
    }
    else
    {
        return logicalParameter;
    }
}

PhysNodePtr HabilisOptimizer::n_createPhysicalNode(std::shared_ptr<LogicalQueryPlanNode> logicalNode,
                                                   bool tileMode)
{
    const std::shared_ptr<LogicalOperator>& logicalOp = logicalNode->getLogicalOperator();
    const string& logicalName = logicalOp->getLogicalName();

    OperatorLibrary* opLibrary = OperatorLibrary::getInstance();
    vector<string> physicalOperatorsNames;
    opLibrary->getPhysicalNames(logicalName, physicalOperatorsNames);
    const string &physicalName = physicalOperatorsNames[0];
    const vector<std::shared_ptr<LogicalQueryPlanNode> >& children = logicalNode->getChildren();

    // Collection of input schemas of operator for resolving references
    vector<ArrayDesc> inputSchemas;
    tileMode &= logicalOp->getProperties().tile;
    for (size_t ch = 0; ch < children.size(); ch++)
    {
        inputSchemas.push_back(children[ch]->getLogicalOperator()->getSchema());
    }
    const ArrayDesc& outputSchema = logicalOp->getSchema();

    const Parameters& logicalParams = logicalOp->getParameters();
    size_t nParams = logicalParams.size();
    Parameters physicalParams(nParams);

  Retry:
    for (size_t i = 0; i < nParams; i++)
    {
        bool paramTileMode = tileMode && logicalOp->compileParamInTileMode(i);
        std::shared_ptr<OperatorParam> param =
            n_createPhysicalParameter(logicalParams[i], inputSchemas, outputSchema, paramTileMode);

        if (!param) {
            assert(paramTileMode);
            tileMode = false;
            goto Retry;
        }
        physicalParams[i] = param;
    }

    KeywordParameters kwPhysParams;
  KwRetry:
    for (auto& kwPair : logicalOp->getKeywordParameters())
    {
        bool paramTileMode = tileMode && logicalOp->compileParamInTileMode(kwPair.first);
        Parameter param =
            n_createPhysicalParameter(kwPair.second, inputSchemas, outputSchema, paramTileMode);

        if (!param) {
            assert(paramTileMode);
            tileMode = false;
            kwPhysParams.clear();
            goto KwRetry;
        }
        kwPhysParams.insert(make_pair(kwPair.first, param));
    }

    PhysOpPtr physicalOp = opLibrary->createPhysicalOperator(logicalName, physicalName, physicalParams, outputSchema);
    physicalOp->setQuery(_query);
    physicalOp->setKeywordParameters(kwPhysParams);
    physicalOp->setTileMode(tileMode);
    physicalOp->inspectLogicalOp(*logicalOp);

    // where [all?] PhysicalQueryPlanNodes are created
    return std::make_shared<PhysicalQueryPlanNode>(physicalOp, logicalNode->isDdl(), tileMode);
}

// TODO: eliminate storeArrayName from SG parameter list
PhysNodePtr HabilisOptimizer::n_buildSgNode(const ArrayDesc & outputSchema,
                                            RedistributeContext const& dist,
                                            const std::shared_ptr<OperatorParam>& storeArrayName)
{
    if (dist.isUndefined()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER,
                               SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);
    }

    ArrayDistPtr arrDist = dist.getArrayDistribution();
    ArrayResPtr   arrRes = dist.getArrayResidency();

    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(arrDist);

    Parameters sgParams;

    std::shared_ptr<Expression> psConst = std::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));

    ps.setInt32(arrDist->getPartitioningSchema());

    psConst->compileConstant(false, TID_INT32, ps);
    std::shared_ptr<OperatorParam> psParam(new OperatorParamPhysicalExpression(
                                            std::make_shared<ParsingContext>(),
                                            psConst,
                                            CONSTANT));
    sgParams.push_back(psParam);

    // this is the "storing" sg (that we no longer support)
    // TODO: remove the ability to even specify it as a parameter
    SCIDB_ASSERT(!storeArrayName);

    ArrayDesc outputDesc(outputSchema);
    outputDesc.setDistribution(arrDist);
    outputDesc.setResidency(arrRes);

    LOG4CXX_TRACE(logger, "Building SG node, output schema = "<<outputDesc);

    PhysOpPtr sgOp = OperatorLibrary::getInstance()->createPhysicalOperator("_sg", "impl_sg", sgParams, outputDesc);
    sgOp->setQuery(_query);

    PhysNodePtr sgNode(new PhysicalQueryPlanNode(sgOp, false, false));
    return sgNode;
}

PhysNodePtr HabilisOptimizer::n_buildReducerNode(PhysNodePtr child,
                                                 const ArrayDistPtr& arrDist)
{
    SCIDB_ASSERT(arrDist);

    //insert a distro reducer node. In this branch sgNeeded is always false.
    Parameters reducerParams;
    std::shared_ptr<Expression> psConst = std::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));
    ps.setInt32(arrDist->getPartitioningSchema());
    psConst->compileConstant(false, TID_INT32, ps);
    std::shared_ptr<OperatorParam> reducer(new OperatorParamPhysicalExpression
                                           (std::make_shared<ParsingContext>(), psConst, CONSTANT));
    reducerParams.push_back(reducer);

    // reducer always changes the distribution
    ArrayDesc outputDesc(child->getPhysicalOperator()->getSchema());
    outputDesc.setDistribution(arrDist);

    PhysOpPtr reducerOp = OperatorLibrary::getInstance()->createPhysicalOperator("_reduce_distro",
                                                                                 "physicalReduceDistro",
                                                                                 reducerParams,
                                                                                 outputDesc);
    reducerOp->setQuery(_query);
    bool useTileMode = child->getPhysicalOperator()->getTileMode();
    PhysNodePtr reducerNode(new PhysicalQueryPlanNode(reducerOp, false, useTileMode));
    reducerNode->getPhysicalOperator()->setTileMode(useTileMode );
    return reducerNode;
}

// TODO: s_setSGDistribution() once had the default
//       isStrict = true
//       That was converted to an explicit true,
//       and the isPull argument added, which now
//       has an explicit fixed argument in many cases
//       The code is passing all tests at present
//       but only a limited number of operators support
//       isPull
//       After the initial checkin of this code
//       but before closing ticket SDB-5720
//       all calls to this method with fixed arguments
//       should be reviewed for whether those arguments
//       will be sufficinet as more operators support
//       reading from an isPull SG.
static void s_setSgDistribution(PhysNodePtr sgNode,
                                RedistributeContext const& dist,
                                bool isStrict, bool isPull)
{
    if (dist.isUndefined()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER,
                               SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);
    }
    LOG4CXX_TRACE(logger, "s_setSgDistribution: isStrict: " << isStrict << " isPull: " << isPull);

    Parameters parameters = sgNode->getPhysicalOperator()->getParameters();
    Parameters newParameters;

    std::shared_ptr<Expression> psConst = std::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));
    ps.setInt32(dist.getPartitioningSchema());
    psConst->compileConstant(false, TID_INT32, ps);
    std::shared_ptr<OperatorParam> psParam(new OperatorParamPhysicalExpression(
                                            std::make_shared<ParsingContext>(),
                                            psConst,
                                            CONSTANT));
    newParameters.push_back(psParam);
    LOG4CXX_TRACE(logger, "s_setSGDistribution: Added newParameters[0] to SG node, ps="<<ps.get<int32_t>());
    SCIDB_ASSERT(newParameters.size() == SGPARAM_DISTRIBUTION+1);

    if( dist.getPartitioningSchema() == psLocalInstance)
    {   //add instance number for local node distribution

        const LocalArrayDistribution* localDist =
           safe_dynamic_cast<const LocalArrayDistribution*>(dist.getArrayDistribution().get());

        Value instanceId(TypeLibrary::getType(TID_INT64));
        instanceId.setInt64(localDist->getLogicalInstanceId());

        std::shared_ptr<Expression> instanceIdExpr = std::make_shared<Expression> ();
        instanceIdExpr->compileConstant(false, TID_INT64, instanceId);
        std::shared_ptr<OperatorParam> instParam(new OperatorParamPhysicalExpression(
                                                  std::make_shared<ParsingContext>(),
                                                  instanceIdExpr,
                                                  CONSTANT));
        newParameters.push_back(instParam);
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Added newParameters[1] to SG node, instanceId="<<instanceId.get<int64_t>());
        SCIDB_ASSERT(newParameters.size() == SGPARAM_INSTANCE_ID+1);
    }

    // transfer the rest of the existing params
    for (size_t i = newParameters.size(); i< parameters.size() && i< SGPARAM_OFFSET_START; i++) {
        newParameters.push_back(parameters[i]);
        LOG4CXX_TRACE(logger, "s_setSGDistribution: Copying existing param to SG node, from oldParam["<<i<<"]");
    }

    // JHM ?copy SG operator's distribution and residency onto the node?
    ArrayDesc const& opSchema = sgNode->getPhysicalOperator()->getSchema();
    if (!opSchema.getDistribution()->isCompatibleWith(dist.getArrayDistribution()) ||
        !opSchema.getResidency()->isEqual(dist.getArrayResidency())) {

        ArrayDesc sgSchema(opSchema);

        SCIDB_ASSERT(dist.getArrayDistribution());
        sgSchema.setDistribution(dist.getArrayDistribution());

        SCIDB_ASSERT(dist.getArrayResidency());
        sgSchema.setResidency(dist.getArrayResidency());

        sgNode->getPhysicalOperator()->setSchema(sgSchema);
    }

    if (newParameters.size() <= SGPARAM_INSTANCE_ID)
    {   //if we don't already have an instance parameter - add a fake instance
        std::shared_ptr<Expression> instanceConst(new Expression());;
        Value instance(TypeLibrary::getType(TID_INT64));
        instance.setInt64(-1);
        instanceConst->compileConstant(false, TID_INT64, instance);
        std::shared_ptr<OperatorParam> instParam(new OperatorParamPhysicalExpression(
                                                  std::make_shared<ParsingContext>(),
                                                  instanceConst,
                                                  CONSTANT));
        newParameters.push_back(instParam);
        LOG4CXX_TRACE(logger, "s_setSGDistribution: Added newParameters[1] to SG node, instanceId="<<instance.get<int64_t>());
        SCIDB_ASSERT(newParameters.size() == SGPARAM_INSTANCE_ID+1);
    }
    SCIDB_ASSERT(newParameters.size() > SGPARAM_INSTANCE_ID);

    // if not already there, add the isStrict flag
    std::shared_ptr<Expression> strictFlagExpr(new Expression());
    Value strictFlag(TypeLibrary::getType(TID_BOOL));
    strictFlag.setBool(isStrict);
    strictFlagExpr->compileConstant(false, TID_BOOL, strictFlag);
    std::shared_ptr<OperatorParam> strictParam(make_shared<OperatorParamPhysicalExpression>(std::make_shared<ParsingContext>(),
                                                                                            strictFlagExpr,
                                                                                            CONSTANT));

    // TODO: cleaner to just resize() and then set IS_STRICT and IS_PULL between here and line 559?
    //       or to factor the if() { push_back() } else { = } to a helper
    if (newParameters.size() <= SGPARAM_IS_STRICT ) {
        SCIDB_ASSERT(newParameters.size() == SGPARAM_IS_STRICT);
        newParameters.push_back(strictParam);
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Added newParameters[" << SGPARAM_IS_STRICT<<"] to SG node, isStrict="<<isStrict);
    } else {
        newParameters[SGPARAM_IS_STRICT] = strictParam;
    }
    SCIDB_ASSERT(newParameters.size() > SGPARAM_IS_STRICT);
    LOG4CXX_TRACE(logger, "Adding new param to SG node, [" << SGPARAM_IS_STRICT<<"] isStrict="<<isStrict);

    // if not already there, add the isPull flag - mimicking the isStrict code above
    // TODO: decide if this should never be passed in
    //       (like the offsets below which are not)
    std::shared_ptr<Expression> pullFlagExpr(new Expression());
    Value pullFlag(TypeLibrary::getType(TID_BOOL));
    pullFlag.setBool(isPull);
    pullFlagExpr->compileConstant(false, TID_BOOL, pullFlag);
    std::shared_ptr<OperatorParam> pullParam(make_shared<OperatorParamPhysicalExpression>(std::make_shared<ParsingContext>(),
                                                                                            pullFlagExpr,
                                                                                            CONSTANT));
    if (newParameters.size() <= SGPARAM_IS_PULL) {
        SCIDB_ASSERT(newParameters.size() == SGPARAM_IS_PULL);
        newParameters.push_back(pullParam);
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Added newParameters[" << SGPARAM_IS_PULL<<"] to SG node, isPull="<<isPull);
    } else {
        newParameters[SGPARAM_IS_PULL] = pullParam;
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Set newParameters[" << SGPARAM_IS_PULL<<"] on SG node, isPull="<<isPull);
    }
    SCIDB_ASSERT(newParameters.size() > SGPARAM_IS_PULL);
    LOG4CXX_TRACE(logger, "Adding new param to SG node, [" << SGPARAM_IS_PULL <<"] isPull="<<isPull);

    // add offset coordinates (JHM ?cannot already be there?)
    Coordinates offset;
    InstanceID instanceShift;
    ArrayDistributionFactory::getTranslationInfo(dist.getArrayDistribution().get(),
                                                 offset,
                                                 instanceShift);
    SCIDB_ASSERT(instanceShift == 0);

    for ( size_t i = 0;  i < offset.size(); i++)
    {
        std::shared_ptr<Expression> vectorValueExpr(new Expression());
        Value vectorValue(TypeLibrary::getType(TID_INT64));
        vectorValue.setInt64(offset[i]);
        vectorValueExpr->compileConstant(false, TID_INT64, vectorValue);
        std::shared_ptr<OperatorParam> vecValParam(new OperatorParamPhysicalExpression
                                                   (std::make_shared<ParsingContext>(),
                                                    vectorValueExpr, CONSTANT));
        newParameters.push_back(vecValParam);
        LOG4CXX_TRACE(logger, "s_setSGDistribution: Added new param to SG node, <offset vector> [" <<i<< "]");
    }

    LOG4CXX_TRACE(logger, "s_setSGDistribution: sgNode->setParamters(), size = "<<newParameters.size());

    sgNode->getPhysicalOperator()->setParameters(newParameters);
}

PhysNodePtr HabilisOptimizer::tw_createPhysicalTree(std::shared_ptr<LogicalQueryPlanNode> logicalRoot, bool tileMode)
{
   logicalRoot = logicalRewriteIfNeeded(_query, logicalRoot);

    vector<std::shared_ptr<LogicalQueryPlanNode> > logicalChildren = logicalRoot->getChildren();
    vector<PhysNodePtr> physicalChildren(logicalChildren.size());
    bool rootTileMode = tileMode;
    for (size_t i = 0; i < logicalChildren.size(); i++)
    {
        std::shared_ptr<LogicalQueryPlanNode> logicalChild = logicalChildren[i];
        PhysNodePtr physicalChild = tw_createPhysicalTree(logicalChild, tileMode);
        rootTileMode &= physicalChild->getPhysicalOperator()->getTileMode();
        physicalChildren[i] = physicalChild;
    }
    PhysNodePtr physicalRoot = n_createPhysicalNode(logicalRoot, rootTileMode);

    if (physicalRoot->isSgNode())
    {
        //this is a user-inserted explicit SG. So we don't mess with it.
        physicalRoot->setSgMovable(false);
        physicalRoot->setSgOffsetable(false);
    }
    for (size_t i = 0; i < physicalChildren.size(); i++)
    {
        PhysNodePtr physicalChild = physicalChildren[i];
        physicalRoot->addChild(physicalChild);
    }
    std::shared_ptr<LogicalOperator> logicalOp = logicalRoot->getLogicalOperator();

    physicalRoot->inferBoundaries();
    return physicalRoot;
}

static PhysNodePtr s_findThinPoint(PhysNodePtr root)
{
    double dataWidth = root->getDataWidth();
    PhysNodePtr candidate = root;

    while (root->isSgNode() == false &&
           root->needsSpecificDistribution() == false &&
           root->changesDistribution() == false &&
           root->outputFullChunks() &&
           root->getChildren().size() == 1 )
    {
        root = root->getChildren()[0];
        if (root->getDataWidth() < dataWidth)
        {
            dataWidth = root->getDataWidth();
            candidate = root;
        }
    }
    return candidate;
}

static RedistributeContext s_propagateDistribution(PhysNodePtr node,
                                                 PhysNodePtr end)
{
    SCIDB_ASSERT(node);
    SCIDB_ASSERT(end);
    LOG4CXX_TRACE(logger, "[s_propagateDistribution] begin");
    logPlanTrace(logger, node, 0 , false);
    LOG4CXX_TRACE(logger, "[s_propagateDistribution] propogation: begin");
    RedistributeContext dist;
    do
    {
        dist = node->inferDistribution();
        if (node == end) {
            break;
        } else {
            node = node->getParent();
        }
    } while (node->getChildren().size() <= 1);

    LOG4CXX_TRACE(logger, "[s_propagateDistribution] propogation: end");
    logPlanTrace(logger, node, 0 , false);
    LOG4CXX_TRACE(logger, "[s_propagateDistribution] end");

    return dist;
}

PhysNodePtr
HabilisOptimizer::insertReducerAndSgNode(PhysNodePtr root,
                                         PhysNodePtr child,
                                         RedistributeContext const& dist)

{
    SCIDB_ASSERT(child->getDistribution().getPartitioningSchema() == psReplication);

    ArrayDistPtr arrDist;
    if (dist.getPartitioningSchema() == psReplication) {
        //XXX TODO: In DEGRADED mode scan() would return psUndefined instead of psReplication.
        //XXX TODO: So, this logic would not catch it. Currently, scan() just errors out if it is forced
        //XXX TODO: to return psUndefined instead of psReplication. See PhysicalScan::getOutputDistribution()
        SCIDB_ASSERT(!child->getDistribution().getArrayResidency()->isEqual(dist.getArrayResidency()));
        arrDist = _defaultArrDist;
    } else {
        arrDist = dist.getArrayDistribution();
    }

    if (arrDist->getPartitioningSchema() == psUndefined) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER,
                               SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);
    }

    // child has psReplicated distribution, have to insert a reducer
    // operator that will filter out chunks that do not belong to the target distribution.
    PhysNodePtr sgNode;
    PhysNodePtr reducerNode = n_buildReducerNode(child, arrDist);

    n_addParentNode(child, reducerNode);

    reducerNode->inferBoundaries();

    if (!child->getDistribution().getArrayResidency()->isEqual(dist.getArrayResidency())) {
        sgNode = n_buildSgNode(reducerNode->getPhysicalOperator()->getSchema(),
                               dist);
        sgNode->setSgMovable(false);
        sgNode->setSgOffsetable(false);
        n_addParentNode(reducerNode, sgNode);
        sgNode->inferBoundaries();
    }
    s_propagateDistribution(reducerNode, root);

    return (sgNode ? sgNode : reducerNode);
}

void HabilisOptimizer::tw_insertSgNodes(PhysNodePtr root)
{
    // root is parent of some children, not necessarily root of tree
    LOG4CXX_TRACE(logger, "[tw_insertSgNodes] enter, root PhysicalName:" << root->getPhysicalOperator()->getPhysicalName());
    assert(_root.get() != NULL);

    // a post-order traversal
    for (size_t i = 0; i < root->getChildren().size(); i ++)
    {
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes] recursing to " << root->getPhysicalOperator()->getPhysicalName() << "["<<i<<"]");
        tw_insertSgNodes(root->getChildren()[i]);
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes] returning from " << root->getPhysicalOperator()->getPhysicalName() << "["<<i<<"]");
    }

    RedistributeContext defaultDistro(_defaultArrDist, _defaultArrRes);

    if (root->isSgNode() == false)
    {
        if (root->getChildren().size() == 1)
        {
            PhysNodePtr child = root->getChildren()[0];
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes] one-input candidate root PhysicalName:" << root->getPhysicalOperator()->getPhysicalName());
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes] its only child PhysicalName: " << child->getPhysicalOperator()->getPhysicalName());
            RedistributeContext cDist = child->getDistribution();
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes] cDist->getPartitioningSchema(): " << cDist.getPartitioningSchema());
            PhysNodePtr sgCandidate = child;

            bool sgNeeded = false;
            RedistributeContext newDist;
            bool sgMovable = true, sgOffsetable = true;

            if (child -> outputFullChunks() == false || cDist.getPartitioningSchema() == psLocalInstance)
            {
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes]    outputFullChunks() " << child->outputFullChunks() << " or psLocalInstance");

                if(root->needsSpecificDistribution())
                {
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       root needs specific distribution");
                    RedistributeContext reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];
                    if (reqDistro.isViolated())
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED)
                           << "requiring violated distributions";
                    }
                    if (reqDistro == cDist && child->outputFullChunks())
                    {
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       distrib matches and is full chunks, no SG needed");
                        //op1 returns data on local node and op2 REQUIRES all data on local node
                    }
                    else
                    {
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       adding sg to requested distrib");
                        sgNeeded = true;
                        newDist = reqDistro;
                        sgOffsetable = false;
                    }
                }
                else if (child -> isSgNode() && child->outputFullChunks())
                {
                    //user inserted sg to local node because they felt like it
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       child is already an SG node");
                }
                else if (!root->requiresInputSG() &&  // case where SG not required, and is inefficient
                          cDist.getPartitioningSchema() == psLocalInstance &&
                          child->estimatedMaxDataVolume() < EFFICIENT_SG_MIN_DATA_BYTES)
                {
                    // redistribution would take longer than just doing it locally
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       no-required SG omitted due to low maxDataVolume: " << child->estimatedMaxDataVolume()
                                                                    << " root->requiresInputSG() "<< root->requiresInputSG());
                }
                else
                {
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       root->requiresInputSG(): " << root->requiresInputSG());
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       cDist.getPartitioningSchema() == psLocalInstance: " << (cDist.getPartitioningSchema() == psLocalInstance));
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       child->estimatedMaxDataVolume(): " << child->estimatedMaxDataVolume());
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       adding SG to defaultDistrib");
                    sgNeeded = true;
                    newDist = defaultDistro;
                }
                sgMovable = false;
            }
            else if( cDist.getPartitioningSchema() == psReplication )
            {
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes]    cDist.getPartitioningSchema() == psReplication case");
                //replication distributions can be reduced instead of sg-ed so they are handled as a special case
                RedistributeContext reqDistro = defaultDistro;

                //does root want a particular distribution? if so - use that
                //if not - force round robin! Otherwise we may get incorrect results
                if( root->needsSpecificDistribution())
                {
                    reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];
                }
                if (reqDistro.isViolated()) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED)
                    << "requiring violated distributions";
                }
                if (reqDistro != cDist)
                {
                    //insert a distro reducer node. In this branch sgNeeded is always false.
                    insertReducerAndSgNode(root, child, reqDistro);
                }
            }
            else if (root->needsSpecificDistribution())
            {
                RedistributeContext reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];

                LOG4CXX_TRACE(logger, "[tw_insertSgNodes] candidate with one input requires specific dist: "
                                      << reqDistro << " child dist: "<< cDist);
                logPlanTrace(root);
                logPlanTrace(child);

                if (reqDistro.isViolated()) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED)
                       << "requiring violated distributions";
                }
                if (reqDistro != cDist)
                {
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes] reqDistro:" << reqDistro << " != child dist: "<< cDist);
                    sgNeeded = true;
                    newDist = reqDistro;
                    sgOffsetable = false;
                    sgCandidate = s_findThinPoint(child);
                } else {
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes] reqDistro:" << reqDistro << " == child dist: "<< cDist);
                }
            }

            if (sgNeeded)
            {
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes] SG needed: adding it");
                logPlanTrace(sgCandidate);
                PhysNodePtr sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(),
                                                   newDist);
                n_addParentNode(sgCandidate,sgNode);

                // is the trailing node something that can pull? (Insert & Store are primary examples)
                bool isPull = sgNode->getParent()->getPhysicalOperator()->acceptsPullSG(0);
                LOG4CXX_TRACE(logger, "[tw_insertSGNodes] thisOp " << (void*)(sgNode->getPhysicalOperator().get())
                                                           << ", " <<         sgNode->getPhysicalOperator()->getLogicalName());
                LOG4CXX_TRACE(logger, "[tw_insertSGNodes] parentOp " << (void*)(sgNode->getParent()->getPhysicalOperator().get())
                                                             << ", " <<         sgNode->getParent()->getPhysicalOperator()->getLogicalName()
                                              << ", acceptsPullSG: " << isPull);
                LOG4CXX_TRACE(logger, "[tw_insertSGNodes] 1 input, sgNeeded, isStrict: hardcoded true, isPull: " << isPull);
                s_setSgDistribution(sgNode, newDist, /*strict*/true, isPull); //XXX somewhat redundant

                sgNode->inferBoundaries();
                sgNode->setSgMovable(sgMovable);
                sgNode->setSgOffsetable(sgOffsetable);
                s_propagateDistribution(sgNode, root);
            }
        }
        else if (root->getChildren().size() == 2)
        {
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes] candidate has two inputs");
            PhysNodePtr lhs = root->getChildren()[0];
            PhysNodePtr rhs = root->getChildren()[1];

            if(!root->requiresInputSG() &&  // case where SG not required, and is inefficient
               lhs->getDistribution().getPartitioningSchema() == psLocalInstance &&
               rhs->getDistribution().getPartitioningSchema() == psLocalInstance &&
               lhs->estimatedMaxDataVolume() < EFFICIENT_SG_MIN_DATA_BYTES &&
               rhs->estimatedMaxDataVolume() < EFFICIENT_SG_MIN_DATA_BYTES ) {

               // redistribution would take longer than just doing it where-it-lies, so just do it where-it-lies
               LOG4CXX_TRACE(logger, "[tw_insertSgNodes]       not-required SGs omitted due to low maxDataVolume: "
                                     << lhs->estimatedMaxDataVolume()
                                     << " and "
                                     << rhs->estimatedMaxDataVolume());
            }
            else if (root->needsSpecificDistribution())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR);
            }
            else
            {
                RedistributeContext lhsDist = lhs->getDistribution();
                if (lhs->outputFullChunks() == false ||
                    lhsDist.getPartitioningSchema() == psLocalInstance)
                {   // example: lhs is an input() [e.g. load()]
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes] candidate has two inputs, left");
                    logPlanTrace(lhs);

                    PhysNodePtr sgNode = n_buildSgNode(lhs->getPhysicalOperator()->getSchema(), defaultDistro);
                    n_addParentNode(lhs, sgNode);
                    sgNode->inferBoundaries();
                    sgNode->setSgMovable(false);
                    lhsDist = s_propagateDistribution(sgNode, root);
                }

                RedistributeContext rhsDist = rhs->getDistribution();
                if (rhs->outputFullChunks() == false ||
                    rhsDist.getPartitioningSchema() == psLocalInstance)
                {   // example: rhs is an input() [e.g. load()]
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes] candidate has two inputs, right");
                    logPlanTrace(rhs);

                    PhysNodePtr sgNode = n_buildSgNode(rhs->getPhysicalOperator()->getSchema(), defaultDistro);
                    n_addParentNode(rhs, sgNode);
                    sgNode->inferBoundaries();
                    sgNode->setSgMovable(false);
                    rhsDist = s_propagateDistribution(sgNode, root);
                }

                //FYI: the logic below can be short-circuited by forcing defaultDistro on both legs

                if(root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated)
                {
                    LOG4CXX_TRACE(logger,
                                  "[tw_insertSgNodes] candidate requests two collocated inputs, lhsDist="
                                  << lhsDist);
                    LOG4CXX_TRACE(logger,
                                  "[tw_insertSgNodes] candidate requests two collocated inputs, rhsDist="
                                  << rhsDist);

                    if (lhsDist != rhsDist ||
                        lhsDist != defaultDistro)
                    {
                        bool canMoveLeftToRight = (rhsDist.isViolated() == false &&
                                                   rhsDist == defaultDistro);
                        bool canMoveRightToLeft = (lhsDist.isViolated() == false &&
                                                   lhsDist == defaultDistro);

                        LOG4CXX_TRACE(logger,
                                      "[tw_insertSgNodes] candidate requests two collocated inputs, canMoveLeft="
                                      << canMoveLeftToRight);
                        LOG4CXX_TRACE(logger,
                                      "[tw_insertSgNodes] candidate requests two collocated inputs, canMoveRigth="
                                      << canMoveRightToLeft);

                        PhysNodePtr leftCandidate  = s_findThinPoint(lhs);
                        PhysNodePtr rightCandidate = s_findThinPoint(rhs);

                        double leftDataWidth = leftCandidate->getDataWidth();
                        double rightDataWidth = rightCandidate->getDataWidth();

                        LOG4CXX_TRACE(logger,
                                      "[tw_insertSgNodes] candidate requests two collocated inputs, leftWidth="
                                      << leftDataWidth);
                        LOG4CXX_TRACE(logger,
                                      "[tw_insertSgNodes] candidate requests two collocated inputs, rigthWidth="
                                      << rightDataWidth);

                        if (leftDataWidth < rightDataWidth && canMoveLeftToRight)
                        {   //move left to right
                            if(lhsDist.getPartitioningSchema() == psReplication)
                            {   //left is replicated - reduce it
                                insertReducerAndSgNode(root, lhs, rhsDist);
                            }
                            else
                            {   //left is not replicated - sg it

                                LOG4CXX_TRACE(logger,
                                              "[tw_insertSgNodes] candidate requests two collocated inputs, left=");
                                logPlanTrace(leftCandidate);

                                PhysNodePtr sgNode = n_buildSgNode(leftCandidate->getPhysicalOperator()->getSchema(),
                                                                   rhsDist);
                                n_addParentNode(leftCandidate, sgNode);
                                sgNode->inferBoundaries();
                                s_propagateDistribution(sgNode, root);
                            }
                        }
                        else if (canMoveRightToLeft)
                        {   //move right to left
                            if(rhsDist.getPartitioningSchema() == psReplication)
                            {   //right is replicated - reduce it
                                insertReducerAndSgNode(root, rhs, lhsDist);
                            }
                            else
                            {   //right is not replicated - sg it

                                LOG4CXX_TRACE(logger,
                                              "[tw_insertSgNodes] candidate requests two collocated inputs, right=");
                                logPlanTrace(rightCandidate);

                                PhysNodePtr sgNode = n_buildSgNode(rightCandidate->getPhysicalOperator()->getSchema(),
                                                                   lhsDist);
                                n_addParentNode(rightCandidate, sgNode);
                                sgNode->inferBoundaries();
                                s_propagateDistribution(sgNode, root);
                            }
                        }
                        else
                        {   //move both left and right to roundRobin
                            if(lhsDist.getPartitioningSchema() == psReplication)
                            {   //left is replicated - reduce it
                                insertReducerAndSgNode(root, lhs, defaultDistro);
                            }
                            else
                            {   //left is not replicated - sg it

                                LOG4CXX_TRACE(logger,
                                              "[tw_insertSgNodes] candidate requests two collocated inputs, left (both)=");
                                logPlanTrace(leftCandidate);

                                PhysNodePtr leftSg = n_buildSgNode(leftCandidate->getPhysicalOperator()->getSchema(),
                                                                   defaultDistro);
                                n_addParentNode(leftCandidate, leftSg);
                                leftSg->inferBoundaries();
                                s_propagateDistribution(leftSg, root);
                            }

                            if(rhsDist.getPartitioningSchema() == psReplication)
                            {   //right is replicated - reduce it
                                insertReducerAndSgNode(root, rhs, defaultDistro);
                            }
                            else
                            {   //right is not replicated - sg it

                                LOG4CXX_TRACE(logger,
                                              "[tw_insertSgNodes] candidate requests two collocated inputs, rigth (both)=");
                                logPlanTrace(rightCandidate);

                                PhysNodePtr rightSg = n_buildSgNode(rightCandidate->getPhysicalOperator()->getSchema(),
                                                                    defaultDistro);
                                n_addParentNode(rightCandidate, rightSg);
                                rightSg->inferBoundaries();
                                s_propagateDistribution(rightSg, root);
                            }
                        }
                    }
                }
            }
        }
        else if (root->getChildren().size() > 2)
        {
            bool needCollocation = false;
            if(root->getDistributionRequirement().getReqType() != DistributionRequirement::Any)
            {
                if (root->getDistributionRequirement().getReqType() != DistributionRequirement::Collocated)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR2);
                }
                needCollocation = true;
            }

            for(size_t i=0; i<root->getChildren().size(); i++)
            {
                PhysNodePtr child = root->getChildren()[i];
                RedistributeContext distro = child->getDistribution();

                if (child->outputFullChunks()==false ||
                    (needCollocation && distro != defaultDistro))
                {   //If needCollocation is true, then we have more than two children who must be collocated. This is a hard problem.
                    //Let's move everyone to roundRobin for now.
                    PhysNodePtr sgCandidate = s_findThinPoint(child);

                    LOG4CXX_TRACE(logger,
                                  "[tw_insertSgNodes] candidate requests multiple collocated inputs, sgCandidate=")
                    logPlanTrace(sgCandidate);

                    PhysNodePtr sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(), defaultDistro);
                    sgNode->setSgMovable(false);
                    sgNode->setSgOffsetable(false);
                    n_addParentNode(sgCandidate, sgNode);
                    sgNode->inferBoundaries();
                    s_propagateDistribution(sgNode, root);
                }
                else if(distro.getPartitioningSchema() == psReplication)
                {   //this child is replicated - reduce it to roundRobin no matter what
                    insertReducerAndSgNode(root, child, defaultDistro);
                }
            }
        }
    }

    root->inferDistribution();
}

static PhysNodePtr s_getChainBottom(PhysNodePtr chainRoot)
{
    PhysNodePtr chainTop = chainRoot;
    while (chainTop->getChildren().size() == 1)
    {
        chainTop = chainTop->getChildren()[0];
    }
    assert(chainTop->isSgNode() == false);
    return chainTop;
}

static PhysNodePtr s_getFirstOffsetableSg(PhysNodePtr chainRoot)
{
    if (chainRoot->isSgNode() && chainRoot->isSgOffsetable())
    {
        return chainRoot;
    }

    if (chainRoot->getChildren().size() != 1 ||
        chainRoot->changesDistribution() ||
        chainRoot->outputFullChunks() == false ||
        chainRoot->needsSpecificDistribution())
    {
        return PhysNodePtr();
    }

    return s_getFirstOffsetableSg(chainRoot->getChildren()[0]);
}



void HabilisOptimizer::cw_rectifyChainDistro(PhysNodePtr root,
                                             PhysNodePtr sgCandidate,
                                             const RedistributeContext & requiredDistribution)
{
    RedistributeContext currentDistribution = root->getDistribution();
    PhysNodePtr chainParent = root->getParent();

    if (requiredDistribution != currentDistribution)
    {
        PhysNodePtr sgNode = s_getFirstOffsetableSg(root);
        if (sgNode.get() == NULL)
        {
            sgNode = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(),
                                   requiredDistribution);
            n_addParentNode(sgCandidate,sgNode);
            sgNode->inferBoundaries();
            if (sgCandidate == root)
            {
                root = sgNode;
            }
        }
        SCIDB_ASSERT(!requiredDistribution.isViolated() ||
                     requiredDistribution.hasMapper());

        LOG4CXX_TRACE(logger, "[cw_recitfyChainDistro], isStrict: hardcoded true, isPull: hardcoded false");
        s_setSgDistribution(sgNode, requiredDistribution, /*strict*/true, /*pull*/false);

        RedistributeContext newRdStats = s_propagateDistribution(sgNode, chainParent);
    }

    assert(root->getDistribution() == requiredDistribution);
}

void HabilisOptimizer::tw_collapseSgNodes(PhysNodePtr root)
{
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] begin");

    bool topChain = (root == _root);

    PhysNodePtr chainBottom = s_getChainBottom(root);
    PhysNodePtr curNode = chainBottom;
    PhysNodePtr sgCandidate = chainBottom;

    RedistributeContext runningDistribution = curNode->getDistribution();
    RedistributeContext chainOutputDistribution = root->getDistribution();

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle: begin");
    do
    {
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle iteration: begin");
        logPlanTrace(root);
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] chainBottom:");
        logPlanTrace(chainBottom, false);
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode:");
        logPlanTrace(curNode, false);
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate:");
        logPlanTrace(sgCandidate, false);

        runningDistribution = curNode->inferDistribution();

        if (curNode->isSgNode() || // sgCandidate cannot stay below an SG because this SG will undo any effects of an SG below
             (curNode->changesDistribution() ||
              curNode->outputFullChunks() == false ||
              curNode->getDataWidth() < sgCandidate->getDataWidth()))
        {
            LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate switched to curNode");
            sgCandidate = curNode;
        }
        if (curNode->hasParent() &&
            curNode->getParent()->getChildren().size() == 1 &&
            curNode->getParent()->needsSpecificDistribution())
        {
            LOG4CXX_TRACE(logger,
                          "[tw_collapseSgNodes] curNode has parent and single child; need specific distribution");

            RedistributeContext neededDistribution = curNode->getParent()->getDistributionRequirement().getSpecificRequirements()[0];
            if (runningDistribution != neededDistribution)
            {
                LOG4CXX_TRACE(logger,
                              "[tw_collapseSgNodes] curNode and required for parent distributions are different");
                if (curNode->isSgNode() )
                {
                    LOG4CXX_TRACE(logger,
                                  "[tw_collapseSgNodes] curNode is SG, update distribution: begin");
                    logPlanTrace(curNode, false);
                    curNode->getPhysicalOperator()->setSchema(curNode->getPhysicalOperator()->getSchema());
                    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes], isStrict: hardcoded true, isPull: hardcoded false");
                    s_setSgDistribution(curNode, neededDistribution, /*strict*/true, /*pull*/false);

                    curNode->setSgMovable(false);
                    curNode->setSgOffsetable(false);
                    runningDistribution = curNode->inferDistribution();
                    LOG4CXX_TRACE(logger,
                                  "[tw_collapseSgNodes] curNode is SG, update distribution: end");
                    logPlanTrace(curNode, false);

                    SCIDB_ASSERT(runningDistribution == neededDistribution);
                }
                else
                {
                    LOG4CXX_TRACE(logger,
                                  "[tw_collapseSgNodes] curNode is not SG, inserting one: begin");
                    PhysNodePtr newSg = n_buildSgNode(sgCandidate->getPhysicalOperator()->getSchema(),
                                                      neededDistribution);

                    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate:");
                    logPlanTrace(sgCandidate, false);

                    n_addParentNode(sgCandidate,newSg);

                    LOG4CXX_DEBUG(logger, "[tw_collapseSgNodes], isStrict: hardcoded true, isPull: hardcoded false");
                    s_setSgDistribution(newSg, neededDistribution, /*strict*/true, /*pull*/false);
                    newSg->inferBoundaries();
                    runningDistribution = s_propagateDistribution(newSg, curNode->getParent());
                    newSg->setSgMovable(false);
                    newSg->setSgOffsetable(false);

                    if (curNode == sgCandidate)
                    {
                        LOG4CXX_TRACE(logger,
                                      "[tw_collapseSgNodes] curNode switched to sgCandidate");
                        curNode = newSg;
                    }
                    LOG4CXX_TRACE(logger,
                                  "[tw_collapseSgNodes] curNod is not SG, inserting one: end");
                }
            }
        }
        else if (curNode->isSgNode() && curNode->isSgMovable())
        {
            LOG4CXX_TRACE(logger,
                          "[tw_collapseSgNodes] curNode is movable SG node, remove it: begin");
            PhysNodePtr newCur = curNode->getChildren()[0];
            n_cutOutNode(curNode);
            if (curNode == sgCandidate)
            {
                LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate switched to newCur");
                logPlanTrace(newCur, false);
                sgCandidate = newCur;
            }
            curNode = newCur;
            runningDistribution = curNode->getDistribution();
            LOG4CXX_TRACE(logger,
                          "[tw_collapseSgNodes] curNode is movable SG node, remove it: end");
        }

        root = curNode;
        curNode = curNode->getParent();

        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle iteration: end");
    } while (curNode.get() != NULL && curNode->getChildren().size()<=1);
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] cycle: end");

    assert(root);

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] chainBottom:");
    logPlanTrace(chainBottom, false);
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] curNode:");
    logPlanTrace(curNode, false);
    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] sgCandidate:");
    logPlanTrace(sgCandidate, false);

    if (!topChain)
    {
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] is not top chain: begin");
        PhysNodePtr parent = root->getParent();
        if (parent->getDistributionRequirement().getReqType() != DistributionRequirement::Any)
        {
            LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] required distribution is not Any");
            //we have a parent instance that has multiple children and needs a specific distribution
            //so we must correct the distribution back to the way it was before we started messing with the chain
            cw_rectifyChainDistro(root, sgCandidate, chainOutputDistribution);
        }
        LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] is not top chain: end");
    }

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] process children chains");
    for (size_t i = 0; i< chainBottom->getChildren().size(); i++)
    {
        tw_collapseSgNodes(chainBottom->getChildren()[i]);
    }

    LOG4CXX_TRACE(logger, "[tw_collapseSgNodes] end");
}

static PhysNodePtr s_getTopSgFromChain(PhysNodePtr chainRoot)
{
    PhysNodePtr chainTop = chainRoot;

    while (chainTop->getChildren().size() == 1)
    {
        if(chainTop->isSgNode())
        {
            return chainTop;
        }
        else if (chainTop->changesDistribution() ||
                 chainTop->outputFullChunks() == false)
        {
            //TODO: this case can be opened up.. but it requires subtraction of offset vectors
            return PhysNodePtr();
        }

        chainTop = chainTop->getChildren()[0];
    }
    return PhysNodePtr();
}

void HabilisOptimizer::cw_pushupSg (PhysNodePtr root, PhysNodePtr sgToRemove, PhysNodePtr sgToOffset)
{
    LOG4CXX_TRACE(logger, "[cw_pushupSg] removing ");
    logPlanTrace(sgToRemove, false);

    PhysNodePtr sgrChild = sgToRemove->getChildren()[0];
    n_cutOutNode(sgToRemove);

    RedistributeContext newSgrDistro = sgrChild->getDistribution();
    for (PhysNodePtr n = sgrChild->getParent(); n != root; n = n->getParent())
    {
        if (n->getParent() == root) {
            newSgrDistro = n->inferDistribution();
        } else {
            n->inferDistribution();
        }
    }
    SCIDB_ASSERT(newSgrDistro.hasMapper());
    LOG4CXX_TRACE(logger, "[cw_pushupSg] newSgrDistro: " << newSgrDistro);

    LOG4CXX_TRACE(logger, "[cw_pushupSg], isStrict: hardcoded true, isPull: hardcoded false");
    s_setSgDistribution(sgToOffset, newSgrDistro, /*strict*/true, /*pull*/false);

    RedistributeContext newSgoDistro = sgToOffset->inferDistribution();
    for (PhysNodePtr n = sgToOffset->getParent(); n != root; n = n->getParent())
    {
        if (isDebug()) {
            newSgoDistro = n->inferDistribution();
        } else {
            n->inferDistribution();
        }
    }
    LOG4CXX_TRACE(logger, "[cw_pushupSg] newSgoDistro: " << newSgoDistro);
    SCIDB_ASSERT(newSgrDistro == newSgoDistro);

    root->inferDistribution();

    PhysNodePtr newSg = n_buildSgNode(root->getPhysicalOperator()->getSchema(),
                                      RedistributeContext(_defaultArrDist, _defaultArrRes));
    newSg->setSgMovable(true);
    newSg->setSgOffsetable(true);
    n_addParentNode(root,newSg);
    newSg->inferDistribution();
    newSg->inferBoundaries();

    LOG4CXX_TRACE(logger, "[cw_pushupSg] newSg ");
    logPlanTrace(newSg, false);
}

void HabilisOptimizer::cw_swapSg (PhysNodePtr root, PhysNodePtr sgToRemove, PhysNodePtr oppositeThinPoint)
{
    PhysNodePtr sgrChild = sgToRemove->getChildren()[0];
    n_cutOutNode(sgToRemove);

    RedistributeContext newSgrDistro = sgrChild->getDistribution();

    for (PhysNodePtr n = sgrChild->getParent(); n != root; n = n->getParent())
    {
        if (n->getParent() == root) {
            newSgrDistro = n->inferDistribution();
        } else {
            n->inferDistribution();
        }
    }

    SCIDB_ASSERT(newSgrDistro.hasMapper());

    PhysNodePtr newOppositeSg = n_buildSgNode(oppositeThinPoint->getPhysicalOperator()->getSchema(), newSgrDistro);
    n_addParentNode(oppositeThinPoint, newOppositeSg);
    s_setSgDistribution(newOppositeSg, newSgrDistro, /*strict*/true, /*pull*/false);
    newOppositeSg->inferBoundaries();
    RedistributeContext newOppositeDistro = newOppositeSg->inferDistribution();
    for (PhysNodePtr n = newOppositeSg->getParent(); n != root; n = n->getParent())
    {
        if (isDebug()) {
            newOppositeDistro = n->inferDistribution();
        } else {
            n->inferDistribution();
        }
    }

    SCIDB_ASSERT(newSgrDistro == newOppositeDistro);
    root->inferDistribution();

    PhysNodePtr newRootSg = n_buildSgNode(root->getPhysicalOperator()->getSchema(),
                                          RedistributeContext(_defaultArrDist, _defaultArrRes));
    newRootSg->setSgMovable(true);
    newRootSg->setSgOffsetable(true);
    n_addParentNode(root,newRootSg);
    newRootSg->inferDistribution();

    logPlanDebug();

    newRootSg->inferBoundaries();

    logPlanDebug();
}

bool HabilisOptimizer::tw_pushupJoinSgs(PhysNodePtr root)
{
    //"pushup" is a transformation from root(...join(sg(A),sg(B))) into root(...sg(join(sg(A),B)))
    //Note this is advantageous if placing sg on top results in less data movement

    //True if top chain SG will be "collapsed" by subsequent collapse()
    bool parentChainWillCollapse = root==_root ||
                                   root->getDistribution().hasMapper();

    //Thinnest available data point in top chain
    double parentChainThinPoint = root->getDataWidth();

    while (root->getChildren().size() == 1)
    {
        double currentThickness = root->getChildren()[0]->getDataWidth();
        if (currentThickness < parentChainThinPoint)
        {
            parentChainThinPoint = currentThickness;
        }

        //If the closest node above the join is an SG, then we can place another
        //SG onto top chain and the two SGs will collapse.

        //Otherwise, if the closest node above join needs correct distribution,
        //new SG will have to stay on top chain and get run

        if (root->isSgNode())
        {
            parentChainWillCollapse = true;
        }
        else if (root->needsSpecificDistribution())
        {
            parentChainWillCollapse = false;
            parentChainThinPoint = currentThickness;
        }

        root = root->getChildren()[0];
    }

    bool transformPerformed = false;

    if (root->getChildren().size() == 2)
    {
        if (root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated &&
            root->getChildren()[0]->getPhysicalOperator()->getSchema().getDimensions().size() ==
            root->getChildren()[1]->getPhysicalOperator()->getSchema().getDimensions().size())
        {
            PhysNodePtr leftChainRoot = root->getChildren()[0];
            PhysNodePtr rightChainRoot = root->getChildren()[1];

            PhysNodePtr leftSg = s_getTopSgFromChain(leftChainRoot);
            PhysNodePtr rightSg = s_getTopSgFromChain(rightChainRoot);

            if (leftSg.get()!=NULL && rightSg.get()!=NULL)
            {
                double leftAttributes = leftSg->getDataWidth();
                double rightAttributes = rightSg->getDataWidth();

                //the cost of not doing anything - run left SG and right SG
                double currentCost = leftAttributes + rightAttributes;

                //the cost of removing either SG
                double moveLeftCost = rightAttributes;
                double moveRightCost = leftAttributes;

                if (parentChainWillCollapse == false)
                {
                    //we will put sg on top and it will not collapse - add to the cost
                    moveLeftCost += parentChainThinPoint;
                    moveRightCost += parentChainThinPoint;
                }

                bool canMoveLeft = leftSg->isSgMovable() &&
                                   leftSg->getChildren()[0]->getDistribution().hasMapper() &&
                                   rightSg->isSgOffsetable();

                bool canMoveRight = rightSg->isSgMovable() &&
                                    rightSg->getChildren()[0]->getDistribution().hasMapper() &&
                                    leftSg->isSgOffsetable();

                if (canMoveLeft && moveLeftCost <= moveRightCost && moveLeftCost <= currentCost)
                {
                    cw_pushupSg(root,leftSg,rightSg);
                    transformPerformed = true;
                }
                else if (canMoveRight && moveRightCost <= currentCost)
                {
                    cw_pushupSg(root,rightSg,leftSg);
                    transformPerformed = true;
                }
            }
            else if ( leftSg.get() != NULL || rightSg.get() != NULL )
            {
                PhysNodePtr sg = leftSg.get() != NULL ? leftSg : rightSg;
                PhysNodePtr oppositeChain = leftSg.get() != NULL ? rightChainRoot : leftChainRoot;
                oppositeChain = s_findThinPoint(oppositeChain);

                bool canMoveSg = sg->isSgMovable() &&
                                 sg->getChildren()[0]->getDistribution().hasMapper();

                double currentCost = sg->getDataWidth();
                double moveCost = oppositeChain->getDataWidth();

                if (parentChainWillCollapse == false)
                {
                    //we will put sg on top and it will not collapse - add to the cost
                    moveCost += parentChainThinPoint;
                }

                if ( canMoveSg && moveCost < currentCost )
                {
                    cw_swapSg(root, sg, oppositeChain);
                    transformPerformed = true;
                }
            }
        }
    }

    bool result = transformPerformed;
    for (size_t i = 0; i< root->getChildren().size(); i++)
    {
        bool transformPerformedAtChild = tw_pushupJoinSgs(root->getChildren()[i]);
        result = transformPerformedAtChild || result;
    }
    return result;
}

void HabilisOptimizer::tw_updateSgStrictness(PhysNodePtr root)
{
    if (root->isSgNode())
    {
        LOG4CXX_TRACE(logger, "[tw_updateSgStrictness] this SG node's Op " << (void*)(root->getPhysicalOperator().get()));

        // NOTE: user-inserted SG will still have isStrict overriden
        // (sg() should not be inserted by hand, so that is ok)
        bool isStrict = true; //default
        PhysNodePtr child = root->getChildren()[0];
        PhysOpPtr inputOp = child->getPhysicalOperator();
        if (inputOp->canToggleStrict())
        {
            isStrict = inputOp->getIsStrict();
        }

        // NOTE: user-inserted SG will still have isPull overriden
        // (sg() should not be inserted by hand, so that is ok)

        bool isPull = false;
        PhysNodePtr parent = root->getParent();
        if (parent) {
            PhysOpPtr outputOp = parent->getPhysicalOperator();
            // TODO: SDB-5720
            // while assuming the input would be input 0 works
            // at the moment (because all operators accepting PullSG
            // are unary ops like Input and Store),
            // SDB-5720 can't be closed until we implement a way to know
            // which input of the output is the one we are feeding.
            // tw_updateSgStrictness is not given that information at
            // this time, but it can be maintained and passed
            // in a follow-on checkin
            isPull = outputOp->acceptsPullSG(0);   // assuming input 0 is not generally correct
                                                   // though it is correct at the moment
        }

        const RedistributeContext distro = root->getDistribution();

        LOG4CXX_DEBUG(logger, "[tw_updateSgStrictness]: distrib ="<<distro.getPartitioningSchema());
        LOG4CXX_DEBUG(logger, "[tw_updateSgStrictness], isStrict: "<<isStrict<<", isPull: " << isPull);
        s_setSgDistribution(root, distro, isStrict, /*pull*/isPull);

        const PhysOpPtr sgOp = root->getPhysicalOperator();
        SCIDB_ASSERT(sgOp->getParameters().size() >= 4);
        SCIDB_ASSERT(sgOp->getParameters()[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
    }
    for (size_t i =0; i<root->getChildren().size(); i++)
    {
        tw_updateSgStrictness(root->getChildren()[i]);
    }
}

bool
HabilisOptimizer::allowEndMaxChange(std::shared_ptr<PhysicalOperator> const & physOperator) const
{
    return physOperator && physOperator->getPhysicalName() == "physicalMerge";
}


bool
HabilisOptimizer::isRedimCollapsible(PhysNodePtr const& parent,
                                     ArrayDesc const& desired,
                                     ArrayDesc const& given,
                                     ArrayDesc& result) const
{
    ASSERT_EXCEPTION(!desired.isAutochunked(),
                     "Unexpected Autochunk schema specified as exemplar schema");

    LOG4CXX_TRACE(logger,  parent->getPhysicalOperator()->getPhysicalName()
                  << " has redimension/repart ("
                  << (given.isAutochunked() ? "" : "NOT")
                  << " autochunked) child: \n"
                  << "  desired schema: " << desired << "\n"
                  << "    given schema: " << given);

    if (!given.isAutochunked()) {
        return false;
    }

    Dimensions const& desiredDims = desired.getDimensions();
    Dimensions const& givenDims = given.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    ASSERT_EXCEPTION(desiredDims.size() == givenDims.size(),
                     "Desired redimension schema cannot alter the number of dimensions");
    // Start with the "final" resulting schema identical to the given
    // child schema
    result = given;
    Dimensions resultDimensions;
    resultDimensions.reserve(desiredDims.size());

    Dimensions::const_iterator itDesiredDim, itGivenDim;
    for(itDesiredDim = desiredDims.begin(), itGivenDim = givenDims.begin();
        itDesiredDim != desiredDims.end();
        ++itDesiredDim, ++itGivenDim)
    {
        DimensionDesc nextDim = *itGivenDim;
        //
        // startMin
        if (itDesiredDim->getStartMin() != itGivenDim->getStartMin()) {
            LOG4CXX_TRACE(logger, "redimension/repart is NOT collapsible startMin differs");
            return false;
        }
        //
        // endMax
        if (itDesiredDim->getEndMax() != itGivenDim->getEndMax()) {
            if (allowEndMaxChange(parent->getPhysicalOperator()) &&
                (itDesiredDim->getEndMax() < itGivenDim->getEndMax())) {
                LOG4CXX_TRACE(logger, "setting endMax from "
                              << itGivenDim->getEndMax() << " to " << itDesiredDim->getEndMax()
                              << " for " << itGivenDim->getBaseName());
                nextDim.setEndMax(itDesiredDim->getEndMax());
            }
            else {
                LOG4CXX_TRACE(logger, "redimension/repart is NOT collapsible endMax differs");
                return false;
            }
        }
        //
        // chunkOverlap
        if (itDesiredDim->getChunkOverlap() != itGivenDim->getChunkOverlap()) {
            LOG4CXX_TRACE(logger,"redimension/repart is NOT collapsible"
                          << " desired ChunkOverlap is != input chunkOverlap");
            return false;
        }
        //
        // chunkInterval
        if (itGivenDim->isAutochunked() && !itDesiredDim->isAutochunked()) {
            // Use (get|set)rawChunkInterval since the Interval could be
            // DimensionDesc::PASSTHRU.
            nextDim.setRawChunkInterval(itDesiredDim->getRawChunkInterval());
        }
        else if (itDesiredDim->getRawChunkInterval() != itGivenDim->getRawChunkInterval())
        {
            LOG4CXX_TRACE(logger,"redimension/repart is NOT collapsible"
                          << " desired Chunk Interval differs and is NOT autochunked.")
            return false;
        }
        //
        // add this dimension to the result
        resultDimensions.push_back(nextDim);
    }
    result.setDimensions(resultDimensions);
    LOG4CXX_TRACE(logger, "Redim is collapsible into" << "\n"
                  << "         " << result);
    return true;
}

/**
 *  Insert any needed redimension()/repart() operators into the physical plan.
 */
bool HabilisOptimizer::tw_insertRedimensionOrRepartitionNodes(PhysNodePtr nodep)
{
    bool subtreeModified = false;

    // Leaf node?  Done.
    const size_t N_CHILDREN = nodep->getChildren().size();
    if (N_CHILDREN == 0)
    {
        nodep->inferBoundaries();
        nodep->inferDistribution();
        return subtreeModified;
    }

    // Build input vectors for making the repart/no-repart decision.
    vector<ArrayDesc> schemas(nodep->getChildSchemas());
    assert(schemas.size() == N_CHILDREN);
    vector<ArrayDesc const*> modifiedPtrs(N_CHILDREN);

    // Initial scan of the children.
    for (size_t nChild = 0; nChild < N_CHILDREN; ++nChild)
    {
        PhysNodePtr& child = nodep->getChildren()[nChild];

        // Handle children first.  Change the tree from bottom to top,
        // so that any inferences about boundaries and distributions
        // can percolate up.
        //
        subtreeModified |= tw_insertRedimensionOrRepartitionNodes(child);

        // If any child is itself a repartition or redimension operator, it must
        // have been manually inserted, since we haven't altered our immediate
        // children yet.  Set the corresponding repartPtrs entry to be non-NULL.
        //
        if (child->isRepartNode() || child->isRedimensionNode())
        {
            modifiedPtrs[nChild] = &schemas[nChild];
        }
    }

    // Now for the current node.  Ask it: want to repartition any input schema?
    nodep->getPhysicalOperator()->requiresRedimensionOrRepartition(schemas, modifiedPtrs);
    if (modifiedPtrs.empty())
    {
        // Nothing to do here, but keep the inference chain going.
        nodep->inferBoundaries();
        nodep->inferDistribution();
        return subtreeModified;
    }

    // The modifiedPtrs vector describes how the nodep operator wants
    // each of its children repartitioned.
    //
    OperatorLibrary* oplib = OperatorLibrary::getInstance();
    Parameters params(1);
    size_t numSchemaChanges = 0;
    for (size_t nChild = 0; nChild < N_CHILDREN; ++nChild)
    {
        if (modifiedPtrs[nChild] == nullptr) {
            // This child's schema is fine, no change.
            continue;
        }
        numSchemaChanges += 1;
        PhysNodePtr& child = nodep->getChildren()[nChild];
        ArrayDesc const& childSchema = schemas[nChild];
        ArrayDesc const& desiredSchema = *modifiedPtrs[nChild];

        if (child->isRepartNode() || child->isRedimensionNode()) {
            if (childSchema.sameShape(desiredSchema)) {
                // The desired schema and the child's schema are the same.
                //  -- no need to insert redimension
                continue;
            }
            // Check if the given child and the desired repart/redimension from
            // requiresRedimensionOrRepartition can be collapsed into a single
            // operation.
            ArrayDesc resultSchema;
            if (isRedimCollapsible(nodep,
                                   desiredSchema,
                                   childSchema,
                                   resultSchema)) {

                // Create a new Physical Operator based upon the original child
                Parameters childParams =
                     child->getPhysicalOperator()->getParameters();

                ArrayDistPtr undefDist =
                    ArrayDistributionFactory::getInstance()->construct(psUndefined,
                                                                       DEFAULT_REDUNDANCY);
                resultSchema.setDistribution(undefDist);
                // Wrap desired schema in Parameters object.
                childParams[0] = std::shared_ptr<OperatorParam>(
                    new OperatorParamSchema(make_shared<ParsingContext>(), resultSchema));

                // Create phys. plan node for "op" based upon child
                PhysOpPtr op;
                if (child->isRedimensionNode()) {
                    op = oplib->createPhysicalOperator("redimension","PhysicalRedimension",
                                                       childParams, resultSchema);
                }
                else if (!childSchema.sameDimensionRanges(resultSchema)) {
                    assert(child->isRepartNode());
                    // Child is specified as a repart, but the operator requires
                    // a redimension because the dimension range needs to
                    // change.
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_BAD_EXPLICIT_REPART2)
                        << nodep->getPhysicalOperator()->getLogicalName();
                }
                else {
                    assert(child->isRepartNode());
                    op = oplib->createPhysicalOperator("repart", "physicalRepart",
                                                       childParams, resultSchema);
                }
                op->setQuery(_query);

                // and supplant child node with the new node.
                PhysNodePtr newNode(new PhysicalQueryPlanNode(op, false/*ddl*/, false/*tile*/));
                nodep->supplantChild(child,newNode);

                // Re-run inferences for the new child.
                newNode->inferBoundaries();
                newNode->inferDistribution();

                continue;
            } // if (isRedimCollapsible(...))
            else {
                // The desired schema does not match the explicitly provided
                // redimension/repart AND the desired schema cannot be
                // "collapsed" with the given repart/redimension, so the query
                // is invalid.
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_BAD_EXPLICIT_REPART)
                    << nodep->getPhysicalOperator()->getLogicalName()
                    << childSchema.getDimensions()
                    << desiredSchema.getDimensions();
            }
        } // if (child->isRepartNode() || child->isRedimensionNode())

        // Child input is NOT repart/redimension.
        ArrayDesc modifiedSchema = *modifiedPtrs[nChild];
        ArrayDistPtr undefDist =
            ArrayDistributionFactory::getInstance()->construct(psUndefined,
                                                               DEFAULT_REDUNDANCY);
        modifiedSchema.setDistribution(undefDist);
        //
        // Wrap desired schema in Parameters object.
        params[0] = std::shared_ptr<OperatorParam>(
            new OperatorParamSchema(make_shared<ParsingContext>(), modifiedSchema));
        //
        // If the ranges changed, we must redimension() rather than repart().
        PhysOpPtr op =
            schemas[nChild].sameDimensionRanges(*modifiedPtrs[nChild])
            ? oplib->createPhysicalOperator("repart", "physicalRepart", params, modifiedSchema)
            : oplib->createPhysicalOperator("redimension", "PhysicalRedimension", params, modifiedSchema);
        op->setQuery(_query);
        op->setControlCookie("repart"); // Puts redimension() in "repart mode".

        // Create phys. plan node for "op" and splice it in
        // above child[nChild].
        PhysNodePtr newNode(new PhysicalQueryPlanNode(op, false/*ddl*/, false/*tile*/));
        n_addParentNode(nodep->getChildren()[nChild], newNode);

        // Re-run inferences for the new child.
        newNode->inferBoundaries();
        newNode->inferDistribution();
    }

    // If requiresRedimensionOrRepartition() gave us a non-empty vector, it better have at
    // least one repartSchema/redimensionSchema for us.
    if (isDebug())
    {
        assert(numSchemaChanges > 0);
    }

    // Re-run inferences for this node and we are done.
    nodep->inferBoundaries();
    nodep->inferDistribution();
    return true;
}

void HabilisOptimizer::tw_insertChunkMaterializers(PhysNodePtr root)
{
    if ( root->hasParent() && root->getChildren().size() != 0)
    {
        PhysNodePtr parent = root->getParent();
        if (root->isSgNode() == false && root->getPhysicalOperator()->getTileMode() != parent->getPhysicalOperator()->getTileMode())
        {
            ArrayDesc const& schema = root->getPhysicalOperator()->getSchema();
            Value formatParameterValue;
            formatParameterValue.setInt64(MaterializedArray::RLEFormat);
            std::shared_ptr<Expression> formatParameterExpr = std::make_shared<Expression> ();
            formatParameterExpr->compileConstant(false, TID_INT64, formatParameterValue);
            Parameters params;
            params.push_back(std::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(
                std::make_shared<ParsingContext>(), formatParameterExpr, CONSTANT)));

            PhysOpPtr materializeOp = OperatorLibrary::getInstance()->createPhysicalOperator(
                "_materialize", "impl_materialize", params, schema);
            materializeOp->setQuery(_query);

            PhysNodePtr materializeNode(new PhysicalQueryPlanNode(materializeOp, false, false));
            n_addParentNode(root, materializeNode);
            materializeNode->inferBoundaries();
            materializeNode->inferDistribution();
        }
    }

    for (size_t i =0; i < root->getChildren().size(); i++)
    {
        tw_insertChunkMaterializers(root->getChildren()[i]);
    }
}

std::shared_ptr<Optimizer> Optimizer::create()
{
    LOG4CXX_TRACE(logger, "Creating Habilis optimizer instance")
    return std::shared_ptr<Optimizer> (new HabilisOptimizer());
}

} // namespace
