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
 * @file QueryTree.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <memory>

#include <log4cxx/logger.h>
#include <query/QueryPlanUtilites.h>
#include <query/QueryPlan.h>
#include <query/LogicalExpression.h>

using namespace boost;
using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

//
// LogicalQueryPlanNode
//

LogicalQueryPlanNode::LogicalQueryPlanNode(
    const std::shared_ptr<ParsingContext>& parsingContext,
    const std::shared_ptr<LogicalOperator>& logicalOperator):
    _logicalOperator(logicalOperator),
    _parsingContext(parsingContext)
{
}

LogicalQueryPlanNode::LogicalQueryPlanNode(
    const std::shared_ptr<ParsingContext>& parsingContext,
    const std::shared_ptr<LogicalOperator>& logicalOperator,
    const std::vector<std::shared_ptr<LogicalQueryPlanNode> > &childNodes):
    _logicalOperator(logicalOperator),
    _childNodes(childNodes),
    _parsingContext(parsingContext)
{
}

const ArrayDesc& LogicalQueryPlanNode::inferTypes(std::shared_ptr< Query> query, bool forceSchemaUpdate)
{
    if (_logicalOperator->hasSchema() && !forceSchemaUpdate) {
        return _logicalOperator->getSchema(); // the cached answer will work, avoids excessive recursion
    }

    // recurse on the child nodes to determine their ArrayDescriptors, prior to
    // computing this node's ArrayDescriptor as derived from the children's descriptors.
    std::vector<ArrayDesc> inputSchemas;
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        inputSchemas.push_back(_childNodes[i]->inferTypes(query));
    }

    ArrayDesc outputSchema = _logicalOperator->inferSchema(inputSchemas, query);
    LOG4CXX_TRACE(logger, "LogicalQueryPlanNode::inferTypes produced PS: " << outputSchema.getDistribution()->getPartitioningSchema());

    // TODO: Consider wrapping LogicalOperator::inferSchema with a method which does the following
    //       addAlias() logic rather than having this method take responsibility for it.
    if (!_logicalOperator->getAliasName().empty())
    {
        outputSchema.addAlias(_logicalOperator->getAliasName());  // TBD: investigate addAlias vs setAliasName
    }

    _logicalOperator->setSchema(outputSchema);  // cache the result

    return _logicalOperator->getSchema();
}

void LogicalQueryPlanNode::inferAccess(std::shared_ptr<Query>& query)
{
    //XXX TODO: consider non-recursive implementation
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        _childNodes[i]->inferAccess(query);
    }

    assert(_logicalOperator);
    _logicalOperator->inferAccess(query);

    if (!_logicalOperator->hasRightsForAllNamespaces(query)) {
        // One or more of the namespaces referred to, explicitly or
        // implicitly, by this operator's array parameters does not
        // appear in the query's "rights I need" map.  That's a bug on
        // the part of the operator's author.
        //
        // It's better to throw this error than to allow the query to
        // run without access control enforcement.
        stringstream ss;
        ss << "Operator '" << _logicalOperator->getLogicalName()
           << "' is missing rights for one or more namespaces it uses,"
           << " see log file for details";
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
            << ss.str();
    }
}

void LogicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lInstance] children "<<_childNodes.size()<<"\n";
    _logicalOperator->toString(out,indent+1);

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++)
        {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

//
// PhysicalQueryPlanNode
//

PhysicalQueryPlanNode::PhysicalQueryPlanNode()
{
     LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(), no-arg ctor!!!");
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const std::shared_ptr<PhysicalOperator>& physicalOperator,
                                             bool ddl, bool tile)
: _physicalOperator(physicalOperator),
  _parent(), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(3arg) opName: " << physicalOperator->getPhysicalName());
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(3arg) ps: " << physicalOperator->getSchema());
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(3arg) _distribution() is defaulted");
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const std::shared_ptr<PhysicalOperator>& physicalOperator,
		const std::vector<std::shared_ptr<PhysicalQueryPlanNode> > &childNodes,
                                             bool ddl, bool tile):
	_physicalOperator(physicalOperator),
	_childNodes(childNodes),
    _parent(), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(4arg) opName: " << physicalOperator->getPhysicalName());
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(4arg) ps: " << physicalOperator->getSchema());
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(4arg) _distribution() is defaulted");
}

PhysicalQueryPlanNode::~PhysicalQueryPlanNode()
{}

void PhysicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);

    out<<"[pNode] "<<_physicalOperator->getPhysicalName()<<" ID "<< _physicalOperator->getOperatorID().getValue()
       <<" ddl "<<isDdl()<<" tile "<<supportsTileMode()<<" children "<<_childNodes.size()<<"\n";
    _physicalOperator->toString(out,indent+1);

    if (children) {
        out << prefix(' ');
        out << "output full chunks: ";
        out << (outputFullChunks() ? "yes" : "no");
        out << "\n";
        out << prefix(' ');
        out << "changes dstribution: ";
        out << (changesDistribution() ? "yes" : "no");
        out << "\n";
    }

    out << prefix(' ');
    out<<"props sgm "<<_isSgMovable<<" sgo "<<_isSgOffsetable<<"\n";
    out << prefix(' ');
    out<<"diout "<<_distribution<<"\n";
    const ArrayDesc& schema = _physicalOperator->getSchema();
    out << prefix(' ');
    out<<"bound "<<_boundaries
      <<" cells "<<_boundaries.getNumCells();

    if (_boundaries.getStartCoords().size() == schema.getDimensions().size()) {
        out  << " chunks ";
        try {
            uint64_t n = _boundaries.getNumChunks(schema.getDimensions());
            out << n;
        } catch (PhysicalBoundaries::UnknownChunkIntervalException&) {
            out << '?';
        }
        out << " est_bytes " << _boundaries.getSizeEstimateBytes(schema)
            << '\n';
    }
    else {
        out <<" [improperly initialized]\n";
    }

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++) {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

bool PhysicalQueryPlanNode::isStoringSg() const
{
    if ( isSgNode() ) {
        return (!getSgArrayName(_physicalOperator->getParameters()).empty());
    }
    return false;
}

string PhysicalQueryPlanNode::getSgArrayName(const Parameters& sgParameters)
{
    std::string arrayName;
    if (sgParameters.size() >= 3) {
        arrayName = static_cast<OperatorParamReference*>(sgParameters[2].get())->getObjectName();
    }
    return arrayName;
}

void
PhysicalQueryPlanNode::supplantChild(const PhysNodePtr& targetChild,
                                     const PhysNodePtr& newChild)
{
    assert(newChild);
    assert(targetChild);
    assert(newChild.get() != this);
    int removed = 0;
    std::vector<PhysNodePtr> newChildren;

    if (logger->isTraceEnabled()) {
        std::ostringstream os;
        os << "Supplanting targetChild Node:\n";
        targetChild->toString(os, 0 /*indent*/,false /*children*/);
        os << "\nwith\n";
        newChild->toString(os, 0 /*indent*/,false /*children*/);
        LOG4CXX_TRACE(logger, os.str());
    }

    for(auto &child : _childNodes) {
        if (child != targetChild) {
            newChildren.push_back(child);
        }
        else {
            // Set the parent of the newChild to this node.
            newChild->_parent = shared_from_this();

            // NOTE: Any existing children of the newChild are removed from the
            // Query Plan.
            if ((newChild->_childNodes).size() > 0) {
                LOG4CXX_INFO(logger,
                             "Child nodes of supplanting node are being removed from the tree.");
            }

            // Re-parent the children of the targetChild to the newChild
            newChild->_childNodes.swap(targetChild->_childNodes);
            for (auto grandchild : newChild -> _childNodes) {
                assert(grandchild != newChild);
                grandchild->_parent = newChild;
            }

            // Remove any references to the children from the targetChild
            targetChild->_childNodes.clear();
            targetChild->resetParent();

            // Add the newChild to this node
            newChildren.push_back(newChild);
            ++removed;
        }
    }
    _childNodes.swap(newChildren);

    if (logger->isTraceEnabled()) {
        std::ostringstream os;
        newChild->toString(os);
        LOG4CXX_TRACE(logger, "New Node subplan:\n"
                      << os.str());
    }

    SCIDB_ASSERT(removed==1);
}

void PhysicalQueryPlanNode::visitDepthFirst(PhysicalQueryPlanNode::Visitor& v)
{
    for (auto& child : _childNodes) {
        child->visitDepthFirst(v);
    }
    v(*this);
}

size_t PhysicalQueryPlanNode::estimatedMaxDataVolume(size_t maxStringLength) const
{
    // estimate meaning it does not have to be perfect, it need
    // only be good enough for estimating the cost/benefit of inserting
    // _SG()'s when not required for correctness

    const auto& schema = _physicalOperator->getSchema();

    size_t maxBytesPerCell = 0;
    for(auto const& attr : schema.getAttributes()){
        auto bytes = attr.getSize();
        if (bytes == 0) {             // variable length
            bytes = maxStringLength;  // caller determines estimate
        }
        maxBytesPerCell += bytes;
    }

    LOG4CXX_TRACE(logger, "estimatedMaxDataVolume: maxBytesPerCell " << maxBytesPerCell);
    LOG4CXX_TRACE(logger, "estimatedMaxDataVolume: numCells " << _boundaries.getNumCells());
    size_t maxBytes = maxBytesPerCell * _boundaries.getNumCells() ;

    return maxBytes;
}

// LogicalPlan
LogicalPlan::LogicalPlan(const std::shared_ptr<LogicalQueryPlanNode>& root):
        _root(root)
{

}

void LogicalPlan::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lPlan]:\n";
    _root->toString(out, indent+1, children);
}

// PhysicalPlan
PhysicalPlan::PhysicalPlan(const std::shared_ptr<PhysicalQueryPlanNode>& root):
        _root(root)
{

}

void PhysicalPlan::toString(std::ostream &out, int const indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[pPlan]:";
    if (_root.get() != NULL)
    {
        out << "\n";
        _root->toString(out, indent+1, children);
    }
    else
    {
        out << "[NULL]\n";
    }
}

const RedistributeContext& PhysicalQueryPlanNode::inferDistribution ()
{
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::inferDistribtuion()  opName: " << _physicalOperator->getPhysicalName());

    std::vector<RedistributeContext> childDistros;
    for (size_t i =0; i<_childNodes.size(); i++)
    {
        childDistros.push_back(_childNodes[i]->getDistribution());
        LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::inferDistribution() childDistros["<<i<<"] = " <<  childDistros.back());
    }

    _distribution = _physicalOperator->getOutputDistribution(childDistros, getChildSchemas());
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::inferDistribution() output _distribution set to " << _distribution);
    return _distribution;
}

} // namespace
