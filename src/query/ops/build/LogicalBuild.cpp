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
 * LogicalBuild.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include <array/Dense1MChunkEstimator.h>
#include <query/Operator.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb {

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.build"));
#define debug(e) LOG4CXX_DEBUG(logger, "LogicalBuild: " << e)
#define trace(e) LOG4CXX_TRACE(logger, "LogicalBuild: " << e)

/**
 * @brief The operator: build().
 *
 * @par Synopsis:
 *   build( schemaArray | schema, expression, mustBeConstant = false )
 *
 * @par Summary:
 *   Produces a result array according to a given schema, and populates values based on the given expression.
 *   The schema must have a single attribute.
 *
 * @par Input:
 *   - schemaArray | schema: an array or a schema, from which attrs and dims will be used by the output array.
 *   - expression: the expression which is used to compute values for the output array.
 *   - mustBeConstant: whether the expression must be a constant.
 *
 * @par Output array:
 *        <
 *   <br>   attrs
 *   <br> >
 *   <br> [
 *   <br>   dims
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64> [year, item] =
 *     <br> year, item, quantity
 *     <br> 2011,  2,      7
 *     <br> 2011,  3,      6
 *     <br> 2012,  1,      5
 *     <br> 2012,  2,      9
 *     <br> 2012,  3,      8
 *   - build(A, 0) <quantity: uint64> [year, item] =
 *     <br> year, item, quantity
 *     <br> 2011,  1,      0
 *     <br> 2011,  2,      0
 *     <br> 2011,  3,      0
 *     <br> 2012,  1,      0
 *     <br> 2012,  2,      0
 *     <br> 2012,  3,      0
 *     Note that the cell (2011, 1), which was empty in the source array, is populated.
 *
 * @par Errors:
 *   - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_OP_BUILD_ERROR2, if the source array has more than one attribute.
 *
 * @par Notes:
 *   - The build operator can only take as input bounded dimensions.
 *
 */
class LogicalBuild: public LogicalOperator
{
public:
    LogicalBuild(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_SCHEMA()
        ADD_PARAM_VARIES()

        // Say "from: '[(1),(2)]'" instead of "'[(1),(2)]', true"
        addKeywordPlaceholder("from",   PARAM_CONSTANT(TID_STRING));
    }

    Placeholders nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        Placeholders res;
        switch (_parameters.size()) {
        case 1:
            res.push_back(PARAM_EXPRESSION(TID_VOID));
            res.push_back(END_OF_VARIES_PARAMS());
            break;
        case 2:
            res.push_back(END_OF_VARIES_PARAMS());
            res.push_back(PARAM_CONSTANT(TID_BOOL));
            break;
        case 3:
            res.push_back(END_OF_VARIES_PARAMS());
            break;
        default:
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR,
                                 SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3)
                << "build" << "two or three";
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(schemas.empty());

        bool asArrayLiteral = false;
        Parameter exprParam;
        if (_parameters.size() == 1) {
            // build(schema, keyword_params)
            exprParam = findKeyword("from");
            if (!exprParam) {
                // Need either an expression (param[1]) or a "from: string" keyword.
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2)
                    << "build";
            }
            asArrayLiteral = true;
        } else {
            // build(schema, expr [ , bool_exp ])
            if (findKeyword("from")) {
                // Given a param[1] expression, "from:" is disallowed.
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL)
                    << "build" << "from" << 2;
            }
            SCIDB_ASSERT(_parameters.size() == 2 || _parameters.size() == 3);
            exprParam = _parameters[1];
            if (_parameters.size() == 3) {
                asArrayLiteral = evaluate(
                    ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                    TID_BOOL).getBool();
            }
        }

        ArrayDesc desc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
        if (!asArrayLiteral && desc.getAttributes(true).size() != 1) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_BUILD_ERROR2,
                                       _parameters[0]->getParsingContext());
        }

        if (desc.getName().empty())
        {
            desc.setName("build");
        }

        // If an array name was used in the build use that as the namespaces.
        // Otherwise, use the namespace name specified by set_namespace().
        std::string arrayName = desc.getName();
        std::string namespaceName = desc.getNamespaceName();
        // query->getNamespaceArrayNames(arrayName, namespaceName, arrayName);
        desc.setName(arrayName);
        desc.setNamespaceName(namespaceName);

        // Check dimensions
        Dense1MChunkEstimator::estimate(desc.getDimensions());
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = dims.size();  i < n; i++)
        {
            if (dims[i].isAutochunked())
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                           SCIDB_LE_AUTOCHUNKING_NOT_SUPPORTED,
                                           _parameters[0]->getParsingContext()) << getLogicalName();
            }

            // Eventually this check should be removed.
            if (dims[i].isMaxStar() && !asArrayLiteral)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_BUILD_ERROR3,
                                           _parameters[0]->getParsingContext());
            }
        }

        if (asArrayLiteral)
        {
            bool good = true;
            //Check second argument type (must be string) and constness
            try
            {
                Expression e;
                constexpr bool TILE_MODE = true;
                e.compile(((std::shared_ptr<OperatorParamLogicalExpression>&)exprParam)->getExpression(),
                    !TILE_MODE, TID_STRING);
                good = e.isConstant();
            }
            catch(...)
            {
                good = false;
            }
            if (!good)
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_ARRAY_LITERAL,
                    _parameters[1]->getParsingContext());
            }

            ArrayDistPtr localDist = std::make_shared<LocalArrayDistribution>(query->getInstanceID());
            desc.setDistribution(localDist);
        }
        else
        {
            desc.setDistribution(defaultPartitioning());
        }

        desc.setResidency(query->getDefaultArrayResidency());
        debug("inferSchema: returning schema with distribution: " << desc.getDistribution()
              << ", asArrayLiteral: " << asArrayLiteral);
        return desc;
    }
};

} // namespace

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBuild, "build")

} // namespace scidb
