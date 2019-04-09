/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2018 SciDB, Inc.
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
 * @file LogicalShowArrays.cpp
 */

#include "FilterArrays.h"

#include <query/Operator.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

#include <sstream>
#include <vector>


namespace scidb
{

/**
 * @brief The operator: _show_arrays().
 *
 * @par Synopsis:
 *   _show_arrays(regularExpression, ignoreOrphanAttributes, ignoreVersions)
 *   requires namespaces root privileges
 *
 * @returns Summary:
 *   Returns an attribute of a table based on a regular expression
 *
 */
class LogicalShowArrays : public LogicalOperator
{
public:
    /**
     * see LogicalOperator
     */
    LogicalShowArrays(
        const std::string& logicalName,
        const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_CONSTANT(TID_STRING);  // regExNamespace
        ADD_PARAM_CONSTANT(TID_STRING);  // regExArray
        ADD_PARAM_CONSTANT(TID_BOOL);  // ignoreOrphanAttributes
        ADD_PARAM_CONSTANT(TID_BOOL);  // ignoreVersions
    }

    void inferAccess(std::shared_ptr<Query>& query) override
    {
        // Need database administrative powers.
        query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_ADMIN);
    }

    /**
     * see LogicalOperator
     */
    ArrayDesc inferSchema(
            std::vector<ArrayDesc> schemas,
            std::shared_ptr< Query> query)
    {
        std::string regExNamespace = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[0])->getExpression(),
            TID_STRING).getString();

        std::string regExArray = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[1])->getExpression(),
            TID_STRING).getString();

        bool ignoreOrphanAttributes = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[2])->getExpression(),
            TID_BOOL).getBool();

        bool ignoreVersions = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[3])->getExpression(),
            TID_BOOL).getBool();

        const bool orderByName = true;

        std::vector<ArrayDesc> arrayDescs;
        SystemCatalog::getInstance()->getArrays(
            "", arrayDescs, ignoreOrphanAttributes, ignoreVersions, orderByName);
        filterArrays(arrayDescs, regExNamespace, regExArray);

        Attributes attributes(2);
        attributes[0] = AttributeDesc(0, "namespace", TID_STRING,
                                      AttributeDesc::IS_NULLABLE, CompressorType::NONE);
        attributes[1] = AttributeDesc(1, "array", TID_STRING,
                                      AttributeDesc::IS_NULLABLE, CompressorType::NONE);

        /* Add the empty tag attribute. Arrays with the empty tag are "emptyable" meaning that
         * some cells may be empty. It is a good practice to add this to every constructed array.
         * In fact, in the future it may become the default for all arrays.
         */
        attributes = addEmptyTagAttribute(attributes);

        int size = static_cast<int>(arrayDescs.size());
        int end  = size>0 ? size-1 : 0;

        std::stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(
            psLocalInstance, DEFAULT_REDUNDANCY, ss.str());
        Dimensions dimensions(1,DimensionDesc("i", 0, 0, end, end, size, 0));
        return ArrayDesc("showArrays", attributes, dimensions,
                         localDist,
                         query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalShowArrays, "_show_arrays");

} // emd namespace p4
