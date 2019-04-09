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
 * @file LogicalSetopt.cpp
 * @brief Set configuration option values.
 */

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <rbac/Rights.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: _setopt().
 *
 * @par Synopsis:
 *   _setopt( option, newValue )
 *
 * @par Summary:
 *   Sets a config option at runtime.
 *
 * @par Input:
 *   - option: the config option.
 *   - newValue: the new value for the config option.
 *
 * @par Output array:
 *        <
 *   <br>   old: string
 *   <br>   new: string
 *   <br> >
 *   <br> [
 *   <br>   No: start=0, end=#instances-1, chunk interval=1
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalSetopt: public LogicalOperator
{
public:
    LogicalSetopt(const string& logicalName, const string& alias)
        : LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_CONSTANT(TID_STRING);
        ADD_PARAM_CONSTANT(TID_STRING);
    }

    void inferAccess(std::shared_ptr<Query>& query) override
    {
        // Need database administrative powers.
        query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_ADMIN);
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, std::shared_ptr< Query> query) override
    {
        assert(schemas.size() == 0);
        assert(_parameters.size() == 2);

        string whatToChange = evaluate(
            reinterpret_cast<std::shared_ptr<OperatorParamLogicalExpression> &>(
                _parameters[0])->getExpression(), TID_STRING).getString();
        if( whatToChange.compare("security") == 0)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OPTION_NOT_ALLOWED)
                << whatToChange;
        }

        vector<AttributeDesc> attributes;
        AttributeID attributeId = 0;
        attributes.push_back( AttributeDesc(attributeId++, "old",  TID_STRING, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc(attributeId++, "new",  TID_STRING, 0, CompressorType::NONE));

        /* Add the empty tag attribute. Arrays with the empty tag are "emptyable" meaning that
         * some cells may be empty. It is a good practice to add this to every constructed array.
         * In fact, in the future it may become the default for all arrays.
         */
        attributes = addEmptyTagAttribute(attributes);

        vector<DimensionDesc> dimensions(1);
        const size_t nInstances = query->getInstancesCount();
        const size_t end = nInstances>0 ? nInstances-1 : 0;
        dimensions[0] = DimensionDesc("No", 0, 0, end, end, 1, 0);

        return ArrayDesc("Option", attributes, dimensions,
                         defaultPartitioning(),
                         query->getDefaultArrayResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSetopt, "_setopt")

}  // namespace scidb
