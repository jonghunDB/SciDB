/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2018 SciDB, Inc.
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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>
#include <log4cxx/logger.h>

using boost::shared_ptr;

using namespace std;

namespace scidb {

class LogicalSummarize: public LogicalOperator
{
public:
    LogicalSummarize(const std::string& logicalName, const std::string& alias) :
            LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_VARIES();

        addKeywordPlaceholder("by_instance", PARAM_CONSTANT(TID_BOOL));
        addKeywordPlaceholder("by_attribute", PARAM_CONSTANT(TID_BOOL));

    }

    Placeholders nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        Placeholders res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) {
          case 0:
            res.push_back(PARAM_CONSTANT(TID_BOOL));
            break;
          case 1:
            res.push_back(PARAM_CONSTANT(TID_BOOL));
            break;
        }
        return res;
    }

    ArrayDesc getSchema(std::shared_ptr<Query>& query, size_t numInputAttributes)
    {
        size_t numInstances = query->getInstancesCount();
        vector<DimensionDesc> dimensions(2);
        dimensions[0] = DimensionDesc("inst",  0, 0, numInstances-1,       numInstances-1,       1,                   0);
        dimensions[1] = DimensionDesc("attid", 0, 0, numInputAttributes-1, numInputAttributes-1, numInputAttributes, 0);

        vector<AttributeDesc> attributes;
        attributes.push_back(AttributeDesc(0,
                                           "att",
                                           TID_STRING,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(1,
                                           "count",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(2,
                                           "bytes",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(3,
                                           "chunks",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(4,
                                           "min_count",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(5,
                                           "avg_count",
                                           TID_DOUBLE,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(6,
                                           "max_count",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(7,
                                           "min_bytes",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(8,
                                           "avg_bytes",
                                           TID_DOUBLE,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc(9,
                                           "max_bytes",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes = addEmptyTagAttribute(attributes);
        return ArrayDesc("summarize",
                         attributes,
                         dimensions,
                         defaultPartitioning(),
                         query->getDefaultArrayResidency());
   }


    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        size_t numInputAttributes = inputSchema.getAttributes().size();
        return getSchema(query, numInputAttributes);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSummarize, "summarize")

} // end namespace scidb
