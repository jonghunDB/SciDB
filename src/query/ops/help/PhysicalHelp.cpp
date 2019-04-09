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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief This operator shows parameters of other operator
 */

#include "query/Operator.h"
#include "query/OperatorLibrary.h"
#include "array/MemArray.h"

using namespace std;

namespace scidb
{

class PhysicalHelp: public PhysicalOperator
{
public:
    PhysicalHelp(const std::string& logicalName,
        const std::string& physicalName, const Parameters& parameters,
        const ArrayDesc& schema) :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext>& inputDistributions,
                                                 const std::vector< ArrayDesc>& inputSchemas) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        stringstream ss;

        if (_parameters.size() == 1)
        {
            const string opName = paramToString(_parameters[0]);
            std::shared_ptr<LogicalOperator> op =
                OperatorLibrary::getInstance()->createLogicalOperator(opName);
            ss << "Operator: " << opName << endl << "Usage: ";

            if (op->getUsage() == "")
            {
                ss << opName << "(";
                bool first = true;

                for (auto const& ph : op->getParamPlaceholders())
                {
                    if (first) {
                        first = false;
                    } else {
                        ss << ", ";
                    }
                    ss << _placeholderTypeToString(ph->getPlaceholderType());
                }

                if (!op->getKeywordPlaceholders().empty())
                {
                    if (first) {
                        ss << '[';
                    } else {
                        ss << ", [";
                        first = true;
                    }

                    for (auto const& kw : op->getKeywordPlaceholders())
                    {
                        if (kw.first[0] == '_')
                            continue; // undocumented keyword
                        if (first) {
                            first = false;
                        } else {
                            ss << ", ";
                        }
                        ss << kw.first << ": "
                           << _placeholderTypeToString(kw.second->getPlaceholderType());
                    }
                    ss << ']';
                }

                ss << ')';
            }
            else
            {
                ss << op->getUsage();
            }
        }
        else
        {
            ss << "Use existing operator name as argument for help operator. "
                "You can see all operators by executing list('operators').";
        }

        _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        std::shared_ptr<ArrayIterator> arrIt = _result->getIterator(0);
        Coordinates coords;
        coords.push_back(0);
        Chunk& chunk = arrIt->newChunk(coords);
        std::shared_ptr<ChunkIterator> chunkIt = chunk.getIterator(query);
        Value v(TypeLibrary::getType(TID_STRING));
        v.setString(ss.str().c_str());
        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    std::shared_ptr<Array> execute(
        std::vector<std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(inputArrays.size() == 0);
        if (!_result) {
            _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        }
        return _result;
    }

private:

    static const char* _placeholderTypeToString(OperatorParamPlaceholderType x)
    {
        switch (x) {
        case PLACEHOLDER_INPUT:          return "<input>";
        case PLACEHOLDER_ARRAY_NAME:     return "<array name>";
        case PLACEHOLDER_ATTRIBUTE_NAME: return "<attribute name>";
        case PLACEHOLDER_CONSTANT:       return "<constant>";
        case PLACEHOLDER_DIMENSION_NAME: return "<dimension name>";
        case PLACEHOLDER_EXPRESSION:     return "<expression>";
        case PLACEHOLDER_SCHEMA:         return "<schema>";
        case PLACEHOLDER_AGGREGATE_CALL: return "<aggregate call>";
        case PLACEHOLDER_NS_NAME:        return "<namespace name>";
        case PLACEHOLDER_VARIES:         return "...";
        default:
            SCIDB_ASSERT(false);
            return "<unknown placeholder>"; // avoid compiler warning
        }

        SCIDB_UNREACHABLE();
    }

    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalHelp, "help", "impl_help")

} //namespace
