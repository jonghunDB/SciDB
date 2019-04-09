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
 * @file LogicalSave.cpp
 * @author roman.simakov@gmail.com
 * @brief Save operator for saving data from external files into array
 */

#include "Save.h"

#include <query/Operator.h>
#include <smgr/io/ArrayWriter.h>
#include <system/Exceptions.h>
#include <util/PathUtils.h>

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: save().
 *
 * @par Synopsis:
 *   save( srcArray, file, instanceId = -2, format = 'store' )
 *
 * @par Summary:
 *   Saves the data in an array to a file.
 *
 * @par Input:
 *   - srcArray: the source array to save from.
 *   - file: the file to save to.
 *   - instanceId: positive number means an instance ID on which file will be saved.
 *                 -1 means to save file on every instance. -2 - on coordinator.
 *   - format: @c ArrayWriter format in which file will be stored
 *
 * @see ArrayWriter::isSupportedFormat
 *
 * @par Output array:
 *   the srcArray is returned
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
/**
 * Must be called as SAVE('existing_array_name', '/path/to/file/on/instance')
 */
class LogicalSave: public LogicalOperator
{
    string _expandedPath;

public:
    LogicalSave(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT(TID_STRING);
        ADD_PARAM_VARIES();

        addKeywordPlaceholder(KW_INSTANCE, PARAM_CONSTANT(TID_INT64));
        addKeywordPlaceholder(KW_FORMAT, PARAM_CONSTANT(TID_STRING));
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) {
          case 0:
            assert(false);
            break;
          case 1:
            res.push_back(PARAM_CONSTANT(TID_INT64));
            break;
          case 2:
            res.push_back(PARAM_CONSTANT(TID_STRING));
            break;
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        assert(_parameters.size() >= 1);

        if (_expandedPath.empty()) {
            // Expand once and cache the result (since we're called many times).  Even though we
            // don't actually use _expandedPath here in the logical op, compute it anyway to detect
            // errors early.
            string path =
                evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                         TID_STRING).getString();
            _expandedPath = path::expandForSave(path, *query);
        }

        Parameter instParam;
        if (_parameters.size() >= 2) {
            instParam = _parameters[1];
        }
        if (instParam && findKeyword(KW_INSTANCE)) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                       instParam->getParsingContext())
                << "save" << KW_INSTANCE << 2;
        }

        Parameter fmtParam = findKeyword(KW_FORMAT);
        if (_parameters.size() >= 3) {
            if (fmtParam) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                           fmtParam->getParsingContext())
                    << "save" << KW_FORMAT << 3;
            }
            fmtParam = _parameters[2];
        }
        if (fmtParam) {
            Value v = evaluate(
                ((std::shared_ptr<OperatorParamLogicalExpression>&)fmtParam)->getExpression(),
                TID_STRING);
            string const& format = v.getString();

            if (!format.empty()
                && compareStringsIgnoreCase(format, "auto") != 0
                && !ArrayWriter::isSupportedFormat(format))
            {
                throw  USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                            SCIDB_LE_UNSUPPORTED_FORMAT,
                                            _parameters[2]->getParsingContext())
                    << format;
            }
        }

        return inputSchemas[0];
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSave, "save")


} //namespace
