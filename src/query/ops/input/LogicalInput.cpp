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
 * @file LogicalInput.cpp
 * @author roman.simakov@gmail.com
 * @brief Input operator for reading data from external files.
 */

#include "LogicalInput.h"

#include "ChunkLoader.h"
#include "InputArray.h"

#include <array/Dense1MChunkEstimator.h>
#include <query/Operator.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>
#include <system/Cluster.h>
#include <system/Resources.h>
#include <system/Warnings.h>

#include <log4cxx/logger.h>

using namespace std;
namespace bfs = boost::filesystem;

static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.input"));

namespace scidb
{

LogicalInput::LogicalInput(const string& logicalName, const string& alias)
  : LogicalOperator(logicalName, alias)
{
    ADD_PARAM_SCHEMA();   //0
    ADD_PARAM_CONSTANT(TID_STRING);//1
    ADD_PARAM_VARIES();          //2

    addKeywordPlaceholder(InputSettings::KW_INSTANCE, PARAM_CONSTANT(TID_INT64));
    addKeywordPlaceholder(InputSettings::KW_FORMAT, PARAM_CONSTANT(TID_STRING));
    addKeywordPlaceholder(InputSettings::KW_MAX_ERRORS, PARAM_CONSTANT(TID_INT64));
    addKeywordPlaceholder(InputSettings::KW_STRICT, PARAM_CONSTANT(TID_BOOL));
}

Placeholders LogicalInput::nextVaryParamPlaceholder(const vector< ArrayDesc> &schemas)
{
    Placeholders res;
    res.reserve(2);
    res.push_back(END_OF_VARIES_PARAMS());
    switch (_parameters.size()) {
      case 0:
      case 1:
        assert(false);
        break;
      case 2:
        // instance_id
        res.push_back(PARAM_CONSTANT(TID_INT64));
        break;
      case 3:
        // format
        res.push_back(PARAM_CONSTANT(TID_STRING));
        break;
      case 4:
        // maxErrors
        res.push_back(PARAM_CONSTANT(TID_INT64));
        break;
      case 5:
        // isStrict
        res.push_back(PARAM_CONSTANT(TID_BOOL));
        break;
      default:
        // Translator will see END_OF_VARIES_PARAMS() and report the
        // "too many arguments" error.
        break;
    }
    return res;
}

ArrayDesc LogicalInput::inferSchema(vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query)
{
    assert(inputSchemas.size() == 0);

    _settings.parse(_parameters, _kwParameters, *query);

    InstanceID instanceID = _settings.getInstanceId();
    if (instanceID == ALL_INSTANCE_MASK)
    {
        /* Let's support it: lets each instance assign unique coordiantes to its chunks based on
        * distribution function.  It is based on two assumptions:
        * - coordinates are not iportant (as in SQL)
        * - there can be holes in array
        *
        *           if (format[0] == '(') { // binary template loader
        *               throw  USER_QUERY_EXCEPTION(
        *                   SCIDB_SE_INFER_SCHEMA,
        *                   SCIDB_LE_INVALID_INSTANCE_ID,
        *                   _parameters[2]->getParsingContext())
        *                       << "-1 can not be used for binary template loader";
        *           }
        */
        //Distributed loading let's check file existence on all instances
        map<InstanceID, bool> instancesMap;
        Resources::getInstance()->fileExists(_settings.getPath(), instancesMap, query);

        bool fileDetected = false;
        vector<InstanceID> instancesWithoutFile;
        for (map<InstanceID, bool>::const_iterator it = instancesMap.begin(); it != instancesMap.end(); ++it)
        {
            if (it->second)
            {
                if (!fileDetected)
                    fileDetected = true;
            }
            else
            {
                //Remembering file name on each missing file
                LOG4CXX_WARN(oplogger, "File '" << _settings.getPath()
                             << "' not found on instance #" << it->first);
                instancesWithoutFile.push_back(it->first);
            }
        }

        //Such file not found on any instance. Failing with exception
        if (!fileDetected)
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << _settings.getPath();
        }

        //If some instances missing this file posting appropriate warning
        if (instancesWithoutFile.size())
        {
            stringstream instancesList;
            for (size_t i = 0, count = instancesWithoutFile.size();  i < count; ++i)
            {
                instancesList << instancesWithoutFile[i] << (i == count - 1 ? "" : ", ");
            }
            LOG4CXX_WARN(oplogger, "File " << _settings.getPath()
                         << " not found on instances " << instancesList.str());
            query->postWarning(SCIDB_WARNING(SCIDB_LE_FILE_NOT_FOUND_ON_INSTANCES)
                               << _settings.getPath() << instancesList.str());
        }

        // Even if the chunks are distributed perfectly across the instances, there is no guarantee that
        // the distribution will be level (for example, the HASHED distribution).
        // Therefore, the array size cannot be bounded ... instances need to keep moving beyond the dimensions
        // that would be filled densely and skip forward an arbitrary amount to find another chunk that maps
        // to that instance.  Therefore, the schema for a parallel load must be unbounded in the first
        // (slowest-changing) dimension.
        //
        // The exception is the 'opaque' format, which contains embedded chunk positions and does
        // not rely on ChunkLoader::nextImplicitChunkPosition() to navigate the array dimensions.
        // For opaque parallel loads we can skip the check, since an optimizer-inserted SG will put
        // things right.
        //
        bool isOpaque = !compareStringsIgnoreCase(_settings.getFormat(), "opaque");
        if (!isOpaque) {
            const ArrayDesc& arrayDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
            Dimensions const& dims = arrayDesc.getDimensions();
            if(!dims[0].isMaxStar()) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INPUT_PLOAD_BOUNDED,
                _parameters[0]->getParsingContext());
            }
        }
    }
    else if (instanceID == COORDINATOR_INSTANCE_MASK)
    {
        //This is loading from local instance. Throw error if file not found.
        // XXX Can someone explain what's the deal with the at-sign??
        string const& path = _settings.getPath();
        if (path.find('@') == string::npos && !Resources::getInstance()->fileExists(path, query->getInstanceID(), query))
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << bfs::absolute(path);
        }
    }
    else
    {
        // convert from physical to logical
        instanceID = query->mapPhysicalToLogical(instanceID);

        //This is loading from single instance. Throw error if file not found.
        string const& path = _settings.getPath();
        if (!Resources::getInstance()->fileExists(path, instanceID, query))
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << bfs::absolute(path);
        }
    }

    ArrayDesc arrayDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
    if (compareStringsIgnoreCase(_settings.getFormat(), "opaque")) {
        // Only estimate for non-opaque input, since opaque input
        // encodes the dimension parameters.
        Dense1MChunkEstimator::estimate(arrayDesc.getDimensions());
    } else if (arrayDesc.isAutochunked()) {
        // For opaque input, autochunked schemas are resolved by
        // peeking at the input file.
        OpaqueChunkLoader::reconcileSchema(arrayDesc, _settings.getPath());
    }
    Dimensions const& srcDims = arrayDesc.getDimensions();

    //Use array name from catalog if possible or generate temporary name
    string inputArrayName = arrayDesc.getName();
    PartitioningSchema partitioningSchema = psUninitialized;
    if (arrayDesc.getDistribution()) {
        partitioningSchema = arrayDesc.getDistribution()->getPartitioningSchema();
    }

    ArrayDistPtr dist;
    if (instanceID != ALL_INSTANCE_MASK) {
        // loading from a single file/instance as in
        // load(ARRAY, 'file.x', -2) or input(<XXX>[YYY], 'file.x', 0)
        partitioningSchema = psLocalInstance;
        dist = std::make_shared<LocalArrayDistribution>((instanceID == COORDINATOR_INSTANCE_MASK) ?
                                                        query->getInstanceID() :
                                                        instanceID);
    } else if (partitioningSchema == psUninitialized) {
        // in-line schema currently does not provide a distribution, i.e.
        // input(<XXX>[YYY], 'file.x')
        dist = createDistribution(psUndefined);
    } else {
        // the user-specified schema will be used for generating the implicit coordinates.
        // NOTICE that the optimizer will still be told psUndefined for any parallel ingest
        // (e.g.  load(ARRAY, 'file.x', -1, ...)
        // by PhysicalInput::getOutputDistribution() because some input formats (e.g. opaque, text)
        // may specify the data coordinates (in any distribution).
        dist = arrayDesc.getDistribution();
    }
    if (inputArrayName.empty())
    {
        inputArrayName = "tmp_input_array";
    }

    SCIDB_ASSERT(dist);

    return ArrayDesc(inputArrayName,
                     arrayDesc.getAttributes(),
                     srcDims,
                     dist,
                     query->getDefaultArrayResidency(),
                     arrayDesc.getFlags());
}

void LogicalInput::inferAccess(std::shared_ptr<Query>& query)
{
    LogicalOperator::inferAccess(query);
    _settings.parse(_parameters, _kwParameters, *query);
}

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalInput, "input")

} //namespace
