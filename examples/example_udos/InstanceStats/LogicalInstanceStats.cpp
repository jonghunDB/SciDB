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
 * @file LogicalInstanceStats.cpp
 * An example operator that outputs interesting statistics for a single-attribute array with a double attribute. This
 * is a simple UDO designed to provide an example for reading data from an input array, processing multiple optional
 * parameters, logging, and exchanging messages between instances. Running the operator illustrates how SciDB
 * distributes data between instances. The operator may be extended to become a more general data distribution, size and
 * statistics tool.
 *
 * @brief The operator: instance_stats()
 *
 * @par Synopsis: instance_stats( input_array
 *                                [, log: true/false]
 *                                [, global: true/false] )
 *
 * @par Examples:
 *   <br> instance_stats (my_array, log: true, global: true)
 *   <br> instance_stats (project(big_array, double_attribute), log: true)
 *
 * @par Summary:
 *   <br>
 *   There are 2 optional string "flag" parameters: log, and global. They are all set to false by default.
 *   If log is true, all the  local data from the input array is saved to scidb.log on each instance. If global is true,
 *   the operator returns a single summary for the entire array. Else, it returns a per-instance summary of the data
 *   located on each instance.
 *
 *   Note: if the array has overlaps, the result may or may not include overlaps - an inconsistency in the count()
 *   function that ought to be addressed soon.
 *
 * @par Input: array <attribute:double> [*]
 *
 * @par Output array:
 *   <br> If global is true:
 *   <br> <
 *   <br>   num_chunks: uint64          --the total number of chunks in the array
 *   <br>   num_cells:  uint64          --the total number of cells in the array
 *   <br>   min_cells_per_chunk: uint64 --the number of cells in the smallest chunk (null if num_cells is 0)
 *   <br>   max_cells_per_chunk: uint64 --the number of cells in the largest chunk (null if num_cells is 0)
 *   <br>   avg_cells_per_chunk: double --num_cells divided by num_chunks (null if num_cells is 0)
 *   <br> >
 *   <br> [
 *   <br>   i = 0:0,1,0                 --single cell
 *   <br> ]
 *   <br>
 *   <br> If global is false the values returned are per-instance and the dimension is:
 *   <br> [
 *   <br>   instance_no = 0:INSTANCE_COUNT-1,1,0  --one cell per instance
 *   <br> ]
 *
 * The code assumes familiarity with the concepts described in hello_instances. Consider reading that operator first if
 * you have not already.
 * @see LogicalHelloInstances.cpp
 * @author apoliakov@paradigm4.com
 */

#include <query/Operator.h>

using namespace std;

namespace scidb
{

class LogicalInstanceStats : public LogicalOperator
{
public:
    LogicalInstanceStats(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        /* Tell SciDB we accept one input array. All input arrays must be listed first. */
        ADD_PARAM_INPUT()

        /* Tell SciDB we also accept these optional keyword parameters */
        addKeywordPlaceholder("log", PARAM_CONSTANT(TID_BOOL));
        addKeywordPlaceholder("global", PARAM_CONSTANT(TID_BOOL));
    }

    /**
     * @note all the parameters are assembled in the _parameters member variable
     */
    ArrayDesc inferSchema(vector< ArrayDesc>, std::shared_ptr< Query> query)
    {
        /* Check for a "global: true" parameter. */
        bool global = false;
        Parameter globalParam = findKeyword("global");
        if (globalParam) {
            global = evaluate(
                ((std::shared_ptr<OperatorParamLogicalExpression>&)globalParam)->getExpression(),
                TID_BOOL).getBool();
        }

        /* Make the output schema.
         */
        Attributes outputAttributes;
        outputAttributes.push_back(
            AttributeDesc(0, "num_chunks", TID_UINT64, 0, CompressorType::NONE));
        outputAttributes.push_back(
            AttributeDesc(1, "num_cells",  TID_UINT64, 0, CompressorType::NONE));
        outputAttributes.push_back(
            AttributeDesc(2, "min_cells_per_chunk", TID_UINT64,
                          AttributeDesc::IS_NULLABLE, CompressorType::NONE));
        outputAttributes.push_back(
            AttributeDesc(3, "max_cells_per_chunk", TID_UINT64,
                          AttributeDesc::IS_NULLABLE, CompressorType::NONE));
        outputAttributes.push_back(
            AttributeDesc(4, "avg_cells_per_chunk", TID_DOUBLE,
                          AttributeDesc::IS_NULLABLE, CompressorType::NONE));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        if(global)
        {
            outputDimensions.push_back(DimensionDesc("i", 0, 0, 1, 0));
        }
        else
        {
            outputDimensions.push_back(DimensionDesc("instance_no", 0, query->getInstancesCount(), 1, 0));
        }
        return ArrayDesc("instance_stats", outputAttributes, outputDimensions,
                         defaultPartitioning(),
                         query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalInstanceStats, "instance_stats");

} //namespace scidb
