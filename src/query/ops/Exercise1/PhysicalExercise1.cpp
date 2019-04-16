/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2015 SciDB, Inc.
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

#include <util/Network.h>
#include "query/Operator.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "Exercise1.h"
#include <sstream>
#include <iostream>
#include <fstream>
#include <stdio.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.Exercise1.PhysicalExercise1"));

namespace scidb {

    using namespace boost;
    using namespace std;

    class PhysicalExercise1: public  PhysicalOperator
    {
        // PhysicalExercise1 and execute
    private:

    public:
        PhysicalExercise1(const std::string& logicalName, const std::string& physicalName,
                       const Parameters& parameters, const ArrayDesc& schema)
                : PhysicalOperator(logicalName, physicalName, parameters, schema)
        {
            LOG4CXX_DEBUG(logger,"PhysicalExercise1::PhysicalExercise1 called");
        }
        //Return the starting coordinates of the subarray window, relative to the input schema
        inline Coordinates getWindowStart(ArrayDesc const& inputSchema) const
        {
            Dimensions const& dims = inputSchema.getDimensions();
            size_t nDims = dims.size();
            Coordinates result (nDims);
            for (size_t i = 0; i < nDims; i++)
            {
                Value const& low = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate();
                if ( low.isNull() || low.getInt64() < dims[i].getStartMin())
                {
                    result[i] = dims[i].getStartMin();
                }
                else
                {
                    result[i] = low.getInt64();
                }
            }
            return result;
        }

        //Return the ending coordinates of the subarray window, relative to the input schema
        inline Coordinates getWindowEnd(ArrayDesc const& inputSchema) const
        {
            Dimensions const& dims = inputSchema.getDimensions();
            size_t nDims = dims.size();
            Coordinates result (nDims);
            for (size_t i  = 0; i < nDims; i++)
            {
                Value const& high = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i + nDims])->getExpression()->evaluate();
                if (high.isNull() || high.getInt64() > dims[i].getEndMax())
                {
                    result[i] = dims[i].getEndMax();
                }
                else
                {
                    result[i] = high.getInt64();
                }
            }
            return result;
        }


        std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query) override
        {
            LOG4CXX_DEBUG(logger,"PhysicalExercise1::execute called");
            SCIDB_ASSERT(inputArrays.size() == 1); // or assert(inputArrays.size() ==1);
            SCIDB_ASSERT(_schema.getResidency()->isEqual(inputArrays[0]->getArrayDesc().getResidency()));
            //autochunk interval이면 그 interval을 instance에서 확인하고 설정한다.
            LOG4CXX_DEBUG(logger,"Check or update Intervals");
            checkOrUpdateIntervals(_schema, inputArrays[0]);

            LOG4CXX_DEBUG(logger,"input array and desc");
            //random access으로 Iterator
            std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
            ArrayDesc const& inDesc = inputArray->getArrayDesc();

            LOG4CXX_DEBUG(logger,"input array dimensions");
            Dimensions const& srcDims = inDesc.getDimensions();
            size_t nDims = srcDims.size();

            LOG4CXX_DEBUG(logger,"for output array  attribute, we choose one of the input array's attribute");
            Attributes outputAttr;
            AttributeID attributeID = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[nDims * 2])->getExpression()->evaluate().getUint32();
            outputAttr.push_back(AttributeDesc( attributeID, "attributeName", TID_DOUBLE, 0, CompressorType::NONE));
            outputAttr = addEmptyTagAttribute(outputAttr);

            /*// input AttrID and low and high position of subarray
            Coordinates lowPos = getWindowStart(inDesc);
            Coordinates highPos = getWindowEnd(inDesc);

            //low가 high보다 클 경우 empty array를 반환한다.
            for(size_t i=0; i<nDims; i++)
            {
                if (lowPos[i] > highPos[i]) {
                    return std::shared_ptr<Array>(new MemArray(_schema,query));
                }

            }*/

            LOG4CXX_DEBUG(logger,"set starting cell and ending cell about subarray window ");
            vector<int64_t> startingCell;
            vector<int64_t> endingCell;
            vector<double> outputs;

            LOG4CXX_DEBUG(logger,"make startingcell");
            for (size_t i = 0; i < nDims; i ++) {
                int64_t dimension =
                        ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64();
                startingCell.push_back(dimension);
                LOG4CXX_DEBUG(logger,"starting cell : "<<dimension);
            }
            LOG4CXX_DEBUG(logger,"make endingcell ");
            for (size_t i = nDims; i < nDims * 2; i ++) {
                int64_t dimension =
                        ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64();
                endingCell.push_back(dimension);
                LOG4CXX_DEBUG(logger,"ending cell : "<<dimension);
            }


            std::shared_ptr<ConstArrayIterator> arrayIterator = inputArray->getConstIterator(attributeID);
            LOG4CXX_DEBUG(logger,"array Iterator start");



            while (!arrayIterator->end())
            {//현재 instance가 가지고 있는 chunk들을 순회(현재는 전체 array가 하나의 chunk로 구성되어 있음)
                ConstChunk const &chunk = arrayIterator->getChunk();//arrayIterator로부터 chunk를 읽어옴
                LOG4CXX_DEBUG(logger,"Chunk size :"<< chunk.getSize());
                if(chunk.contains(startingCell,false))
                {
                    std::shared_ptr<ConstChunkIterator> chunkIterator = chunk.getConstIterator();
                    chunkIterator->setPosition(startingCell);

                    while(!chunkIterator->end() || (chunkIterator->getPosition() < endingCell) ) {
                        double cell = chunkIterator->getItem().getDouble();//read cell
                        outputs.push_back(cell);
                        LOG4CXX_DEBUG(logger, "contain cells value : " << cell << "   position" << chunkIterator->getPosition());
                        ++(*chunkIterator);
                    }
                }
                ++(*arrayIterator);
            }
            LOG4CXX_DEBUG(logger,"make output attributes complete");

            /*결과 반환*/
            if (query->getInstanceID() == 0)
            {
                LOG4CXX_DEBUG(logger,"Exercise1Array start ");
                return Exercise1Array(startingCell,endingCell,outputs,query);
            }
            else
            {
                /* Instance 0번이 아닐 경우 empty array 반환*/
                LOG4CXX_DEBUG(logger,"Exercise1Array start ");
                return std::shared_ptr<Array>(new MemArray(_schema, query));
            }
        }

        std::shared_ptr<Array> Exercise1Array
            (Coordinates startingCell,Coordinates endingCell,vector<double> attributeValues,std::shared_ptr<Query>& query){
            LOG4CXX_INFO(logger,"Exercise1Array called");
            std::shared_ptr<Array> outputArray(new MemArray(_schema, query));
            //Coordinates startingPosition(1, query->getInstanceID());

            //output attribute
            std::shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
            LOG4CXX_INFO(logger,"ArrayIterator outputArrayIter start");
            std::shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(startingCell).getIterator(query,ChunkIterator::SEQUENTIAL_WRITE);
            LOG4CXX_INFO(logger,"ChunkIterator outputChunkIter start");
            outputChunkIter->setPosition(startingCell);


            while(!outputArrayIter->end()){
                while(!outputChunkIter->end()){

                    for(size_t i = 0; i <attributeValues.size() ;i++)
                    {
                        Value value;
                        value.setDouble(attributeValues[i]);
                        outputChunkIter->writeItem(value);
                        ++(*outputChunkIter);
                    }
                    outputChunkIter->flush();
                }
            }
            //"Example" of writing cells

         /*   Value value;
            value.setDouble(attributeValues[0]);
            outputChunkIter->writeItem(value);

            ++(*outputChunkIter); // move to the next cell

            value.setDouble(attributeValues[1]);
            outputChunkIter->writeItem(value);

            outputChunkIter->flush(); // After completing a chunk, you have to flush.*/

            LOG4CXX_INFO(logger,"Exercise1Array finished");

            return outputArray;
        }
    };

    REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalExercise1, "Exercise1", "physicalExercise1");

}  // namespace scidb
