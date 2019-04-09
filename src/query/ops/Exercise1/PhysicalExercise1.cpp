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
    private:

    public:

        PhysicalExercise1(const std::string& logicalName, const std::string& physicalName,
                       const Parameters& parameters, const ArrayDesc& schema)
                : PhysicalOperator(logicalName, physicalName, parameters, schema)
        {
            LOG4CXX_DEBUG(logger,"PhysicalExercise1::PhysicalExercise1 called");
        }

        std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query) override
        {
            //LOG4CXX_DEBUG(logger,"PhysicalExercise1::execute called");
            SCIDB_ASSERT(inputArrays.size() == 1);

            std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
            ArrayDesc const& inDesc = inputArray->getArrayDesc();

            AttributeID inputAttrID;
            size_t nDims = inDesc.getDimensions().size();

            vector<int64_t> startingCell;
            vector<int64_t> endingCell;
            vector<double> outputs;
            /**
             * Get parameters
             */
            for (size_t i = 0; i < nDims; i ++) {
                int64_t dimension =
                        ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64();
                startingCell.push_back(dimension);
                LOG4CXX_DEBUG(logger,"starting cell : "<<dimension);
            }
            for (size_t i = nDims; i < nDims * 2; i ++) {
                int64_t dimension =
                        ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64();
                endingCell.push_back(dimension);
                LOG4CXX_DEBUG(logger,"ending cell : "<<dimension);
            }
            inputAttrID =
                    ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[nDims * 2])->getExpression()->evaluate().getUint32();

            std::shared_ptr<ConstArrayIterator> arrayIterator = inputArray->getConstIterator(inputAttrID);
            while (!arrayIterator->end()) {//현재 instance가 가지고 있는 chunk들을 순회(현재는 전체 array가 하나의 chunk로 구성되어 있음)

                ConstChunk const &chunk = arrayIterator->getChunk();//arrayIterator로부터 chunk를 읽어옴

                std::shared_ptr<ConstChunkIterator> chunkIterator = chunk.getConstIterator();

                while(!chunkIterator->end()){//cell 순회
                    double cell = chunkIterator->getItem().getDouble();//read cell

                    ++(*chunkIterator);
                }

                ++(*arrayIterator);
            }


            /**
             * 결과 반환
             */
            if (query->getInstanceID() == 0)
            {
                /**
                 * Instance 0번일 경우 local top-k들 중에서 최종적으로 top-k를 선택함.
                 */
                return makeFinalTopKArray(startingCell,endingCell,outputs,query);
            }
            else
            {
                /**
                 * Instance 0번이 아닐 경우 empty array 반환
                 */
                return std::shared_ptr<Array>(new MemArray(_schema, query));
            }
        }


        std::shared_ptr<Array> makeFinalTopKArray
                (Coordinates startingCell,Coordinates endingCell,vector<double> attributeValues,std::shared_ptr<Query>& query){
            LOG4CXX_INFO(logger,"makeFinalTopKArray called");
            std::shared_ptr<Array> outputArray(new MemArray(_schema, query));
            //Coordinates startingPosition(1, query->getInstanceID());

            //output attribute
            std::shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
            std::shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(startingCell).getIterator(query,ChunkIterator::SEQUENTIAL_WRITE);
            outputChunkIter->setPosition(startingCell);

            //"Example" of writing cells
            Value value;
            value.setDouble(attributeValues[0]);
            outputChunkIter->writeItem(value);

            ++(*outputChunkIter); // move to the next cell

            value.setDouble(attributeValues[1]);
            outputChunkIter->writeItem(value);

            outputChunkIter->flush(); // After completing a chunk, you have to flush.

            LOG4CXX_INFO(logger,"makeFinalTopKArray finished");

            return outputArray;
        }
    };

    REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalExercise1, "Exercise1", "physicalExercise1");

}  // namespace scidb
