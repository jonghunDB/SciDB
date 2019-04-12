/*
    teatr df     teatr df     teatr df **
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


#include <memory>
#include <log4cxx/logger.h>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"
#include "Exercise1.h"

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.Exercise1.LogicalExercise1"));

namespace scidb {

    using namespace std;
    /***
    * Helper function to set the dimension start and length properties in the array descriptor.
    * Constructs a new array descriptor with the appropriate dimensions.
    * Output array의 Dimensions과 DimensionDesc를 정의
    * **/
    ArrayDesc setDimensionsiAndAttribute(ArrayDesc desc, Coordinates& lowPos, Coordinates& highPos, std::shared_ptr<Query> const& query)
    {
        Dimensions dims = desc.getDimensions();
        Dimensions newDims(dims.size());

        Attributes outputAttr;
        outputAttr.push_back(AttributeDesc(0, "attributeName", TID_DOUBLE, 0, CompressorType::NONE));
        outputAttr = addEmptyTagAttribute(outputAttr);

        for(size_t i = 0 , n = dims.size(); i< n ;i++) {
            DimensionDesc const &srcDim = dims[i];
            size_t end =(size_t)std::max(highPos[i] - lowPos[i], 0L);
            //각각의 dimension에 DimensionDesc의 결과값을 넘겨준다.
            newDims[i] = DimensionDesc(srcDim.getBaseName(), srcDim.getNamesAndAliases(), 0, 0, end, end,
                                       srcDim.getRawChunkInterval(), srcDim.getChunkOverlap());
        }
            return ArrayDesc(desc.getName(), outputAttr , newDims, createDistribution(psUndefined),desc.getResidency());


    }



/**
 * @brief The operator: Exercise1().
 *
 * @par Synopsis:
 * Exercise1(srcArray, {, lowCoordinates} +{, highCoordinates}, AttributeID)

 * @par Summary:
 * out a result array with array, boundary of subarray window, and attribute id
 *
 * @par Input:
 * srcArray : input array for Exercise1 with boundary of Exercise window.+ Attribute ID.
 * Output : resultArray
 *
 * @par Output array:
 * resultarray
 *
 * @par Examples:

 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 */

/*
 * 우선 작성해야할 함수 3종류
 *
 * std::vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(
 * ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query)
 * REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalExercise1, "Exercise1")
 *
 * */
// Operator.h 함수를 구현
class LogicalExercise1 : public LogicalOperator {
    public:
        LogicalExercise1(const std::string &logicalName, const std::string &alias) :
                LogicalOperator(logicalName, alias) {
            ADD_PARAM_INPUT(); /// input array
            ADD_PARAM_VARIES(); /// additional inputs
        }
        /**
         * @see LogicalOperator::nextVaryParamPlaceholder()
         */
        std::vector<std::shared_ptr<OperatorParamPlaceholder> >
        nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas) {
            LOG4CXX_DEBUG(logger,"nexVaryParamPlaceholder, _parameters.size() : "<<_parameters.size());
            //
            //  The arguments to the Exercise1(...) operator are:
            //     Exercise1( srcArray, startingCell, endingCell, attributeID )
            //      Exercise1( srcArray, low_coord1[,low_coord2,...], high_coord1[,high_coord1,...], attributeID )
            std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
            // vector _parameters : parameter로 구성된 vector, logicalOperator에서 생성. parameter는 OperatorParam 타입으로
            // 생성.
            //array name은 parameters에 들어가지 않는듯 ?
            size_t i = _parameters.size();
            Dimensions const& dims = schemas[0].getDimensions();
            size_t nDims = dims.size();
            // example) 2-D array input, nDims = 2, i = 4 , para0~3 = coord, para4 = attrId
            if( i < nDims*2) {                                  //starting cell, ending cell
                res.push_back(PARAM_CONSTANT(TID_INT64));
            } else if(i == nDims*2) {                           //attribute id
                res.push_back(PARAM_CONSTANT(TID_INT32));
            } else {                                            //add the other parameters
                res.push_back(END_OF_VARIES_PARAMS());
            }
/*            // push_back: std::vector의 멤버함수, vector res의 끝에 추가함.
            if (_parameters.size() < schemas[0].getDImensions().size() * 2) { /// starting cell, ending cell
                res.push_back(PARAM_CONSTANT("int64"));
            } else if (_parameters.size() == schemas[0].getDimensions().size() * 2) { /// attribute id
                res.push_back(PARAM_CONSTANT("int32"));
            } else{
                res.push_back(END_OF_VARIES_PARAMS());
            }*/
            return res;
        }

        /**
         *  @see LogicalOperator::inferSchema()
         */
        ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query) {
            SCIDB_ASSERT(schemas.size() == 1);//한개의 array
            assert(_parameters.size() == 0 || _parameters.size() == schemas[0].getDimensions().size() * 2);

            for(Parameters::const_iterator it = _parameters.begin(); it != _parameters.end(); it++)
            {
                assert(((std::shared_ptr<OperatorParam>&)*it)->getParamType() == PARAM_LOGICAL_EXPRESSION);
                assert(((std::shared_ptr<OperatorParamLogicalExpression>&)*it)->isConstant());
            }



            ArrayDesc& desc = schemas[0];
            Dimensions const& dims = desc.getDimensions();
            size_t nDims = dims.size();

            Dimensions newDims(dims.size());

            //low 와 high coord를 패치한다.
            Coordinates lowPos(nDims);
            Coordinates highPos(nDims);

            // 각각의 dimension에 대해 coord의 starting and end points를 설정한다.
            for(size_t i =0 ; i < nDims; i++) {
                // low coord
                Value const& low = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),TID_INT64);
                if(low.isNull() || low.getInt64() < dims[i].getStartMin()) {
                    lowPos[i] = dims[i].getStartMin();
                } else {
                    lowPos[i] = low.getInt64();
                }
                //high coord
                Value const& high = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i+nDims])->getExpression(),TID_INT64);
                if(high.isNull() || high.getInt64() > dims[i].getEndMax() ) {
                    highPos[i] = dims[i].getEndMax();
                } else {
                    highPos[i] = high.getInt64();
                }
                // 예외 처리. if lowPos is 10 and highPos is 5, insert highPos 9 ? why do this ?
                if(lowPos[i] > highPos[i]) {
                    highPos[i] = lowPos[i] - 1;
                }
            }

            // Attribute의 Vector.
            Attributes outputAttr;
            //Value const& attrID = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[nDims*2])->getExpression(),TID_UINT32);
            AttributeID attributeID = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[nDims * 2])->getExpression()->evaluate().getUint32();
            outputAttr.push_back(AttributeDesc(attributeID, "attributeName", TID_DOUBLE, 0, CompressorType::NONE));
            outputAttr = addEmptyTagAttribute(outputAttr);

            for(size_t i = 0 , n = dims.size(); i< n ;i++) {
                DimensionDesc const &srcDim = dims[i];
                size_t end =(size_t)std::max(highPos[i] - lowPos[i], 0L);
                //각각의 dimension에 DimensionDesc의 결과값을 넘겨준다.
                newDims[i] = DimensionDesc(srcDim.getBaseName(), srcDim.getNamesAndAliases(), 0, 0, end, end,
                                           srcDim.getRawChunkInterval(), srcDim.getChunkOverlap());
            }
            return ArrayDesc(desc.getName(), outputAttr , newDims, createDistribution(psUndefined),desc.getResidency());

            //return setDimensionsiAndAttribute(desc,lowPos,highPos,query);



/*
            /// read input parameters (starting cell, ending cell)
            vector<int64_t> startingCell;
            vector<int64_t> endingCell;
            for (size_t i = 0; i < nDims; i ++) {
                int64_t dimension =
                        evaluate(((std::shared_ptr<OperatorParamLogicalExpression> &) _parameters[i])->getExpression(), TID_INT64).getInt64();
                startingCell.push_back(dimension);
                LOG4CXX_DEBUG(logger,"starting cell : "<<dimension);
            }
            for (size_t i = nDims; i < nDims * 2; i ++) {
                int64_t dimension =
                        evaluate(((std::shared_ptr<OperatorParamLogicalExpression> &) _parameters[i])->getExpression(), TID_INT64).getInt64();
                endingCell.push_back(dimension);
                LOG4CXX_DEBUG(logger,"ending cell : "<<dimension);
            }



            // output array schema 정의-----------------------------------------------------------
            Attributes outputAttributes;
            outputAttributes.push_back(AttributeDesc(0, "attributeName", TID_DOUBLE, 0, CompressorType::NONE));
            outputAttributes = addEmptyTagAttribute(outputAttributes);

            Dimensions outputDimensions;
            for(size_t i = nDims; i < nDims; i++){
                outputDimensions.push_back(DimensionDesc("",+i, startingCell[i], endingCell[i], endingCell[i]-startingCell[i]+1, 0));
            }
            return ArrayDesc("outputArray", outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());*/
            //-----------------------------------------------------------------------------
        }


    };

    REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalExercise1, "Exercise1");

}  // namespace scidb
