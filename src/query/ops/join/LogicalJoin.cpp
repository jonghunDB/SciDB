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

/*
 * LogicalJoin.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <array/Metadata.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: join().
 *
 * @par Synopsis:
 *   join( leftArray, rightArray )
 *
 * @par Summary:
 *   Combines the attributes of two arrays at matching dimension values.
 *   The two arrays must have the same dimension start coordinates, the same chunk size, and the
 *   same chunk overlap.
 *   The join result has the same dimension names as the first input.
 *   The cell in the result array contains the concatenation of the attributes from the two source cells.
 *   If a pair of join dimensions have different lengths, the result array uses the smaller of the two.
 *
 * @par Input:
 *   - leftArray: the left-side source array with leftAttrs and leftDims.
 *   - rightArray: the right-side source array with rightAttrs and rightDims.
 *
 * @par Output array:
 *        <
 *   <br>   leftAttrs + rightAttrs: in case an attribute in rightAttrs conflicts with an attribute in leftAttrs, '_2' will be appended.
 *   <br> >
 *   <br> [
 *   <br>   leftDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - join() is a special case of cross_join() with all pairs of dimensions given.
 *
 */
class LogicalTestJoin: public LogicalOperator
{
  public:
    LogicalTestJoin(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    	ADD_PARAM_INPUT()
    	ADD_PARAM_INPUT()
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        // 2개의 배열을 인풋으로 함
        assert(schemas.size() == 2);
        // 왼쪽, 오른쪽 배열에 대한 정보를 가져온다.
//        @param namespaceName The name of the namespace the array is in
//        * @param arrayName array name
//        * @param attributes vector of attributes
//        * @param dimensions vector of dimensions
//        * @param arrDist array distribution
//        * @param arrRes array residency
//        * @param flags array flags from ArrayDesc::ArrayFlags
        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];
        Attributes const& leftAttributes = leftArrayDesc.getAttributes();
        Attributes const& rightAttributes = rightArrayDesc.getAttributes();

        // The join attributes are the set of attributes formed from the union of the
        // attributes of the two inputs.
        size_t totalAttributes = leftAttributes.size() + rightAttributes.size();
        int nBitmaps = 0;
        //  empty한 cell에 대해 bitmap을 구성하여 메타데이터로 가지고 있음
        nBitmaps += (leftArrayDesc.getEmptyBitmapAttribute() != NULL);
        nBitmaps += (rightArrayDesc.getEmptyBitmapAttribute() != NULL);
        if (nBitmaps == 2) {
            // The attributes for the join output only require one empty bitmap
            // attribute.
            totalAttributes -= 1;
        }
        Attributes joinAttributes(totalAttributes);
        AttributeID j = 0;
        // Add all of the attributes from the first (left) input (except any
        // empty bitmap attribute).
        // 왼쪽 배열로 부터 empty cell은 빼고 joinAttributes[j]에 정보를 저장함
        for (size_t i = 0, n = leftAttributes.size(); i < n; i++) {
            AttributeDesc const& attr = leftAttributes[i];
            if (!attr.isEmptyIndicator()) {
                joinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                    attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                    attr.getDefaultValueExpr());
                joinAttributes[j].addAlias(leftArrayDesc.getName());
                j += 1;
            }
        }
        // Add all the attributes from the second (right) input (including any
        // empty bitmap attribute).
        // The set of join attributes ONLY needs one empty bitmap attribute.
        // Prefer using the one given in the second (right) input attribute set, otherwise
        // use a completely new empty bitmap attribute.
        //오른쪽 배열에서 모든 attribute에 대한 정보를 저장한다.
        for (size_t i = 0, n = rightAttributes.size(); i < n; i++, j++) {
            AttributeDesc const& attr = rightAttributes[i];
            joinAttributes[j] = AttributeDesc(j, attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            joinAttributes[j].addAlias(rightArrayDesc.getName());
        }
        // Add an empty bitmap Attribute if the right input did not have one.
        if (j < totalAttributes) {
            joinAttributes[j] = AttributeDesc(j, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR,
                AttributeDesc::IS_EMPTY_INDICATOR, CompressorType::NONE);
        }

        // The exemplar schema is the left-most Non-autochunked schema. It will
        // be used to define the needed chunkInterval.
        // left가 0 right이 1, 둘다 오토청크면 right을 exemplar으로..
        size_t exemplarIndex = 0;
        size_t targetIndex = 1;
        if (leftArrayDesc.isAutochunked()) {
            if (rightArrayDesc.isAutochunked()) {
                // Only one input may be autochunked
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ALL_INPUTS_AUTOCHUNKED)
                    << getLogicalName();
            }
            // The left input is autochunked, so the exemplar is the right
            // schema (index 1)
            exemplarIndex = 1;
            targetIndex = 0;
        }
        // 두 배열의 dimension 개수가 같은지 확인
        Dimensions const& exemplarDimensions = schemas[exemplarIndex].getDimensions();
        Dimensions const& targetDimensions = schemas[targetIndex].getDimensions();
        // Check that the two inputs have the same number of dimensions.
        if(exemplarDimensions.size() != targetDimensions.size())
        {
            ostringstream exemplar, target;
            printDimNames(exemplar, exemplarDimensions);
            printDimNames(target, targetDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << getLogicalName() << exemplar.str() << target.str();
        }
        // 여기서 각 배열의 dimension에서 시작하는 지점이 같은지 확인한다.
        // Check that the corresponding dimensions in the two inputs have the
        // same starting index (startMin). Report any and all mismatches.
        // 모범 Dimension?
        ostringstream ss;

        int mismatches = 0;
        for (size_t i = 0, n = exemplarDimensions.size(); i < n; i++)
        {
            if(exemplarDimensions[i].getStartMin() != targetDimensions[i].getStartMin())
            {
                if (mismatches++) {
                    ss << ", ";
                }
                ss << '[' << exemplarDimensions[i] << "] != [" << targetDimensions[i] << ']';
            }
        }
        if (mismatches)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH) << ss.str();
        }
        //Dimension의  수를 비교하여 더 작은 dimension으로 설정.
        Dimensions joinDimensions;
        for (size_t i = 0, n = exemplarDimensions.size(); i < n; i++)
        {
            assert(exemplarDimensions[i].getStartMin() == targetDimensions[i].getStartMin());
            DimensionDesc const& exemplarDim = exemplarDimensions[i];
            DimensionDesc const& targetDim = targetDimensions[i];
            // The names of the dimensions in the output are defined by the names
            // of the first (left) schema, which is not necessarily the exemplarDim.
            //왼쪽 dimension의 이름을 따름..
            DimensionDesc const& leftDim = (exemplarIndex == 0
                                            ? exemplarDim
                                            : targetDim);
            joinDimensions.push_back(
                DimensionDesc(
                        //이름은  왼쪽껄로
                    leftDim.getBaseName(),
                    leftDim.getNamesAndAliases(),
                    //실제 dimension정보는 exemplardim을 따름
                    exemplarDim.getStartMin(),
                    max(exemplarDim.getCurrStart(), targetDim.getCurrStart()),
                    min(exemplarDim.getCurrEnd(), targetDim.getCurrEnd()),
                    min(exemplarDim.getEndMax(), targetDim.getEndMax()),
                    exemplarDim.getChunkInterval(),
                    min(exemplarDim.getChunkOverlap(), targetDim.getChunkOverlap())
                    )
                );
            joinDimensions[i].addAlias(leftArrayDesc.getName());

            for (const ObjectNames::NamesPairType& rDimName : targetDimensions[i].getNamesAndAliases()) {
                for (const string& alias : rDimName.second) {
                   joinDimensions[i].addAlias(alias, rDimName.first);
                }
            }
        }
        return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(),
                         joinAttributes,
                         joinDimensions,
                         createDistribution(psUndefined), // Distribution is unknown until the physical stage.
                         query->getDefaultArrayResidency() );
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalJoin, "join")


} //namespace
