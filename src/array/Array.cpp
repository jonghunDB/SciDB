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
 * @file Array.cpp
 *
 * @brief class Array
 */

#include <vector>
#include <string.h>

#include <log4cxx/logger.h>

#include <array/Array.h>
#include <array/ConstArrayIterator.h>
#include <array/ArrayIterator.h>
#include <array/ConstItemIterator.h>
#include <array/ConstChunk.h>
#include <array/ChunkIterator.h>

#include <query/TypeSystem.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.Array"));

    std::string const& Array::getName() const
    {
        return getArrayDesc().getName();
    }

    ArrayID Array::getHandle() const
    {
        return getArrayDesc().getId();
    }

    void Array::appendHorizontal(const std::shared_ptr<Array>& input,
                                 CoordinateSet* newChunkCoordinates) {
        append(input, false, newChunkCoordinates);
    }

    void Array::appendVertical(const std::shared_ptr<Array>& input,
                               CoordinateSet* newChunkCoordinates) {
        append(input, true, newChunkCoordinates);
    }

    // private implementation
    // with luck, will become appendHorizontal and appendVertical
    // will be eliminated
    void Array::append(const std::shared_ptr<Array>& input,
                       bool const vertical,
                       CoordinateSet* newChunkCoordinates)
    {
        if (vertical) {
            LOG4CXX_WARN(logger, "Array::append() vertical mode is deprecated");
            assert(input->getSupportedAccess() >= MULTI_PASS);
            for (AttributeID i = 0,
                     attribTotal = safe_static_cast<AttributeID>(getArrayDesc().getAttributes().size());
                 i < attribTotal; i++) {
                std::shared_ptr<ArrayIterator> dst = getIterator(i);
                std::shared_ptr<ConstArrayIterator> src = input->getConstIterator(i);
                while (!src->end())
                {
                    if(newChunkCoordinates && i == 0)
                    {
                        newChunkCoordinates->insert(src->getPosition());
                    }
                    dst->copyChunk(src->getChunk());
                    ++(*src);
                }
            }
        }
        else
        {
            size_t nAttrs = getArrayDesc().getAttributes().size();
            std::vector< std::shared_ptr<ArrayIterator> > dstIterators(nAttrs);
            std::vector< std::shared_ptr<ConstArrayIterator> > srcIterators(nAttrs);
            for (size_t i = 0; i < nAttrs; i++)
            {
                AttributeID aid = safe_static_cast<AttributeID>(i);
                dstIterators[i] = getIterator(aid);
                srcIterators[i] = input->getConstIterator(aid);
            }
            while (!srcIterators[0]->end())
            {
                if(newChunkCoordinates)
                {
                    newChunkCoordinates->insert(srcIterators[0]->getPosition());
                }
                for (size_t i = 0; i < nAttrs; i++)
                {
                    std::shared_ptr<ArrayIterator>& dst = dstIterators[i];
                    std::shared_ptr<ConstArrayIterator>& src = srcIterators[i];
                    dst->copyChunk(src->getChunk());
                    ++(*src);
                }
            }
        }
    }

    std::shared_ptr<CoordinateSet> Array::getChunkPositions() const
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << "calling getChunkPositions on an invalid array";
    }

    std::shared_ptr<CoordinateSet> Array::findChunkPositions() const
    {
        if (hasChunkPositions())
        {
            return getChunkPositions();
        }
        if (getSupportedAccess() == SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << "findChunkPositions";
        }
        //Heuristic: make an effort to scan the empty tag. If there is no empty tag - scan the smallest fixed-sized attribute.
        //If an attribute is small in size (bool or uint8), chances are it takes less disk scan time and/or less compute time to pass over it.
        ArrayDesc const& schema = getArrayDesc();
        AttributeDesc const* attributeToScan = schema.getEmptyBitmapAttribute();
        if (attributeToScan == NULL)
        {
            //The array doesn't have an empty tag. Let's pick the smallest fixed-size attribute.
            attributeToScan = &schema.getAttributes()[0];
            size_t scannedAttributeSize = attributeToScan->getSize();
            for(size_t i = 1, n = schema.getAttributes().size(); i < n; i++)
            {
                AttributeDesc const& candidate = schema.getAttributes()[i];
                size_t candidateSize = candidate.getSize();
                if ( candidateSize != 0 && (candidateSize < scannedAttributeSize || scannedAttributeSize == 0))
                {
                    attributeToScan = &candidate;
                    scannedAttributeSize = candidateSize;
                }
            }
        }
        assert(attributeToScan != NULL);
        AttributeID victimId = attributeToScan->getId();
        std::shared_ptr<CoordinateSet> result(new CoordinateSet());
        //Iterate over the target attribute, find the position of each chunk, add all chunk positions to result
        std::shared_ptr<ConstArrayIterator> iter = getConstIterator(victimId);
        while( ! iter->end() )
        {
            result->insert(iter->getPosition());
            ++(*iter);
        }
        return result;
    }

    size_t Array::extractData(AttributeID attrID, void* buf,
                              Coordinates const& first, Coordinates const& last,
                              Array::extractInit_t init,
                              extractNull_t null) const
    {
        ArrayDesc const& arrDesc = getArrayDesc();
        AttributeDesc const& attrDesc = arrDesc.getAttributes()[attrID];
        Type attrType( TypeLibrary::getType(attrDesc.getType()));
        Dimensions const& dims = arrDesc.getDimensions();
        size_t nDims = dims.size();
        if (attrType.variableSize()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_EXTRACT_EXPECTED_FIXED_SIZE_ATTRIBUTE);
        }

        if (attrType.bitSize() < 8) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_EXTRACT_UNEXPECTED_BOOLEAN_ATTRIBUTE);
        }

        if (first.size() != nDims || last.size() != nDims) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                 SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);
        }

        size_t bufSize = 1;
        for (size_t j = 0; j < nDims; j++) {
            if (last[j] < first[j] ||
                (first[j] - dims[j].getStartMin()) % dims[j].getChunkInterval() != 0) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNALIGNED_COORDINATES)
                    << dims[j];
            }
            bufSize *= last[j] - first[j] + 1;
        }

        size_t attrSize = attrType.byteSize();

        switch(init) {
        case EXTRACT_INIT_ZERO:
            memset(buf, 0, bufSize*attrSize);
            break;
        case EXTRACT_INIT_NAN:
            {
                TypeEnum typeEnum = typeId2TypeEnum(attrType.typeId());
                if (typeEnum == TE_FLOAT) {
                    float * bufFlt = reinterpret_cast<float*>(buf);
                    std::fill(bufFlt, bufFlt+bufSize, NAN);
                } else if (typeEnum == TE_DOUBLE) {
                    double * bufDbl = reinterpret_cast<double*>(buf);
                    std::fill(bufDbl, bufDbl+bufSize, NAN);
                } else {
                    assert(false); // there is no such thing as NAN for these types.  The calling programmer made a serious error.
                    SCIDB_UNREACHABLE();
                }
            }
            break;
        default:
            SCIDB_UNREACHABLE(); // all cases should have been enumerated.;
        }

        size_t nExtracted = 0;
        for (std::shared_ptr<ConstArrayIterator> i = getConstIterator(attrID);
             !i->end(); ++(*i)) {
            size_t j, chunkOffs = 0;
            ConstChunk const& chunk = i->getChunk();
            Coordinates const& chunkPos = i->getPosition();
            for (j = 0; j < nDims; j++) {
                if (chunkPos[j] < first[j] || chunkPos[j] > last[j]) {
                    break;
                }
                chunkOffs *= last[j] - first[j] + 1;
                chunkOffs += chunkPos[j] - first[j];
            }
            if (j == nDims) {
                for (std::shared_ptr<ConstChunkIterator> ci =
                         chunk.getConstIterator(ChunkIterator::IGNORE_OVERLAPS |
                                                ChunkIterator::IGNORE_EMPTY_CELLS |
                                                ChunkIterator::IGNORE_NULL_VALUES);
                     !ci->end(); ++(*ci)) {
                    Value const& v = ci->getItem();
                    if (!v.isNull()) {
                        Coordinates const& itemPos = ci->getPosition();
                        size_t itemOffs = 0;
                        for (j = 0; j < nDims; j++) {
                            itemOffs *= last[j] - first[j] + 1;
                            itemOffs += itemPos[j] - first[j];
                        }
                        memcpy((char*)buf + itemOffs*attrSize,
                               ci->getItem().data(), attrSize);
                    } else if (null==EXTRACT_NULL_AS_NAN) {
                        Coordinates const& itemPos = ci->getPosition();
                        size_t itemOffs = 0;
                        for (j = 0; j < nDims; j++) {
                            itemOffs *= last[j] - first[j] + 1;
                            itemOffs += itemPos[j] - first[j];
                        }
                        TypeEnum typeEnum = typeId2TypeEnum(attrType.typeId());
                        // historically, no alignment guarantee on buf
                        char * itemAddr = (char*)buf + itemOffs*attrSize;
                        if (typeEnum == TE_FLOAT) {
                            float nan=NAN;
                            std::copy((char*)&nan, (char*)&nan + sizeof(nan), itemAddr);
                        } else if (typeEnum == TE_DOUBLE) {
                            double nan=NAN;
                            std::copy((char*)&nan, (char*)&nan + sizeof(nan), itemAddr);
                        } else {
                            SCIDB_UNREACHABLE(); // there is no such thing as NaN for other types.  The calling programmer made a serious error.
                        }
                    } else { // EXTRACT_NULL_AS_EXCEPTION
                        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "NULL to non-nullable operator";
                    }
                }
                nExtracted += 1;
            }
        }
        return nExtracted;
    }

    std::shared_ptr<ArrayIterator> Array::getIterator(AttributeID attr)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Array::getIterator";
    }


    std::shared_ptr<ConstItemIterator> Array::getItemIterator(AttributeID attrID, int iterationMode) const
    {
        return std::shared_ptr<ConstItemIterator>(new ConstItemIterator(*this, attrID, iterationMode));
    }

    bool Array::isCountKnown() const
    {
        // if an array keeps track of the count in substantially less time than traversing
        // all chunks of an attribute, then that array should override this method
        // and return true, and override count() [below] with a faster method.
        return false;
    }

    size_t Array::count() const
    {
        // if there is a way to get the count in O(1) time, or without traversing
        // a set of chunks, then that should be done here, when that becomes possible

        if (getSupportedAccess() == SINGLE_PASS)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNSUPPORTED_INPUT_ARRAY) << "findChunkElements";
        }

        //Heuristic: make an effort to scan the empty tag. If there is no empty tag - scan the smallest fixed-sized attribute.
        //If an attribute is small in size (bool or uint8), chances are it takes less disk scan time and/or less compute time to pass over it.
        ArrayDesc const& schema = getArrayDesc();
        AttributeDesc const* attributeToScan = schema.getEmptyBitmapAttribute();
        if (attributeToScan == NULL)
        {
            //The array doesn't have an empty tag. Let's pick the smallest fixed-size attribute.
            attributeToScan = &schema.getAttributes()[0];
            size_t scannedAttributeSize = attributeToScan->getSize();
            for(size_t i = 1, n = schema.getAttributes().size(); i < n; i++)
            {
                AttributeDesc const& candidate = schema.getAttributes()[i];
                size_t candidateSize = candidate.getSize();
                if ( candidateSize != 0 && (candidateSize < scannedAttributeSize || scannedAttributeSize == 0))
                {
                    attributeToScan = &candidate;
                    scannedAttributeSize = candidateSize;
                }
            }
        }
        assert(attributeToScan != NULL);
        AttributeID victimId = attributeToScan->getId();
        size_t result = 0;
        //Iterate over the target attribute, find nElements of each chunk, add such values to the result
        std::shared_ptr<ConstArrayIterator> iter = getConstIterator(victimId);
        while( ! iter->end() )
        {
            ConstChunk const& curChunk = iter->getChunk();
            result += curChunk.count();
            ++(*iter);
        }
        return result;
    }


    void Array::printArrayToLogger() const
    {
        // This function is only usable in debug builds, otherwise it is a no-op
#ifndef NDEBUG
        AttributeID nattrs = safe_static_cast<AttributeID>(
            this->getArrayDesc().getAttributes(true).size());

        vector< std::shared_ptr<ConstArrayIterator> > arrayIters(nattrs);
        vector< std::shared_ptr<ConstChunkIterator> > chunkIters(nattrs);
        vector<TypeId> attrTypes(nattrs);

        LOG4CXX_DEBUG(logger, "[printArray] name (" << this->getName() << ")");

        for (AttributeID i = 0; i < nattrs; i++)
        {
            arrayIters[i] = this->getConstIterator(i);
            attrTypes[i] = this->getArrayDesc().getAttributes(true)[i].getType();
        }

        while (!arrayIters[0]->end())
        {
            for (size_t i = 0; i < nattrs; i++)
            {
                chunkIters[i] = arrayIters[i]->getChunk().getConstIterator();
            }

            while (!chunkIters[0]->end())
            {
                vector<Value> item(nattrs);
                stringstream ssvalue;
                stringstream sspos;

                ssvalue << "( ";
                for (size_t i = 0; i < nattrs; i++)
                {
                    item[i] = chunkIters[i]->getItem();
                    ssvalue << item[i].toString(attrTypes[i]) << " ";
                }
                ssvalue << ")";

                sspos << "( ";
                for (size_t i = 0; i < chunkIters[0]->getPosition().size(); i++)
                {
                    sspos << chunkIters[0]->getPosition()[i] << " ";
                }
                sspos << ")";

                LOG4CXX_DEBUG(logger, "[PrintArray] pos " << sspos.str() << " val " << ssvalue.str());

                for (size_t i = 0; i < nattrs; i++)
                {
                    ++(*chunkIters[i]);
                }
            }

            for (size_t i = 0; i < nattrs; i++)
            {
                ++(*arrayIters[i]);
            }
        }
#endif
    }
}
