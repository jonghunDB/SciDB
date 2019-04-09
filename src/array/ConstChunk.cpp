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
 * @file ConstChunk.cpp
 *
 * @brief class ConstChunk
 */

#include <vector>
#include <string.h>

#include <log4cxx/logger.h>

#include <array/MemChunk.h>
#include <array/PinBuffer.h>
#include <array/RLE.h>
#include <query/Query.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.ConstChunk"));

    size_t ConstChunk::getBitmapSize() const
    {
        if (isMaterialized() && !getAttributeDesc().isEmptyIndicator()) {
            PinBuffer scope(*this);
            ConstRLEPayload payload((char*)getConstData());
            return getSize() - payload.packedSize();
        }
        return 0;
    }

    ConstChunk const* ConstChunk::getBitmapChunk() const
    {
        return this;
    }

    void ConstChunk::makeClosure(Chunk& closure, std::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const
    {
        PinBuffer scope(*this);
        closure.allocate(getSize() + emptyBitmap->packedSize());
        memcpy(closure.getDataForLoad(), getConstData(), getSize());
        emptyBitmap->pack((char*)closure.getDataForLoad() + getSize());
    }

    ConstChunk* ConstChunk::materialize() const
    {
        if (materializedChunk == NULL || materializedChunk->getFirstPosition(false) != getFirstPosition(false)) {
            if (materializedChunk == NULL) {
                ((ConstChunk*)this)->materializedChunk = new MemChunk();
            }
            materializedChunk->initialize(*this);
            materializedChunk->setBitmapChunk((Chunk*)getBitmapChunk());
            std::shared_ptr<ConstChunkIterator> src
                = getConstIterator((getArrayDesc().getEmptyBitmapAttribute() == NULL ?  ChunkIterator::IGNORE_DEFAULT_VALUES : 0 )|ChunkIterator::IGNORE_EMPTY_CELLS|ChunkIterator::INTENDED_TILE_MODE|(materializedChunk->getArrayDesc().hasOverlap() ? 0 : ChunkIterator::IGNORE_OVERLAPS));

            std::shared_ptr<Query> emptyQuery;
            std::shared_ptr<ChunkIterator> dst
                = materializedChunk->getIterator(emptyQuery,
                                                 (src->getMode() & ChunkIterator::TILE_MODE)|ChunkIterator::ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::SEQUENTIAL_WRITE);
            size_t count = 0;
            while (!src->end()) {
                if (!dst->setPosition(src->getPosition())) {
                    Coordinates const& pos = src->getPosition();
                    dst->setPosition(pos);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
                dst->writeItem(src->getItem());
                count += 1;
                ++(*src);
            }
            if (!getArrayDesc().hasOverlap()) {
                materializedChunk->setCount(count);
            }
            dst->flush();
        }
        return materializedChunk;
    }

    void ConstChunk::compress(CompressedBuffer& buf, std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
    {
        materialize()->compress(buf, emptyBitmap);
    }

    void* ConstChunk::getWriteData()
    {
        ASSERT_EXCEPTION_FALSE("getWriteData() on ConstChunk not permitted");
    }

    const void* ConstChunk::getConstData() const
    {
        return materialize()->getConstData();
    }

    size_t ConstChunk::getSize() const
    {
        return materialize()->getSize();
    }

    bool ConstChunk::pin() const
    {
        return false;
    }
    void ConstChunk::unPin() const
    {
        assert(typeid(*this) != typeid(ConstChunk));
    }

    void ConstChunk::showInfo(log4cxx::LoggerPtr const &logger, std::string const &prefix) const
    {
#ifndef NDEBUG
        if( isMaterialized() && getSize() ) {
            std::shared_ptr<ConstChunkIterator> chunkIter = getConstIterator();
            if(chunkIter)
            {
                while (!chunkIter->end()) {
                    std::stringstream ss;

                    const Coordinates & vCoordinates = chunkIter->getPosition();
                    switch(vCoordinates.size())
                    {
                        case 0:
                            ss << prefix << " (EMPTY)";
                            LOG4CXX_DEBUG (logger, ss.str());
                        break;

                        case 1:
                            ss << prefix <<  " (" << vCoordinates[0] <<  ")";
                            LOG4CXX_DEBUG (logger, ss.str());
                        break;

                        case 2:
                            ss << prefix << " (" << vCoordinates[0] << "," << vCoordinates[1] << ")";
                            LOG4CXX_DEBUG (logger, ss.str());
                        break;

                        default:
                            std::vector <Coordinate>::const_iterator it;
                            for(it = vCoordinates.begin();  it != vCoordinates.end();  ++it) {
                                ss << prefix  << " (" << (*it) << ")";
                                LOG4CXX_DEBUG (logger, ss.str());
                                ss.str(std::string());
                            }
                        break;
                    }

                    ++(*chunkIter);
                }
            }
        }
#endif
    }

    bool ConstChunk::contains(Coordinates const& pos, bool withOverlap) const
    {
        Coordinates const& first = getFirstPosition(withOverlap);
        Coordinates const& last = getLastPosition(withOverlap);
        for (size_t i = 0, n = first.size(); i < n; i++) {
            if (pos[i] < first[i] || pos[i] > last[i]) {
                return false;
            }
        }
        return true;
    }

    bool ConstChunk::isCountKnown() const
    {
        return getArrayDesc().getEmptyBitmapAttribute() == NULL
            || (isMaterializedChunkPresent() && materialize()->isCountKnown());
    }

    size_t ConstChunk::count() const
    {
        if (getArrayDesc().getEmptyBitmapAttribute() == NULL) {
            return getNumberOfElements(false);
        }
        if (isMaterializedChunkPresent()) {
            // We have a materialized chunk but its first position may not match
            // that of *this, call materialize() to check and re-materialize
            // the chunk if necessary.
            return materialize()->count();
        }
        std::shared_ptr<ConstChunkIterator> i = getConstIterator();
        size_t n = 0;
        while (!i->end()) {
            ++(*i);
            n += 1;
        }
        return n;
    }

    size_t ConstChunk::getNumberOfElements(bool withOverlap) const
    {
        Coordinates low = getFirstPosition(withOverlap);
        Coordinates high = getLastPosition(withOverlap);
        return getChunkNumberOfElements(low, high);
    }

    bool ConstChunk::isSolid() const
    {
        Dimensions const& dims = getArrayDesc().getDimensions();
        Coordinates const& first = getFirstPosition(false);
        Coordinates const& last = getLastPosition(false);
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            if (dims[i].getChunkOverlap() != 0 || (last[i] - first[i] + 1) != dims[i].getChunkInterval()) {
                return false;
            }
        }
        return !getAttributeDesc().isNullable()
            && !TypeLibrary::getType(getAttributeDesc().getType()).variableSize()
            && getArrayDesc().getEmptyBitmapAttribute() == NULL;
    }

    bool ConstChunk::isReadOnly() const
    {
        return true;
    }

    bool ConstChunk::isMaterialized() const
    {
        return false;
    }

    void ConstChunk::showEmptyBitmap(const std::string & strPrefix) const
    {
        // Purposely empty
    }

    std::shared_ptr<ConstRLEEmptyBitmap> ConstChunk::getEmptyBitmap() const
    {
        if (getAttributeDesc().isEmptyIndicator()/* && isMaterialized()*/) {
            PinBuffer scope(*this);
            return std::shared_ptr<ConstRLEEmptyBitmap>(new RLEEmptyBitmap(ConstRLEEmptyBitmap(*this)));
        }
        AttributeDesc const* emptyAttr = getArrayDesc().getEmptyBitmapAttribute();
        if (emptyAttr != NULL) {
            if (!emptyIterator) {
                ((ConstChunk*)this)->emptyIterator = getArray().getConstIterator(emptyAttr->getId());
            }
            if (!emptyIterator->setPosition(getFirstPosition(false))) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            ConstChunk const& bitmapChunk = emptyIterator->getChunk();
            PinBuffer scope(bitmapChunk);
            return std::shared_ptr<ConstRLEEmptyBitmap>(new RLEEmptyBitmap(ConstRLEEmptyBitmap((char*)bitmapChunk.getConstData())));
        }
        return std::shared_ptr<ConstRLEEmptyBitmap>();
    }


    ConstChunk::ConstChunk() : materializedChunk(nullptr)
    {
    }

    ConstChunk::~ConstChunk()
    {
        delete materializedChunk;
    }

    bool ConstChunk::isMaterializedChunkPresent() const
    {
        return materializedChunk != nullptr;
    }

    void ConstChunk::releaseMaterializedChunk()
    {
        if (materializedChunk) {
            delete materializedChunk;
            materializedChunk = nullptr;
        }
    }
}
