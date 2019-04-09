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

#ifndef SCIDB_CLIENT

#include <array/AccumulatorArray.h>

#include <query/Query.h>

namespace scidb {
    //
    // AccumulatorArray
    //
    AccumulatorArray::AccumulatorArray(std::shared_ptr<Array> array,
                                       std::shared_ptr<Query>const& query)
    : StreamArray(array->getArrayDesc(), false),
      pipe(array),
      iterators(array->getArrayDesc().getAttributes().size())
    {
        assert(query);
        _query=query;
    }
    ConstChunk const* AccumulatorArray::nextChunk(AttributeID attId, MemChunk& chunk)
    {
        if (!iterators[attId]) {
            iterators[attId] = pipe->getConstIterator(attId);
        } else {
            ++(*iterators[attId]);
        }
        if (iterators[attId]->end()) {
            return NULL;
        }
        ConstChunk const& inputChunk = iterators[attId]->getChunk();
        if (inputChunk.isMaterialized()) {
            return &inputChunk;
        }

        Address addr(attId, inputChunk.getFirstPosition(false));
        chunk.initialize(this, &desc, addr, inputChunk.getCompressionMethod());
        chunk.setBitmapChunk((Chunk*)&inputChunk);
        std::shared_ptr<ConstChunkIterator> src = inputChunk.getConstIterator(ChunkIterator::INTENDED_TILE_MODE|
                                                                                ChunkIterator::IGNORE_EMPTY_CELLS);
        std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        std::shared_ptr<ChunkIterator> dst =
            chunk.getIterator(query,
                              (src->getMode() & ChunkIterator::TILE_MODE)|
                              ChunkIterator::NO_EMPTY_CHECK|
                              ChunkIterator::SEQUENTIAL_WRITE);
        size_t count = 0;
        while (!src->end()) {
            if (dst->setPosition(src->getPosition())) {
                dst->writeItem(src->getItem());
                count += 1;
            }
            ++(*src);
        }
        if (!desc.hasOverlap()) {
            chunk.setCount(count);
        }
        dst->flush();
        return &chunk;
    }
}  // scidb
#endif // ifndef SCIDB_CLIENT
