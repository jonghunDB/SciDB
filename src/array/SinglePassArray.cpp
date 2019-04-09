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

#include <array/SinglePassArray.h>

namespace scidb {
SinglePassArray::SinglePassArray(ArrayDesc const& arr)
: StreamArray(arr, false),
  _enforceHorizontalIteration(false),
  _consumed(arr.getAttributes().size()),
  _rowIndexPerAttribute(arr.getAttributes().size(), 0)
{}

std::shared_ptr<ConstArrayIterator>
SinglePassArray::getConstIterator(AttributeID attId) const
{
    ScopedMutexLock cs(_mutex);

    if (_iterators[attId]) { return _iterators[attId]; }

    // Initialize all attribute iterators at once
    // to avoid unnecessary exceptions
    // (in case when getConstIterator(aid) are not called for all attributes before iteration)
    Exception::Pointer err;
    for (AttributeID a=0, n=safe_static_cast<AttributeID>(_iterators.size());
         a<n;
         ++a)
    {
        try {
            if (!_iterators[a]) {
                StreamArray* self = const_cast<StreamArray*>(static_cast<const StreamArray*>(this));
                std::shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, a));
                const_cast< std::shared_ptr<ConstArrayIterator>& >(_iterators[a]) = cai;
            }
        } catch (const StreamArray::RetryException& e) {
            if (a == attId) {
                err = e.copy();
            }
        }
    }
    if (err) { err->raise(); }
    assert(_iterators[attId]);
    return _iterators[attId];
}

ConstChunk const*
SinglePassArray::nextChunk(AttributeID attId, MemChunk& chunk)
{
    ScopedMutexLock cs(_mutex);

    static const char* funcName="SinglePassArray:nextChunk: ";
    // Ensure that attributes are consumed horizontally
    while(true) {

        ConstChunk const* result(NULL);
        assert(attId < _rowIndexPerAttribute.size());

        const size_t nAttrs = _rowIndexPerAttribute.size();
        const size_t currRowIndex = getCurrentRowIndex();

        size_t& chunkIndex = _rowIndexPerAttribute[attId];

        if (chunkIndex != currRowIndex) {
            // the requested chunk must be in the current row
            ASSERT_EXCEPTION((currRowIndex > chunkIndex), funcName);
            ASSERT_EXCEPTION((chunkIndex == (currRowIndex-1)
                              || !_enforceHorizontalIteration) , funcName);

            result = &getChunk(attId, chunkIndex+1);
            ++chunkIndex;
            ++_consumed;
            assert(_consumed<=nAttrs || !_enforceHorizontalIteration);
            return result;
        }

        if (_enforceHorizontalIteration && _consumed < nAttrs) {
            // the previous row has not been fully consumed
            throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
        }

        assert(_consumed==nAttrs || !_enforceHorizontalIteration);

        if (!moveNext(chunkIndex+1)) {
            // no more chunks
            return result;
        }

        // advance to the next row and get the chunk
        _consumed = 0;
        result = &getChunk(attId, chunkIndex+1);
        assert(result);
        ++chunkIndex;
        ++_consumed;

        if (hasValues(result)) { return result; }

        // run through the rest of the attributes discarding empty chunks
        for (size_t a=0; a < nAttrs; ++a) {
            if (a==attId) { continue; }
            ConstChunk const* result = nextChunk(safe_static_cast<AttributeID>(a), chunk);
            ASSERT_EXCEPTION((!hasValues(result)), funcName);
            assert(getCurrentRowIndex() == _rowIndexPerAttribute[a]);
        }
        assert(_consumed == nAttrs);
        assert(getCurrentRowIndex() == _rowIndexPerAttribute[attId]);
    }
    ASSERT_EXCEPTION(false, funcName);
    return NULL;
}

bool
SinglePassArray::hasValues(const ConstChunk* chunk)
{
    bool isEmptyable = (getArrayDesc().getEmptyBitmapAttribute() != NULL);
    bool chunkHasVals = (!isEmptyable) || (!chunk->isEmpty());
    return (chunkHasVals && (chunk->getSize() > 0));
}
} // scidb
#endif // ifndef SCIDB_CLIENT
