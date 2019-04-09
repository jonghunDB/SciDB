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
 * @file ChunkAuxMeta.h
 *
 * @brief ChunkAuxMeta implementation
 */

#ifndef CHUNK_AUX_META_H_
#define CHUNK_AUX_META_H

#include <storage/StorageMgr.h>
#include <query/InstanceID.h>

namespace scidb {

/**
 * Chunk metadata which is not part of the index key
 *
 * ChunkAuxMeta is persisted to disk so the size needs to be fixed.
 */
class ChunkAuxMeta
{
public:
    ChunkAuxMeta()
        : _instanceId(static_cast<uint64_t>(INVALID_INSTANCE))
        , _nElements(0)
        , _storageVersion(static_cast<uint64_t>(StorageMgr::getInstance()->SCIDB_STORAGE_FORMAT_VERSION))
    {}
    InstanceID getInstanceId() const { return _instanceId; }
    void setInstanceId(InstanceID id) { _instanceId = id; }
    uint32_t getStorageVersion() const { return _storageVersion; }
    void setNElements(uint64_t n) { _nElements = n; }
    uint64_t getNElements() const { return _nElements; }

private:
    InstanceID _instanceId;
    uint64_t _nElements;
    uint32_t _storageVersion;
};

}  // namespace scidb

#endif  // CHUNK_AUX_MEtA_H_
