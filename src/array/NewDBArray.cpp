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
 * @file NewDBArray.cpp
 * @brief Next-gen persistent array implementation
 */

#include <log4cxx/logger.h>
#include <array/NewDBArray.h>
#include <array/ArrayDistribution.h>
#include <array/NewReplicationMgr.h>
#include <storage/StorageMgr.h>
#include <system/Exceptions.h>
#include <util/compression/Compressor.h>
#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>
#include <util/OnScopeExit.h>
#include <set>
#include <vector>

namespace scidb {
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.NewDBArray"));

/*
 * NewDBArray
 */

NewDBArray::NewDBArray(ArrayDesc const& desc, const std::shared_ptr<Query>& query)
    : _desc(desc)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArray::DBArray arrayID=" << _desc.getId()
                          << ", UAID=" << _desc.getUAId()
                          << ", ps=" << _desc.getDistribution()->getPartitioningSchema()
                          << ", desc=" << desc);
    // clang-format on
    _query = query;
    DataStore::DataStoreKey dsk = DBArrayMgr::getInstance()->getDsk(_desc);
    DbAddressMeta addressMeta;
    DBIndexMgr::getInstance()->getIndex(_diskIndex, dsk, addressMeta);
    SCIDB_ASSERT(_diskIndex);
    SCIDB_ASSERT(query);
    SCIDB_ASSERT(_desc.getDistribution()->getPartitioningSchema() != psUninitialized);
    SCIDB_ASSERT(_desc.getDistribution()->getPartitioningSchema() != psUndefined);
}

void NewDBArray::removeDeadChunks(std::shared_ptr<Query>& query,
                                  std::set<Coordinates, CoordinatesLess> const& liveChunks)
{
    // We may not need to replicate, but if we do, the machinery has
    // to be set up outside the scope of the _mutex lock.
    NewReplicationManager* repMgr = NewReplicationManager::getInstance();
    std::vector<std::shared_ptr<NewReplicationManager::Item>> replicasVec;
    OnScopeExit replicasCleaner([&replicasVec, repMgr] () {
            repMgr->abortReplicas(replicasVec);
        });

    { // Begin _mutex scope!
    ScopedMutexLock cs(_mutex);

    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArray::removeDeadChunks arrayID=" << _desc.getId());
    // clang-format on

    /* Gather a list of the "dead" chunks... those that need tombstone
       entries...
     */
    typedef std::set<Coordinates, CoordinatesLess> DeadChunks;
    DeadChunks deadChunks;
    std::shared_ptr<ConstArrayIterator> arrIter = getConstIterator(0);

    while (!arrIter->end()) {
        Coordinates currentCoords = arrIter->getPosition();

        // clang-format off
        LOG4CXX_TRACE(logger, "NewDBArray::removeDeadChunks current chunk: "
                              << CoordsToStr(currentCoords));
        // clang-format on

        if (liveChunks.count(currentCoords) == 0) {
            deadChunks.insert(currentCoords);
        }
        ++(*arrIter);
    }

    /* For each address in the deadChunks set, enter a NULL buf
       handle (tombstone) in the index at the current version, for
       all attributes.
     */
    replicasVec.reserve(_desc.getDistribution()->getRedundancy() * deadChunks.size());
    for (Coordinates const& coords : deadChunks) {
        // Start replication for the tombstone to other instances.
        PersistentAddress addr(_desc.getId(), 0, coords);
        repMgr->replicate(_desc, addr, NULL, NULL, 0, 0, query, replicasVec);

        // Store the tombstone locally.
        removeLocalChunkLocked(query, coords);
    }

    } // End _mutex scope!

    // Wait for replicas to be sent (if there were any).  Replicate
    // without _mutex, to avoid inter-instance deadlock (SDB-5866).
    if (!replicasVec.empty()) {
        repMgr->waitForReplicas(replicasVec);
        replicasCleaner.cancel();
    }
}

void NewDBArray::removeLocalChunk(std::shared_ptr<Query> const& query, Coordinates const& coords)
{
    ScopedMutexLock cs(_mutex);
    removeLocalChunkLocked(query, coords);
}

void NewDBArray::removeLocalChunkLocked(std::shared_ptr<Query> const& query,
                                        Coordinates const& coords)
{
    Query::validateQueryPtr(query);

    ChunkAuxMeta chunkAuxMeta;

    chunkAuxMeta.setInstanceId(
        NewReplicationManager::getInstance()->getPrimaryInstanceId(_desc, coords));
    PointerRange<const char> chunkAuxMetaPtr(sizeof(ChunkAuxMeta),
                                             reinterpret_cast<char const*>(&chunkAuxMeta));
    DBDiskIndex::DiskIndexValue value;

    for (AttributeID id = 0; id < _desc.getAttributes().size(); ++id) {
        DbAddressMeta dbam;
        DbAddressMeta::KeyWithSpace key;

        key.initializeKey(dbam, coords.size());
        dbam.fillKey(key.getKey(), _diskIndex->getDsk(), id, coords, _desc.getId());

        _diskIndex->insertRecord(key.getKey(),
                                 value,  // NULL buffer handle
                                 chunkAuxMetaPtr,
                                 false,  // unpin the buffer after insert
                                 true);  // replace any existing value
    }
}

NewDBArray::~NewDBArray()
{
    LOG4CXX_TRACE(logger, "NewDBArray::~NewDBArray dtor, arrayID="
                  << _desc.getId());
}

void NewDBArray::removeVersions(std::shared_ptr<Query>& query, ArrayID lastLiveArrId)
{
    ScopedMutexLock cs(_mutex);

    PersistentAddress currentChunkAddr;
    DbAddressMeta dbam;
    bool currentChunkIsLive = true;

    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArray::removeVersions arrayID=" << _desc.getId()
                          << " lastliveID=" << lastLiveArrId);
    // clang-format on

    NewDBArray::DBDiskIndex::Iterator current = _diskIndex->begin();

    while (!current.isEnd()) {
        PersistentAddress targetAddress;
        dbam.keyToAddress(&(current.getKey()), targetAddress);

        /* If lastLiveArrId is non-zero, we must determine if the chunk is live.
           If lastLiveArrId is zero, then we proceed immediately to remove chunk.
        */
        if (lastLiveArrId) {
            if (!targetAddress.sameBaseAddr(currentChunkAddr)) {
                /* Move on to next coordinate
                 */
                currentChunkAddr = targetAddress;
                currentChunkIsLive = true;
            }
            if (targetAddress.arrVerId > lastLiveArrId) {
                /* Chunk was added after oldest version
                   so it is still live
                */
                ++current;
                continue;
            } else if (targetAddress.arrVerId == lastLiveArrId) {
                /* Chunk was added in oldest version so it is
                   still live, but any older chunks are not
                */
                currentChunkIsLive = false;
                ++current;
                continue;
            } else if (targetAddress.arrVerId < lastLiveArrId) {
                /* Chunk was added prior to oldest version
                 */
                if (currentChunkIsLive) {
                    /* Chunk is still live, but older chunks are not
                     */
                    currentChunkIsLive = false; // coverage
                    ++current;
                    continue;
                }
            }
        }

        /* Chunk should be removed
         */
        // clang-format off
        LOG4CXX_TRACE(logger, "NewDBArray::removeVersions "
                              << "found chunk to remove: " << targetAddress.toString());
        // clang-format on
        _diskIndex->deleteRecord(&(current.getKey()));
        ++current;
    }

    /* Remove the index and backing storage if the array is going
       away.
     */
    if (lastLiveArrId == 0) {
        DBIndexMgr::getInstance()->closeIndex(_diskIndex, true /* remove from disk */);
    }
}

void NewDBArray::flush()
{
    ScopedMutexLock cs(_mutex);
    _diskIndex->flushVersion(_desc.getId());
}

void NewDBArray::pinChunk(CachedDBChunk const& chunk)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArray::pinChunk chunk=" << reinterpret_cast<void const*>(&chunk)
                          << ", accessCount is " << chunk._accessCount
                          << ", Array=" << reinterpret_cast<void*>(this)
                          << ", name " << chunk.arrayDesc->getName()
                          << ", Addr=" << chunk.addr.toString()
                          << ", Size=" << chunk.size
                          << ", Key=" << _diskIndex->getKeyMeta().keyToString(chunk._key.getKey()));
    // clang-format on

    Query::getValidQueryPtr(_query);

    ScopedMutexLock cs(_mutex);
    if (chunk._accessCount++ == 0) {
        /* Retrieve the value stored in the map for this chunk address
         */
        PointerRange<char const> chunkAuxMeta;

        DbAddressMeta::Key* key = chunk._key.getKey();
        NewDBArray::DBDiskIndex::Iterator iter = _diskIndex->find(key);

        // Attempt to use the compressor type given in the bufferhandle in the chunk _index,
        // but use the compressor type specified in the chunk if the bufferhandle
        // doesn't have one set yet (and set the bufferhandle.compressorType to that value).
        //
        // @see SDB-5964 for better function call API recommendations.
        auto& bufHandle = chunk._indexValue.getBufferHandle();
        CompressorType cType = bufHandle.getCompressorType();
        if (cType == CompressorType::UNKNOWN) {
            // The buffer handle's compressionType may be UNKNOWN here if it's a new
            // handle, but the chunk is certain to have a good cType.  If we're about to
            // write a new, empty chunk, it will have the desired cType from the schema.
            // If we've read this chunk from disk (or from a peer instance) it will have
            // its actual cType.  Either way we can now update the buffer handle.
            SCIDB_ASSERT(chunk.compressionMethod != CompressorType::UNKNOWN);
            bufHandle.setCompressorType(chunk.compressionMethod);
        }
        _diskIndex->pinValue(iter, chunk._indexValue);

        chunkAuxMeta = iter.getAuxMeta();
        chunk._chunkAuxMeta =
            reinterpret_cast<ChunkAuxMeta const&>(*chunkAuxMeta.begin());
        chunk.size = chunk._indexValue.constMemory().size();
        chunk.markClean();
    } else {
        assert(chunk.getConstData() != NULL || chunk.size == 0);
    }
}

void NewDBArray::unpinChunk(CachedDBChunk const& chunk)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArray::unpinChunk chunk=" << (void*)&chunk
                  << ", accessCount is " << chunk._accessCount
                  << ", Array=" << (void*)this
                  << ", name " << chunk.arrayDesc->getName()
                  << ", Addr=" << chunk.addr.toString()
                  << ", Size=" << chunk.size
                  << ", Key=" << _diskIndex->getKeyMeta().keyToString(chunk._key.getKey()));
    // clang-format on

    // We may not need to replicate, but if we do, the machinery has
    // to be set up outside the scope of the _mutex lock.
    NewReplicationManager* repMgr = NewReplicationManager::getInstance();
    NewReplicationManager::ItemVector replicasVec;
    OnScopeExit replicasCleaner([&replicasVec, repMgr] () {
            repMgr->abortReplicas(replicasVec);
        });

    { // Begin _mutex scope!
    ScopedMutexLock cs(_mutex);
    assert(chunk._accessCount > 0);
    // chunk.getConstData()==NULL --> chunk.size==0
    assert(chunk.getConstData() != NULL || chunk.size == 0);

    if (--chunk._accessCount == 0) {

        /* We may be here during the abort of a transaction.  If so,
           then we should not try to write the buffer, it will get
           rolledback anyway. Just get out.
         */
        std::shared_ptr<Query> queryPtr;
        try {
            queryPtr = Query::getValidQueryPtr(_query);
        } catch (const std::exception& e) {
            /* If the value is caller pinned, the chunk destructor will
               clean it up.  But if its index pinned, we need to unpin
               the buffer here.
             */
            if (chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::IndexPinned) {
                chunk._indexValue.unpin();
            }
            return;
        }
        // The query is valid so this function was not called during an abort transaction.

        size_t compressedSize = chunk.size;

        if (chunk.isDirty()) {
            auto& bufHandle = chunk._indexValue.getBufferHandle();
            SCIDB_ASSERT(bufHandle.getCompressorType() == chunk.compressionMethod);
            SCIDB_ASSERT(chunk.compressionMethod != CompressorType::UNKNOWN);
            if (CompressorType::NONE != bufHandle.getCompressorType()) {
                // Compression MUST happen before the chunk is replicated.
                compressedSize = CompressorFactory::getInstance()
                    .getCompressor(chunk.getCompressionMethod())
                    ->compress(chunk.getCompressionBuffer(),  // destination
                               chunk,        // source chunk
                               chunk.size);  // uncompressed size
                ASSERT_EXCEPTION(chunk.size >= compressedSize,
                                 "Failed to compress chunk. Compression resulted in size increase.");
                if (compressedSize == chunk.size) {
                    // The compressors return a size equal to the "uncompressed" size in the case of failure.
                    // Use no compressor for this chunk.
                    chunk.compressionMethod = CompressorType::NONE;
                }
                // Update the _indexValue.BufferHandle metadata in the Chunk with the
                // actual compressed size (and the compressionMethod if compression failed).
                //
                // TODO :
                // @see SDB-5964 for better function call API recommendations.
                const_cast<CachedDBChunk&>(chunk).setCompressedSizeAndType(compressedSize,
                                                                           chunk.compressionMethod);
                // After setting the metadata, the CompressorType in the chunk and the
                // BufferHandle should be known and equal.
                SCIDB_ASSERT(chunk.compressionMethod != CompressorType::UNKNOWN);
                SCIDB_ASSERT(chunk.compressionMethod == bufHandle.getCompressorType());

                PointerRange<const char> chunkAuxMeta(sizeof(ChunkAuxMeta),
                                                      reinterpret_cast<char const*>(
                                                          &chunk.getChunkAuxMeta()));
                // Update/insert the diskIndex to record the correct size of the buffer that
                // will be added to the DataStore.
                _diskIndex->insertRecord(chunk._key.getKey(),
                                         chunk._indexValue,
                                         chunkAuxMeta,
                                         true /* keep this chunk pinned */,
                                         true /* update record if exists*/);
            }
        }

        /* If the value is dirty, replicate the data to other instances
         */
        if (chunk.isDirty()) {
            void const * data = nullptr;
            // TODO The assumption is that if the compressed size and uncompressed size
            //      are the same, then go ahead and use the "uncompressed" buffer.  The
            //      code base doesn't have any real consistency in the case where
            //      compression does not result in a size reduction.
            if (compressedSize < chunk.size &&
                CompressorType::NONE != chunk.compressionMethod){
                data = chunk.getCompressionBuffer();
            } else {
                data = chunk.getConstData();
            }

            LOG4CXX_TRACE(logger, "Attempting to replicate..."
                                  << " _desc = " << _desc.getName()
                                  << " chunk.size = " << chunk.size
                                  << " compressedSize = " << compressedSize);

            try {
                repMgr->replicate(_desc,
                                  chunk._storageAddr,
                                  &chunk,
                                  data,
                                  compressedSize,
                                  chunk.size,
                                  queryPtr,
                                  replicasVec);
                LOG4CXX_TRACE(logger, "Replication successful");
            }
            catch (const std::exception& e) {
                LOG4CXX_WARN(logger, "Replication failed on exception " << e.what());
            }
            catch (...) {
                LOG4CXX_WARN(logger, "Replication failed on unknown exception");
            }
        }

        /* If the value is owned by the caller, we need to insert/update
           the value
         */
        DbAddressMeta::Key* key = chunk._key.getKey();
        if (chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::CallerPinned) {
            // clang-format off
            LOG4CXX_TRACE(logger, "NewDBArray::unpinChunk "
                                  << "insert record into index");

            // clang-format on

            // should not allocate mem if not dirty
            SCIDB_ASSERT(chunk.isDirty());

            PointerRange<const char> chunkAuxMeta(sizeof(ChunkAuxMeta),
                                                  reinterpret_cast<char const*>(
                                                      &chunk.getChunkAuxMeta()));

            /* upsert into disk index and unpin dirty chunk */
            bool ok = _diskIndex->insertRecord(key,
                                               chunk._indexValue,
                                               chunkAuxMeta,
                                               /*keepPinned:*/ false,
                                               /*update:*/ true);
            SCIDB_ASSERT(ok);
        } else if (chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::IndexPinned) {
            // The value is already in the index and we simply need to
            // unpin the value...
            LOG4CXX_TRACE(logger, "NewDBArray::unpinChunk unpin index value");
            chunk._indexValue.unpin();
        } else {
             // e.g. when allocation fails, throwing bypassing pinning actions
             // and unpinners detect but assert to identify pin/unpin imbalances
             SCIDB_ASSERT(chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::Unpinned);
             LOG4CXX_WARN(logger, "NewDBArray::unpinChunk chunk already UnPinned, (chunk allocation failed?)");
        }
    }

    } // End _mutex scope!

    // Wait for replicas to be sent (if there were any).  Replicate
    // without _mutex, to avoid inter-instance deadlock (SDB-5866).
    if (!replicasVec.empty()) {
        repMgr->waitForReplicas(replicasVec);
        replicasCleaner.cancel();
    }
}

ArrayDesc const& NewDBArray::getArrayDesc() const
{
    return _desc;
}

void NewDBArray::makeChunk(PersistentAddress const& addr,
                           CachedDBChunk*& chunk,
                           CachedDBChunk*& bitmapchunk,
                           bool newChunk)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    CachedDBChunk::createChunk(_desc, _diskIndex, addr, chunk, bitmapchunk, newChunk, this);
}

std::shared_ptr<ArrayIterator> NewDBArray::getIterator(AttributeID attId)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArray::getIterator Getting arrayID=" << _desc.getId()
                          << ", attrID=" << attId);
    // clang-format on
    std::shared_ptr<NewDBArray> owner;
    owner = this->shared_from_this();
    return std::shared_ptr<ArrayIterator>(new NewDBArrayIterator(owner, attId));
}

std::shared_ptr<ConstArrayIterator> NewDBArray::getConstIterator(AttributeID attId) const
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArray::getConstIterator "
                          << "Getting const DB iterator for arrayID=" << _desc.getId()
                          << ", attrID=" << attId);
    // clang-format on
    return (const_cast<NewDBArray*>(this))->getIterator(attId);
}

void NewDBArray::rollbackVersion(VersionID lastVersion, ArrayID baseArrayId, ArrayID newArrayId)
{
    typedef DiskIndex<DbAddressMeta> DBDiskIndex;
    typedef IndexMgr<DbAddressMeta> DBIndexMgr;

    LOG4CXX_TRACE(logger, "NewDBArray::rollbackVersion()"
                  << " lastVer=" << lastVersion
                  << " uaid=" << baseArrayId
                  << " vaid=" << newArrayId << ")");

    /* Find the disk index associated with the baseArrayId, then
       initiate rollback.
     */
    DBArrayMgr* dbMgr = DBArrayMgr::getInstance();
    std::shared_ptr<DBDiskIndex> diskIndex;
    DataStore::DataStoreKey dsk = dbMgr->getDsk(baseArrayId);
    DbAddressMeta addressMeta;

    DBIndexMgr::getInstance()->getIndex(diskIndex, dsk, addressMeta);
    diskIndex->rollbackVersion(newArrayId, lastVersion);
}

/* NewDBArrayIterator
 */

NewDBArrayIterator::NewDBArrayIterator(std::shared_ptr<NewDBArray> arr, AttributeID attId)
    : _array(arr)
    , _currChunk(NULL)
    , _currBitmapChunk(NULL)
{
    _addr.arrVerId = _array->_desc.getId();
    _addr.attId = attId;
    _addr.coords.insert(_addr.coords.begin(), _array->_desc.getDimensions().size(), 0);
    resetAddrToMin();
    _positioned = false;
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::NewDBArrayIterator()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
}

NewDBArrayIterator::~NewDBArrayIterator()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::~NewDBArrayIterator()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    if (_currChunk) {
        _currChunk->deleteOnLastUnregister();
    }
    if (_currBitmapChunk) {
        _currBitmapChunk->deleteOnLastUnregister();
    }
}

void NewDBArrayIterator::resetAddrToMin()
{
    for (size_t i = 0; i < _array->_desc.getDimensions().size(); ++i) {
        _addr.coords[i] = _array->_desc.getDimensions()[i].getStartMin();
    }
    _addr.arrVerId = _array->_desc.getId();
}

void NewDBArrayIterator::resetChunkRefs()
{
    _positioned = false;
    if (_currChunk) {
        _currChunk->deleteOnLastUnregister();
    }
    if (_currBitmapChunk) {
        _currBitmapChunk->deleteOnLastUnregister();
    }
    _currChunk = NULL;
    _currBitmapChunk = NULL;
}

ConstChunk const& NewDBArrayIterator::getChunk()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::getChunk()"
                          << " This=" << (void*)this
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    position();
    if (!_currChunk || hasInjectedError(CANNOT_GET_CHUNK, __LINE__, __FILE__)) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    return *_currChunk;
}

bool NewDBArrayIterator::end()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::end()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    position();

    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::end() _currChunk = " << (void*)_currChunk);
    // clang-format on

    return _currChunk == NULL;
}

bool NewDBArrayIterator::advanceIters(PersistentAddress& currAddr,
                                      PersistentAddress const& oldAddr)
{
    ++_curr;
    if (_array->_desc.getEmptyBitmapAttribute()) {
        ++_currBitmap;
    }
    if (!_curr.isEnd()) {
        _array->_diskIndex->getKeyMeta().keyToAddress(&_curr.getKey(), currAddr);
    }
    if (_curr.isEnd() || currAddr.attId != oldAddr.attId) {
        /* We have reached the end of the iteration (off the end of
           the list or reached the next attribute)
         */
        return true;
    } else {
        /* Not at the end of the iteration
         */
        return false;
    }
}

void NewDBArrayIterator::findNextLogicalChunk(PersistentAddress& oldAddr,
                                              ArrayID targetVersion,
                                              std::shared_ptr<Query> query)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "findNextLogicalChunk after addr=" << oldAddr.toString());
    // clang-format on

    PersistentAddress currAddr;
    InstanceID currIid = INVALID_INSTANCE;
    NewReplicationManager* repMgr = NewReplicationManager::getInstance();
    bool reachedEnd = false;

    do {
        if (currIid != INVALID_INSTANCE) {
            oldAddr = currAddr;
        }

        do {
            reachedEnd = advanceIters(currAddr, oldAddr);
            if (reachedEnd) {
                break;
            }
        } while (currAddr.coords == oldAddr.coords || currAddr.arrVerId > targetVersion);
        if (reachedEnd) {
            break;
        }
        currIid =
            reinterpret_cast<ChunkAuxMeta const*>(_curr.getAuxMeta().begin())
                ->getInstanceId();
    } while (_curr.isNullValue() ||
             !repMgr->isResponsibleFor(_array->_desc, currAddr, currIid, query));

    setCurrent();
}

void NewDBArrayIterator::operator++()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::operator++()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    ScopedMutexLock cs(_array->_mutex, PTW_SML_MA);
    std::shared_ptr<Query> query = getQuery();
    position();

    findNextLogicalChunk(_addr, _array->_desc.getId(), query);
}

Coordinates const& NewDBArrayIterator::getPosition()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::getPosition()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    position();
    if (!_currChunk ||
        hasInjectedError(NO_CURRENT_CHUNK, __LINE__, __FILE__)) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    return _currChunk->getFirstPosition(false);
}

bool NewDBArrayIterator::setPosition(Coordinates const& pos)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::setPosition()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned
                          << " pos=" << pos);
    // clang-format on

    ScopedMutexLock cs(_array->_mutex);
    std::shared_ptr<Query> query = getQuery();
    DbAddressMeta::KeyWithSpace key;
    key.initializeKey(_array->_diskIndex->getKeyMeta(), pos.size());

    /* Search the disk index for the requested position.
     */
    PersistentAddress target;

    target.coords = pos;
    _array->_desc.getChunkPositionFor(target.coords);
    target.attId = _addr.attId;
    target.arrVerId = _array->_desc.getId();

    _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                             _array->_diskIndex->getDsk(),
                                             target.attId,
                                             target.coords,
                                             target.arrVerId);
    _curr = _array->_diskIndex->leastUpper(key.getKey());

    if (_array->_desc.getEmptyBitmapAttribute()) {
        AttributeID ebmAttrId = _array->_desc.getEmptyBitmapAttribute()->getId();

        _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                 _array->_diskIndex->getDsk(),
                                                 ebmAttrId,
                                                 target.coords,
                                                 target.arrVerId);
        _currBitmap = _array->_diskIndex->leastUpper(key.getKey());
    }

    /* If the requested position exists, set the current chunk refs.
       If not, mark this iterator as "not positioned"
     */
    PersistentAddress currAddr;
    NewReplicationManager* repMgr = NewReplicationManager::getInstance();
    bool sameCoords = true;
    InstanceID currIid =
        reinterpret_cast<ChunkAuxMeta const*>(_curr.getAuxMeta().begin())->getInstanceId();

    _array->_diskIndex->getKeyMeta().keyToAddress(&(_curr.getKey()), currAddr);
    sameCoords = currAddr.coords.size()
        ? (coordinatesCompare(currAddr.coords, target.coords) == 0)
        : false;

    /* Position "exists" iff it is not a tombstone, not a replica chunk,
       and has the correct coordinates.
     */
    if (!_curr.isNullValue() && sameCoords &&
        repMgr->isResponsibleFor(_array->_desc, currAddr, currIid, query)) {
        setCurrent();
    } else {
        resetChunkRefs();
    }

    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::setPosition() returning=" << (_currChunk != NULL));
    // clang-format on

    return _currChunk != NULL;
}

void NewDBArrayIterator::setCurrent()
{
    SCIDB_ASSERT(_array->_mutex.isLockedByThisThread());

    resetChunkRefs();
    _positioned = true;

    /* If we are not off the end, update the chunk refs.
     */
    if (!_curr.isEnd() && _curr.getKey()._attId == _addr.attId) {
        _array->_diskIndex->getKeyMeta().keyToAddress(&_curr.getKey(), _addr);
        _array->makeChunk(_addr, _currChunk, _currBitmapChunk, false);
        if (_currChunk) {
            _currChunk->size = _curr.valueSize();
            _currChunk->_chunkAuxMeta =
                *reinterpret_cast<ChunkAuxMeta const*>(_curr.getAuxMeta().begin());
        }
        if (_currBitmapChunk) {
            _currBitmapChunk->size = _currBitmap.valueSize();
            _currBitmapChunk->_chunkAuxMeta =
                *reinterpret_cast<ChunkAuxMeta const*>(_currBitmap.getAuxMeta().begin());
        }
        // clang-format off
        LOG4CXX_TRACE(logger, "NewDBArrayIterator::setCurrent()"
                              << " Array=" << (void*)_array.get()
                              << " Addr=" << _addr.toString()
                              << " Positioned=" << _positioned
                              << " CurrChunk=" << _currChunk
                              << " CurrBitmapChunk=" << _currBitmapChunk);
        // clang-format on
    } else {
        // clang-format off
        LOG4CXX_TRACE(logger, "NewDBArrayIterator::setCurrent"
                              << " Array=" << (void*)_array.get()
                              << " Addr=" << _addr.toString()
                              << " Positioned=" << _positioned
                              << " Requested position doesn't exist");
        // clang-format on
    }
}

void NewDBArrayIterator::restart()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::restart()"
                          << " This=" << (void*)this
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    ScopedMutexLock cs(_array->_mutex);

    DbAddressMeta::KeyWithSpace key;
    std::shared_ptr<Query> query = getQuery();

    resetAddrToMin();
    key.initializeKey(_array->_diskIndex->getKeyMeta(), _addr.coords.size());
    _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                             _array->_diskIndex->getDsk(),
                                             _addr.attId,
                                             _addr.coords,
                                             _addr.arrVerId);
    _curr = _array->_diskIndex->leastUpper(key.getKey());

    if (_array->_desc.getEmptyBitmapAttribute()) {
        AttributeID ebmAttrId = _array->_desc.getEmptyBitmapAttribute()->getId();

        _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                 _array->_diskIndex->getDsk(),
                                                 ebmAttrId,
                                                 _addr.coords,
                                                 _addr.arrVerId);
        _currBitmap = _array->_diskIndex->leastUpper(key.getKey());
    }

    /* If we are off the end, we should call setCurrent and be done.
     */
    if (_curr.isEnd() || _curr.getKey()._attId != _addr.attId) {
        setCurrent();
        return;
    }

    /* We have found the first entry in the map for this attribute.
       make sure we are viewing the correct version (check the version id)
     */
    PersistentAddress foundAddr;

    _array->_diskIndex->getKeyMeta().keyToAddress(&_curr.getKey(), foundAddr);
    if (foundAddr.arrVerId > _array->_desc.getId()) {
        findNextLogicalChunk(_addr, _array->_desc.getId(), query);
        return;
    }

    /* We are viewing the correct version, but it still may not
       be the one we want for two reasons:
       1) it may be a replica (check if we are not responsible)
       2) it may be a tombstone (check for null value)
     */
    InstanceID foundIid;
    NewReplicationManager* repMgr = NewReplicationManager::getInstance();
    foundIid =
        reinterpret_cast<ChunkAuxMeta const*>(_curr.getAuxMeta().begin())->getInstanceId();

    if (!repMgr->isResponsibleFor(_array->_desc, foundAddr, foundIid, query) ||
        _curr.isNullValue()) {
        findNextLogicalChunk(foundAddr, _array->_desc.getId(), query);
        return;
    }

    /* This must be the one we want
     */
    setCurrent();
}

void NewDBArrayIterator::deleteChunk(Chunk& aChunk)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::deleteChunk()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    CachedDBChunk& chunk = dynamic_cast<CachedDBChunk&>(aChunk);
    ScopedMutexLock cs(_array->_mutex);
    chunk._accessCount = 0;

    DbAddressMeta::Key* key = chunk._key.getKey();
    NewDBArray::DBDiskIndex::Iterator iter = _array->_diskIndex->find(key);
    if (!(iter.isEnd())) {
        _array->_diskIndex->deleteRecord(iter);
    }
}

Chunk& NewDBArrayIterator::newChunk(Coordinates const& pos)
{
    if (!_array->_desc.contains(pos) ||
        hasInjectedError(CHUNK_OUT_OF_BOUNDS, __LINE__, __FILE__)) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
            << CoordsToStr(pos) << _array->_desc.getDimensions();
    }

    ScopedMutexLock cs(_array->_mutex);

    _addr.coords = pos;
    _array->_desc.getChunkPositionFor(_addr.coords);

    // clang-format off
    LOG4CXX_TRACE(logger, "NewDBArrayIterator::newChunk()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    resetChunkRefs();

    /* TODO: previously we checked for existence of chunk at this point.
       Is this necessary?  Can we check when chunk is written (unpin)?
     */
    _array->makeChunk(_addr, _currChunk, _currBitmapChunk, true /* chunk pinned */);
    return *(_currChunk);
}

Chunk& NewDBArrayIterator::newChunk(Coordinates const& pos, CompressorType compressionMethod)   // coverage
{
    SCIDB_ASSERT(compressionMethod != CompressorType::UNKNOWN);
    Chunk& chunk = newChunk(pos);
    ((MemChunk&)chunk).compressionMethod = compressionMethod;
    return chunk;
}
}  // namespace scidb
