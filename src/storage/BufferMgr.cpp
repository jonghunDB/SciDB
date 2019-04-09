/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2018 SciDB, Inc.
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
 * @file BufferMgr.cpp
 *
 * @author Steve Fridella, Donghui Zhang
 *
 * @brief Implementation of buffer manager classes.
 */

#include <storage/BufferMgr.h>

#include <query/FunctionLibrary.h>
#include <query/Query.h>
#include <system/Config.h>
#include <system/Exceptions.h>
#include <system/Warnings.h>
#include <util/compression/Compressor.h>
#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>
#include <util/OnScopeExit.h>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.storage.bufmgr"));
log4cxx::LoggerPtr loggerByteCount(log4cxx::Logger::getLogger("scidb.storage.bufmgr.bytecount"));
}  // namespace

namespace scidb {

// This macro logs message at TRACE level using the loggerByteCount which contains the
// "standard" message followed by a json object so that the logs can more easily be parsed
// by something like python to help ascertain if/where a claim/release "ledger balance" of
// a byte count counter is incorrect. This MACRO is separate, for the time being, from the
// actual _claim/_releaseXXX() class because the __funct__ operator which makes the change
// to the byte counter. As refactoring of the code continues, this may become less important.
//
// _claimrelease_ :  "claim" or "release".
// _countertype_ : Which BufferMgr counter (cached, pinned, dirty, pending, etc) is being
//                 incremented/decremented. This is a free form text, but is was
//                 originally intended to distinguish between bytes associated with the
//                 "blockbase" and the "compressedBlockBase"
// _size_ : The size to increment(claim)/decrement(release) of the counter.
// _bhead_: the BlockHeader associated with the claim/release
//
// SEE: LOGCLAIM/LOGRELEASE. Rather than repeat the same LOG4CXX_TRACE for each of those,
// This macro maintains the same spacing in the JSON object which helps with grep
// operations of the logs. There are 10s of THOUSANDS of such messages when running large
// tests where the cache is actually ejecting/destaging buffers from the cache.
//
// clang-format off
#define LOGCLAIMRELEASE(_claimrelease_, _countertype_, _size_, _bhead_)                \
    LOG4CXX_TRACE(loggerByteCount, _claimrelease_  << " "  << _countertype_ << " -- {" \
                                   << " \"action\": \"" << _claimrelease_ << "\""      \
                                   << " , \"counter\": \"" << _countertype_ << "\""    \
                                   << " , \"sz\": " << _size_                          \
                                   << " , \"function\": " << "\"" << __func__ << "\""  \
                                   << " , \"bhead\": " << _bhead_.toString()           \
                                   << " }")

#define LOGCLAIM(_countertype_, _size_, _bhead_)             \
    LOGCLAIMRELEASE("claim", _countertype_, _size_, _bhead_)

#define LOGRELEASE(_countertype_, _size_, _bhead_)             \
    LOGCLAIMRELEASE("release", _countertype_, _size_, _bhead_)
// clang-format on

using namespace std;

const uint32_t BufferMgrDefaultListSize = 128;

const BufferMgr::BlockPos BufferMgr::MAX_BLOCK_POS =
    std::numeric_limits<BufferMgr::BlockPos>::max();

const int BufferMgr::MAX_PIN_RETRIES = 3;

//
// BufferHandle implementation
//
PointerRange<char> BufferMgr::BufferHandle::getRawData()
{
    if (isNull()) {
        return PointerRange<char>();
    }
    BufferMgr* bm = BufferMgr::getInstance();
    return bm->_getRawData(*this);
}

PointerRange<const char> BufferMgr::BufferHandle::getRawConstData()
{
    if (isNull()) {
        return PointerRange<char>();
    }
    BufferMgr* bm = BufferMgr::getInstance();
    return bm->_getRawConstData(*this);
}

void BufferMgr::BufferHandle::pinBuffer()
{
    if (isNull()) {
        return;
    }
    BufferMgr* bm = BufferMgr::getInstance();
    bm->_pinBuffer(*this);
}

void BufferMgr::BufferHandle::unpinBuffer()
{
    if (isNull()) {
        return;
    }
    BufferMgr* bm = BufferMgr::getInstance();
    bm->_unpinBuffer(*this);
}

void BufferMgr::BufferHandle::discardBuffer()
{
    if (isNull()) {
        return;
    }
    BufferMgr* bm = BufferMgr::getInstance();
    bm->_discardBuffer(*this);
}

// TODO: no coverage, because lack of coverage of CachedDBChunk::upgradeDescriptor
//       which is the sole use, and is not covered for the 17.9 release
void BufferMgr::BufferHandle::setBufferKey(const BufferKey& k)
{
    _key = k;
}

PointerRange<char> BufferMgr::BufferHandle::getCompressionBuffer()
{
    SCIDB_ASSERT(!isNull());
    BufferMgr* bm = BufferMgr::getInstance();
    return bm->_getCompressionBuffer(*this);
}

void BufferMgr::BufferHandle::setCompressedSize(size_t sz)
{
    _key.setCompressedSize(sz);
    BufferMgr::getInstance()->_setCompressedSize(*this, sz);
}

void BufferMgr::BufferHandle::setCompressorType(CompressorType cType)
{
    _compressorType = cType;
}

void BufferMgr::BufferHandle::resetHandle()
{
    BufferKey bk;
    _key = bk;
    _compressorType = CompressorType::UNKNOWN;
    resetSlotGenCount();
}

void BufferMgr::BufferHandle::resetSlotGenCount()
{
    _slot = INVALID_SLOT;
    _gencount = NULL_GENCOUNT;
}

BufferMgr::BufferHandle::BufferHandle()
    : _compressorType(CompressorType::UNKNOWN)
    , _slot(INVALID_SLOT)
    , _gencount(NULL_GENCOUNT)
    , _key()
{}

BufferMgr::BufferHandle::BufferHandle(BufferKey& k, uint64_t slot, uint64_t gencount, CompressorType cType)
    : _compressorType(cType)
    , _slot(slot)
    , _gencount(gencount)
    , _key(k)
{
    // BufferMgr's slot refers to a valid buffer by now, so the
    // slot's (and this handle's) gencount must be non-zero.
    assert(gencount != NULL_GENCOUNT);
}

//
// BufferKey implementation
//

bool BufferMgr::BufferKey::operator<(const BufferMgr::BufferKey& other) const
{
    if (getDsk() != other.getDsk())
        return getDsk() < other.getDsk();
    else if (getOffset() != other.getOffset())
        return getOffset() < other.getOffset();
    else if (getSize() != other.getSize())
        return getSize() < other.getSize();
    else
        return getAllocSize() < other.getAllocSize();
}

BufferMgr::BufferKey::BufferKey(DataStore::DataStoreKey& dsk, off_t off, size_t sz, size_t alloc)
    : _dsk(dsk)
    , _off(off)
    , _size(sz)
    , _compressedSize(sz)
    , _allocSize(alloc)
{}

// TODO: no coverage, single use only.  BC comments (by email) that the single location should be changed
//       to use the other Ctor and then call setCompressedSize().  Dave and James agree to do this post release
BufferMgr::BufferKey::BufferKey(DataStore::DataStoreKey& dsk, off_t off, size_t sz, size_t alloc, size_t cSize)
    : _dsk(dsk)
    , _off(off)
    , _size(sz)
    , _compressedSize(cSize)
    , _allocSize(alloc)
{}

//
// BufferMgr implementation
//

//
// wrapper for newVector<char> that adds error injection capability
//
template<InjectErrCode INJECT_ERROR_CODE>
char* allocBlockBase(arena::Arena& a, arena::count_t c, int line, const char* file)
{
    if (injectedError<INJECT_ERROR_CODE>(line, file)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MEMORY_ALLOCATION_ERROR)
            << "simulated newVector failure";
    }
    return arena::newVector<char>(a, c);
}

// a scidb function used to invoke BufferMgr::clearCache() from the apply() operator
// (registered in BufferMgr::BufferMgr)
//
static void S_clearCache(const Value** args, Value* res, void*)
{
    res->setInt64(0);  // requires some return value
                       // if in the future, not all can be cleared this way,
                       // perhaps this can change to return the amount still cached
    BufferMgr::getInstance()->clearCache();
}

void BufferMgr::clearCache()
{
    LOG4CXX_TRACE(logger, "BufferMgr::clearCache");
    ScopedMutexLock sm(_blockMutex);

    // Attempt to make as much room in the Cache as the cache size
    if (!_reserveSpace(_usedMemLimit)) {
        LOG4CXX_WARN(logger, "BufferMgr::clearCache: _reserveSpace failed (returned false)");
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "BufferMgr::_reserveSpace";
    }
    LOG4CXX_WARN(logger, "BufferMgr::clearCache: _reserveSpace succeeded, bytesCached: " << _bytesCached);

    ASSERT_EXCEPTION(_bytesCached == 0, "BufferMgr::_bytesCached " << _bytesCached << " not zero");
}

BufferMgr::BufferHandle BufferMgr::allocateBuffer(DataStore::DataStoreKey& dsk,
                                                  size_t sz,
                                                  BufferMgr::WritePriority wp,
                                                  CompressorType compressorType)
{
    LOG4CXX_TRACE(logger, "BufferMgr::allocateBuffer for " << dsk.toString());
    SCIDB_ASSERT(sz);
    SCIDB_ASSERT(compressorType != CompressorType::UNKNOWN);
    ScopedMutexLock sm(_blockMutex);

    // Find a slot in the block list and allocate memory for the buffer.
    BlockPos slot = _allocateBlockHeader(dsk, sz, sz, compressorType);
    // Initialize the slot values, allocate the space in the datastore.
    BlockHeader& bhead = _blockList[slot];
    bhead.dirty = true;
    bhead.wp = wp;
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(dsk);
    bhead.offset = ds->allocateSpace(sz, bhead.allocSize);

    SCIDB_ASSERT(sz == bhead.size);
    LOGCLAIM("SpaceUsed", bhead.size, bhead);
    LOGCLAIM("Pinned", bhead.size, bhead);
    LOGCLAIM("Dirty", bhead.size, bhead);
    _claimSpaceUsed(bhead.size);
    _claimPinnedBytes(bhead.size);
    _claimDirtyBytes(bhead.size);

    if (compressorType != CompressorType::NONE) {
        // We need twice the space if doing compression. Once for the "raw buffer" and
        // once for the "compression buffer". The compression buffer will be no larger then
        // the "raw buffer" so use that size.
        LOGCLAIM("SpaceUsed_compressed", bhead.size, bhead);
        LOGCLAIM("Pinned_compressed", bhead.size, bhead);
        LOGCLAIM("Dirty_compressed", bhead.size, bhead);
        _claimSpaceUsed(bhead.size);
        _claimPinnedBytes(bhead.size);
        _claimDirtyBytes(bhead.size);
    }

    // Enter slot into the DS Block Map
    BufferKey bk(dsk, bhead.offset, bhead.size, bhead.allocSize);
    DSBufferMap::value_type mapentry(bk, slot);
    auto result = _dsBufferMap.insert(mapentry);
    SCIDB_ASSERT(result.second);

    // Return the handle
    BufferHandle bhandle(bk, slot, bhead.genCount, compressorType);
    return bhandle;
}

void BufferMgr::flushAllBuffers(DataStore::DataStoreKey& dsk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::flushAllBuffers (dsk: " << dsk.toString() << ")");
    ScopedMutexLock sm(_blockMutex);

    /* Iterate the buffer list for this dsk.  Ensure that no dirty
       buffers are pinned.  (Clean buffers may be pinned by queries
       holding RD lock that access older array versions.)  For each
       dirty buffer which is not pending io, issue a write job.  At
       end, wait for jobs, then iterate again.  Repeat till no buffers
       are dirty.
     */
    bool dirtyBuffers = true;

    while (dirtyBuffers) {
        dirtyBuffers = false;
        BufferKey bk(dsk, 0, 0, 0);
        DSBufferMap::iterator dbit = _dsBufferMap.lower_bound(bk);
        while (dbit != _dsBufferMap.end() && dbit->first.getDsk() == dsk) {
            BlockPos slot = dbit->second;
            BlockHeader& bhead = _blockList[slot];

            if (bhead.dirty) {

                ASSERT_EXCEPTION(!bhead.pinCount,
                                 "pinned dirty buffer in flushAllBuffers "
                                 << dsk.toString());

                dirtyBuffers = true;
                if (!bhead.pending) {
                    // clang-format off
                    LOG4CXX_TRACE(logger, "BufferMgr::flushAllBuffers, issuing write job {"
                                          << " \"slot\": " << slot
                                          << ", \"bhead\": "<< bhead.toString()
                                          << " }");
                    // clang-format on
                    SCIDB_ASSERT(bhead.blockBase);

                    bhead.pending = true;
                    LOGCLAIM("Pending", bhead.size, bhead);
                    _claimPendingBytes(bhead.size);
                    if (bhead.compressedBlockBase) {
                        LOGCLAIM("Pending_compressed", bhead.size, bhead);
                        _claimPendingBytes(bhead.size);
                    }
                    std::shared_ptr<IoJob> job =
                        make_shared<IoJob>(IoJob::IoType::Write, slot, *this);
                    _jobQueue->pushJob(job);
                }
            }
            ++dbit;
        }
        if (dirtyBuffers) {
            _pendingWriteCond.wait(_blockMutex, PTW_SWT_BUF_CACHE);
        }
        InjectedErrorListener::throwif(__LINE__, __FILE__);
    }

    /* Flush the data store data and metadata to ensure all writes are on
       disk.
     */
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(dsk);

    ds->flush();
}

void BufferMgr::discardBuffer(BufferKey& bk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::discardBuffer (bk: " << bk << ")");
    ScopedMutexLock sm(_blockMutex);

    /* Look for the buffer in the cache
     */
    DSBufferMap::iterator dbit = _dsBufferMap.find(bk);
    if (dbit != _dsBufferMap.end()) {
        /* Found it, clear it out
         */
        BlockPos slot = dbit->second;
        BlockHeader& bhead = _blockList[slot];

        SCIDB_ASSERT(bhead.pinCount == 0);

        // NOTE: With gcc 4.9 this trick of using (void) to silence -Wunused-result no
        // longer works, however _removeBufferFromCache does not use
        // __attribute__((warn_unused_result));
        (void) _removeBufferFromCache(dbit);
    }

    /* Deallocate the space in the backend data store
     */
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(bk.getDsk());

    ds->freeChunk(bk.getOffset(), bk.getAllocSize());
}

void BufferMgr::_discardBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_discardBuffer (bh: " << bh << ")");
    discardBuffer(bh._key);
}

/* Discard all buffers for the given key, and remove the datastore from
   the disk
 */
void BufferMgr::discardAllBuffers(DataStore::DataStoreKey& dsk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::discardAllBuffers (dsk: " << dsk.toString() << ")");
    ScopedMutexLock sm(_blockMutex);

    /* Remove all keys with same dsk from the cache
     */
    BufferKey bk(dsk, 0, 0, 0);
    DSBufferMap::iterator dbit = _dsBufferMap.lower_bound(bk);
    while (dbit != _dsBufferMap.end() && dbit->first.getDsk() == dsk) {
        if (_removeBufferFromCache(dbit++)) {
            // TODO: no coverage
            // Mutex was dropped, _dsBufferMap may have changed radically.
            // Refresh the iterator to start-of-dsk.
            dbit = _dsBufferMap.lower_bound(bk);
        }
    }

    /* Remove the data store from the disk
     */
    DataStores::getInstance()->closeDataStore(dsk, true /*remove from disk */);
}

void BufferMgr::flushMetadata(DataStore::DataStoreKey& dsk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::flushMetadata (dsk: " << dsk.toString() << ")");
    ScopedMutexLock sm(_blockMutex);

    /* It suffices to call flush on the datastore...
     */
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(dsk);
    ds->flush();
}

arena::ArenaPtr BufferMgr::getArena()
{
    return _arena;
}

BufferMgr::BufferMgr()
    : InjectedErrorListener(InjectErrCode::WRITE_CHUNK)
    , _bytesCached(0)
    , _dirtyBytes(0)
    , _pinnedBytes(0)
    , _pendingBytes(0)
    , _reserveReqBytes(0)
    , _usedMemLimit(Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD) * MiB)
{
    _blockList.resize(BufferMgrDefaultListSize);
    _slotMutexes.resize(BufferMgrDefaultListSize);
    _lruBlock = MAX_BLOCK_POS;
    _mruBlock = MAX_BLOCK_POS;
    _freeHead = 0;
    for (size_t i = 0; i < _blockList.size(); i++) {
        /* Set the links for the freelist
         */
        _blockList[i].next = i + 1;
    }
    _blockList[_blockList.size() - 1].next = MAX_BLOCK_POS;

    /* Create the arena for buffers
     */
    _arena = arena::newArena(arena::Options("buffer-manager")
                                 .threading(true)  // thread safe
                                 .resetting(true)  // support en-masse deletion
                                 .recycling(true)  // support eager recycling of sub-allocations
    );

    /* Create the async i/o thread pool and job queue and start the
       threads
     */
    _jobQueue = std::shared_ptr<JobQueue>(new JobQueue("BufferMgr IoQueue"));
    _asyncIoThreadPool = std::shared_ptr<ThreadPool>(
        new ThreadPool(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_THREADS),
                       _jobQueue,
                       "BufferMgr ThreadPool"));
    _asyncIoThreadPool->start();
    InjectedErrorListener::start();

    // register a static function as "test_clear_cache" AFL function
    auto fl = FunctionLibrary::getInstance();
    fl->addFunction(FunctionDescription("test_clear_cache",
                                        vector<TypeId>() /*no args*/,
                                        TypeId(TID_INT64),
                                        &S_clearCache,
                                        (size_t)0));
}

void BufferMgr::getByteCounts(BufferStats& bufStats)
{
    ScopedMutexLock sm(_blockMutex);
    bufStats.cached = _bytesCached;
    bufStats.dirty = _dirtyBytes;
    bufStats.pinned = _pinnedBytes;
    bufStats.pending = _pendingBytes;
    bufStats.reserveReq = _reserveReqBytes;
    bufStats.usedLimit = _usedMemLimit;
}

//
// IoJob implementation
//
BufferMgr::IoJob::IoJob(IoType reqType, BlockPos target, BufferMgr& mgr)
    : Job(BufferMgr::_bufferQuery, "BufMgrIoJob")
    , _reqType(reqType)
    , _targetSlot(target)
    , _buffMgr(mgr)
{}

/* IoJob Implementation
 */
std::shared_ptr<Query> const& BufferMgr::_bufferQuery(NULL);

void BufferMgr::IoJob::run()
{
    switch (_reqType) {
    case IoType::Read:
        ASSERT_EXCEPTION_FALSE("unreachable");
        break;
    case IoType::Write:
        _buffMgr._writeSlotUnlocked(_targetSlot);
        break;
    default:
        ASSERT_EXCEPTION_FALSE("unreachable");
        break;
    }
}

//
// more BufferMgr implementation
// TODO: join up with the other section of these as a non-functional change
//       by moving BlockHeader and IoJob stuff before all BufferMgr stuff
//

BufferMgr::BufferHandle BufferMgr::_pinBufferLockedWithRetry(BufferKey& bk, CompressorType cType)
{
    for (int retries = 0; retries < MAX_PIN_RETRIES; ++retries) {
        try {
            return _pinBufferLocked(bk, cType);
        } catch (RetryPinException& ex) {
            // TODO: no coverage
            // No worries, try again!  Problem has already been logged.
        }
    }

    // TODO: no coverage
    // Bummer, still here.
    throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException);
}

/**
 * Return a buffer handle for the indicated buffer.
 * @pre The _blockMutex is held
 * @post The buffer is returned ''pinned''.
 * @throws OutOfMemoryException if buffer is not cached and no space remains
 */
BufferMgr::BufferHandle BufferMgr::_pinBufferLocked(BufferKey& bk, CompressorType cType)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_pinBufferLocked for bk=" << bk);
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());

    /* Look for the buffer in the cache
     */
    DSBufferMap::iterator dbmit = _dsBufferMap.find(bk);
    if (dbmit != _dsBufferMap.end()) {
        // Found it
        BlockPos slot = dbmit->second;
        BlockHeader& bhead = _blockList[slot];
        BufferHandle bhand(bk, slot, bhead.genCount, cType);

        while (bhead.pinCount == 0 && bhead.pending) {
            // TODO: no coverage, how to force bhead.pending?
            //       try a write followed by a clear_cache
            LOG4CXX_DEBUG(logger, "BufferMgr::_pinBufferLocked waiting for pending write");
            _pendingWriteCond.wait(_blockMutex, PTW_SWT_BUF_CACHE);
        }

        //
        // check for slot contents changing during the wait in the while loop above
        //
        bool hasInjected = hasInjectedError(SLOT_GENCOUNT_CHANGE, __LINE__, __FILE__);
        if (bhead.genCount != bhand.getGenCount() || hasInjected) {
            if (bhead.genCount != bhand.getGenCount()) {
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked actual slot change, slot" << slot);
                if (isDebug()) {
                    dbmit = _dsBufferMap.find(bk);
                    SCIDB_ASSERT(dbmit == _dsBufferMap.end() || dbmit->second != slot);
                }
            } else {
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked injected/simulated slot change");
            }
            throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException);
        }

        _updateLru(slot);
        bhead.pinCount++;
        if (bhead.pinCount == 1) {
            LOGCLAIM("Pinned", bhead.size, bhead);
            _claimPinnedBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Pinned_compressed", bhead.size, bhead);
                _claimPinnedBytes(bhead.size);
            }
        }
        return bhand;

    } else {
        /* The buffer wasn't in the cache, find a space in the block list
         * and resize if necessary
         */
        BlockPos slot = _allocateBlockHeader(bk.getDsk(), bk.getSize(), bk.getCompressedSize(), cType);
        BlockHeader& bhead = _blockList[slot];
        LOGCLAIM("SpaceUsed", bhead.size, bhead);
        LOGCLAIM("Pinned", bhead.size, bhead);
        _claimSpaceUsed(bhead.size);
        _claimPinnedBytes(bhead.size);

        // The _reserveSpace() call, invoked from _allocateBlockHeader(),
        // can release the _blockMutex.  We need to revalidate the dbmit
        // because someone else could have brought in the buffer we seek.
        bool hasInjected = hasInjectedError(SLOT_OTHER_THREAD_LOAD, __LINE__, __FILE__);
        dbmit = _dsBufferMap.find(bk);
        if (dbmit != _dsBufferMap.end() || hasInjected) {
            // The sought-for buffer is now resident, but almost
            // certainly in a different slot, so we need a retry.
            // to undo our work and try to pick up that result
            // The slot we just allocated is no longer needed, oh well.
            if (dbmit != _dsBufferMap.end()) {  // rare case
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked lost race to swap in block " << bk);
            } else {
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked injected SLOT_OTHER_THREAD_LOAD");
            }

            // _dsBufferMap.erase(bk);  no, we never set it

            // unpin the slot
            SCIDB_ASSERT(bhead.pinCount == 1);  // from _allocateBlockHeader()
            bhead.pinCount = 0;

            // _freeSlot() with different bookkeeping
            arena::destroy(*_arena, bhead.blockBase);
            bhead.blockBase = nullptr;
            // Release the bytes which were claimed when the slot was allocated.
            LOGRELEASE("SpaceUsed", bhead.size, bhead);
            LOGRELEASE("Pinned", bhead.size, bhead);
            _releaseSpaceUsed(bhead.size);
            _releasePinnedBytes(bhead.size);
            SCIDB_ASSERT(!bhead.compressedBlockBase);  // not yet allocated

            _markSlotAsFree(slot);
            throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException);
        }

        SCIDB_ASSERT(bk.getSize() == bhead.size);
        SCIDB_ASSERT(bk.getCompressedSize() == bhead.compressedSize);
        SCIDB_ASSERT(bk.getDsk() == bhead.dsk);

        // Update the slot values
        bhead.offset = bk.getOffset();

        // Read the data from the disk
        std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(bhead.dsk);

        if (cType == CompressorType::NONE) {
            // clang-format off
            LOG4CXX_TRACE(logger, "bufferMgr swap in buffer.  "
                          << "[slot=" << slot << "]"
                          << "[offset=" << bhead.offset << "]"
                          << "[dsk=" << bhead.dsk.getNsid() << "," << bhead.dsk.getDsid() << "]"
                          << "[size=" << bhead.size << "]"
                          << "[blockptr=" << (void*) bhead.blockBase << "]");
            // clang-format on
            bhead.allocSize = ds->readData(bhead.offset, bhead.blockBase, bhead.size);
        } else {
            SCIDB_ASSERT(bhead.compressedSize < bhead.size);
            // clang-format off
            LOG4CXX_TRACE(logger, "bufferMgr swap in compressed buffer.  "
                          << "[slot=" << slot << "]"
                          << "[offset=" << bhead.offset << "]"
                          << "[dsk=" << bhead.dsk.getNsid() << "," << bhead.dsk.getDsid() << "]"
                          << "[size=" << bhead.size << "]"
                          << "[compressedSize=" << bhead.compressedSize << "]"
                          << "[blockptr=" << (void*) bhead.blockBase << "]");
            // clang-format on

            // Create a temporary compression buffer in which to read the compressed
            // data from disk.
            try {
                bhead.compressedBlockBase =
                    allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_PBL>(*_arena,
                                                                             bhead.compressedSize,
                                                                             __LINE__,
                                                                             __FILE__);
                // For consistency, we claim these bytes. They will be released
                // immediately after read.
                LOGCLAIM("SpaceUsed_compressed", bhead.size, bhead);
                LOGCLAIM("Pinned_compressed", bhead.size, bhead);
                _claimSpaceUsed(bhead.size);  // For the compressedBlockbase
                _claimPinnedBytes(bhead.size);

            } catch (std::exception& ex) {
                // clang-format off
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked allocBlockBase failed at line " << __LINE__
                                     << " because " << ex.what());
                // clang-format on

                // "_freeSlot with different  bookkeeping" (Possible refactoring location).
                arena::destroy(*_arena, bhead.blockBase);
                bhead.blockBase = nullptr;
                // Release the bytes for the blockbase which were claimed after the slot
                // was allocated.
                LOGRELEASE("SpaceUsed", bhead.size, bhead);
                LOGRELEASE("Pinned", bhead.size, bhead);
                _releaseSpaceUsed(bhead.size);
                _releasePinnedBytes(bhead.size);

                // NOTE: if this happens, we no longer attempt to free the slot, as its remotely
                //       possible that could have a side-effect on a non-compressed use
                //       instead we throw, and the upper level iterator code will free chunks
                //       in the normal ways.
                throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
            }
            SCIDB_ASSERT(bhead.compressedBlockBase);

            bhead.allocSize =
                ds->readData(bhead.offset, bhead.compressedBlockBase, bhead.compressedSize);
            size_t len = CompressorFactory::getInstance()
                             .getCompressor(bhead.compressorType)
                             ->decompress(reinterpret_cast<void*>(bhead.blockBase),
                                          bhead.size,
                                          reinterpret_cast<const void*>(bhead.compressedBlockBase),
                                          bhead.compressedSize);
            arena::destroy(*_arena, bhead.compressedBlockBase);
            bhead.compressedBlockBase = nullptr;
            LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
            LOGRELEASE("Pinned_compressed", bhead.size, bhead);
            _releasePinnedBytes(bhead.size);  // For the compressedBlockBase
            _releaseSpaceUsed(bhead.size);
            if (0 == len || len != bhead.size) {
                // 1. Decompression failed if the len is 0,
                // 2. If the uncompressed size (len) is not what is expected(bhead.size),
                //    then the data on disk was wrong (possibly incorrectly modified and
                //    written in another transaction, which used chunk.getData() when
                //    chunk.getConstData() was proper).
                //
                // The data on disk is corrupt. Check is performed after arena::destroy,
                // and _release..Bytes(), so that memory is returned to the Arena, and the
                // counters are set correctly.
                //
                // And clear the counters for the "raw buffer" (which were claimed after
                //  _allocateBlockHeader), as well.
                //
                // "_freeSlot with different  bookkeeping" (Possible refactoring location).
                arena::destroy(*_arena, bhead.blockBase);
                bhead.blockBase = nullptr;
                // Release the bytes for the blockbase which were claimed after the slot
                // was allocated.
                LOGRELEASE("SpaceUsed", bhead.size, bhead);
                LOGRELEASE("Pinned", bhead.size, bhead);
                _releaseSpaceUsed(bhead.size);
                _releasePinnedBytes(bhead.size);
                if (len == 0) {
                    LOG4CXX_FATAL(logger, "Compressed Data in DataStore is Corrupt"
                                          << " (Unsuccessful decompression): "
                                          << "{ \"blockHeader\": " << bhead.toString() << " }");
                } else {
                    LOG4CXX_FATAL(logger, "Compressed Data in DataStore is Corrupt"
                                          << " (Unexpected Data Written earlier): "
                                          << "{ \"len\": " << len
                                          << " , \"blockHeader\": " << bhead.toString()
                                          << " }");
                }
                SCIDB_ASSERT(false);  // Dump core for debugging purposes.
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CORRUPT_DATA_ON_DISK);
            }
        }
        LOG4CXX_TRACE(logger, "bufferMgr bhead after read: " << bhead.toString());

        // Enter slot into the DS Block Map
        DSBufferMap::value_type mapentry(bk, slot);
        auto result = _dsBufferMap.insert(mapentry);
        SCIDB_ASSERT(result.second);

        // Return the handle
        BufferHandle bhand(bk, slot, bhead.genCount, cType);
        return bhand;
    }
}

/* Reserve Cache Space
   All routines which plan to put more data in the cache should
   first call "reserve(sz)".  This method keeps track of the
   remaining space, and kicks off necessary space freeing
   activities such as ejecting buffers from the LRU list and
   initiating write-back for dirty buffers.
   pre: blockMutex is held
   returns: true if there are sz bytes free in the cache
            false if sz bytes could not be freed
   note: this routine may block waiting for background i/o
         to complete, releasing blockMutex for a while
   note: guarantee of free bytes upon return only lasts as
         long as blockMutex is held
 */
bool BufferMgr::_reserveSpace(size_t sz)
{
    static InjectedErrorListener s_injectErrReserveSpaceFail(InjectErrCode::RESERVE_SPACE_FAIL);
    if (s_injectErrReserveSpaceFail.test(__LINE__, __FILE__)) {
        LOG4CXX_WARN(logger, "BufferMgr::_reserveSpace injected RESERVE_SPACE_FAIL, early return");
        return false;
    }

    LOG4CXX_TRACE(logger, "BufferMgr::_reserveSpace (sz: " << sz << ")");
    /* Calc the space we need to clear.  As long as it is positive, keep
       trying to clear space.
     */
    size_t target = 0;
    unsigned int loopIterations = 0;

    // clang-format off
    while ((target = (_usedMemLimit - _bytesCached < sz ? sz - _usedMemLimit + _bytesCached
                                                        : 0)) > 0) {
        LOG4CXX_TRACE(logger, "BufferMgr::_reserveSpace must clear space."
                      << "[bytescached=" << _bytesCached << "][size req="
                      << sz << "][limit=" << _usedMemLimit << "]"
                      << "[loopIterations=" << loopIterations << "]");
        // clang-format on

        /* First try to eject clean buffers from the LRU list
         */
        if (_ejectCache(target)) {
            ++loopIterations;
            continue;
        }

        target = (_usedMemLimit - _bytesCached < sz ? sz - _usedMemLimit + _bytesCached : 0);

        // clang-format off
        LOG4CXX_TRACE(logger, "BufferMgr::_reserveSpace must destage cache."
                      << "[bytescached=" << _bytesCached << "][size req="
                      << target << "][limit=" << _usedMemLimit << "]"
                      << "[loopIterations=" << loopIterations << "]");
        // clang-format on

        /* Try to destage the cache.
         */
        if (_destageCache(target)) {
            ++loopIterations;
            _waitOnReserveQueue(target);
            continue;
        }

        /* Ejection and destaging failed to clear enough space, return false
         */
        // NOTE: it is not yet safe to inject an error to cause this case
        // because in Debug or Assert build, an unpin() during an interator
        // destruction (boo!) will assert and create a core
        SCIDB_ASSERT(target > 0);
        // clang-format off
        LOG4CXX_ERROR(logger, "BufferMgr::_reserveSpace failed to clear space. returning false."
                               << "[bytescached=" << _bytesCached
                              << "][pendingBytes=" << _pendingBytes
                              << "][dirtyBytes=" << _dirtyBytes
                               << "][bytes left to clear=" << target
                               << "][limit=" << _usedMemLimit << "]"
                               << "[loopIterations=" << loopIterations << "]");
        // clang-format on
        return false;
    }

    SCIDB_ASSERT(target == 0);
    return true;
}

/** Write data for requested slot to data store.
 *
 * Write data in @c target slot, and mark slot as no longer dirty when write complete.
 * Wakeup waiting thread if necessary. Called from async job.
 *
 * @pre blockMutex is NOT held
 */
void BufferMgr::_writeSlotUnlocked(BlockPos targetSlot)
{
    /* Get the header info under lock
     */
    BlockHeader bhcopy;
    {
        ScopedMutexLock sm(_blockMutex);
        BlockHeader& bhead = _blockList[targetSlot];
        bhcopy = bhead;
    }
    // clang-format off
    LOG4CXX_TRACE(logger, "bufferMgr::_writeSlotUnlocked  "
                          << "{ \"slot\": " << targetSlot
                          << " , \"bhcopy\": " << bhcopy.toString()
                          << "}");
    // clang-format on

    OnScopeExit finishAccounting([this, targetSlot, &bhcopy]() {
        /* Mark the buffer as not dirty, signal reserve waiters and
           write waiters
        */
        ScopedMutexLock sm(_blockMutex);
        BlockHeader& bhead = _blockList[targetSlot];
        SCIDB_ASSERT(bhead.genCount == bhcopy.genCount);
        // Confirm compressedBlockBase was updated if re-compression happened.
        SCIDB_ASSERT(bhead.compressedBlockBase == bhcopy.compressedBlockBase);

        bhead.dirty = false;
        bhead.pending = false;
        LOGRELEASE("Dirty", bhead.size, bhead);
        _releaseDirtyBytes(bhead.size);

        if (bhead.compressedBlockBase) {
            LOGRELEASE("Dirty_compressed", bhead.size, bhead);
            _releaseDirtyBytes(bhead.size);
        }

        size_t pending = bhead.size;
        LOGRELEASE("Pending", bhead.size, bhead);
        if (bhead.compressedBlockBase) {
            LOGRELEASE("Pending_compressed", bhead.size, bhead);
            pending += bhead.size;
        }
        _signalReserveWaiters(pending);
        _releasePendingBytes(pending);
        _pendingWriteCond.broadcast();

        if (bhead.compressedBlockBase) {
            // Once the data in the compressedBlockBase are written to the DataStore, only
            // the data in the blockBase is needed for future READ operations.
            // Free the space in the cache taken by the now unnecessary compressedBlockBase.
            arena::destroy(*_arena, bhead.compressedBlockBase);
            bhead.compressedBlockBase = nullptr;
            LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
            _releaseSpaceUsed(bhead.size);
        }
    });

    SCIDB_ASSERT(bhcopy.pinCount == 0);
    SCIDB_ASSERT(bhcopy.pending);
    SCIDB_ASSERT(bhcopy.blockBase);

    try {
        /* Write the data to the data store
         */
        std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(bhcopy.dsk);
        if (CompressorType::NONE == bhcopy.compressorType) {
            ds->writeData(bhcopy.offset, bhcopy.blockBase, bhcopy.size, bhcopy.allocSize);
        } else {
            if (!bhcopy.compressedBlockBase) {
                // The compression should normally occur during NewDBArray::unpinChunk,
                // before the async write IoJob was scheduled in the chunk's unpin call.
                // However a buffer could have been incorrectly marked as "dirty" when
                // READ/WRITE access was requested (via ChunkIter::getWriteData()). In
                // that case the compressedBlockBase will be null when the finial unpin is
                // called. To prevent data loss or corruption, re-compress the blockbase
                // buffer into the compressedBlockBase.

                // clang-format off
                // DO not change the "Re-writing Compressed BlockBase" portion of this log
                // message or  the checkin/storage/badreadwrite test will fail.
                LOG4CXX_WARN(logger, "Re-writing Compressed BlockBase. Bad getWriteData() call?: "
                                     << " { \"bhead\" " << bhcopy.toString()
                                     << " }");
                // clang-format on
                try {
                    bhcopy.compressedBlockBase =
                        allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_WSU>(*_arena,
                                                                                 bhcopy.size,
                                                                                 __LINE__,
                                                                                 __FILE__);
                } catch (std::exception& ex) {
                    // Since we didn't allocate memory for compressedBlockBase, and didn't
                    // update the accounting counters, make certain the machinery won't
                    // decrement too much. There is a precondition in OnScopeExit that
                    // the bhcopy.compressedBlockBase is the same as bhead.compressedBlockBase.
                    bhcopy.compressedBlockBase = nullptr;
                    // clang-format off
                    LOG4CXX_WARN(logger, "BufferMgr::_writeSlotUnlocked allocBlockBase failed at line "
                                         << __LINE__
                                         << " because " << ex.what());
                    // clang-format on
                    throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
                }
                size_t compressedSize;
                compressedSize = CompressorFactory::getInstance()
                                     .getCompressor(bhcopy.compressorType)
                                     ->compress(bhcopy.compressedBlockBase,  // destination
                                                bhcopy.blockBase,            // source buffer
                                                bhcopy.size);                // source size

                if (compressedSize == bhcopy.size || compressedSize != bhcopy.compressedSize) {
                    // compress() failed (the compressedSize is the same as the source size).
                    // or The resulting size doesn't match what the IndexMgr has.
                    // The blockheader says it would succeed and would be
                    // bhcopy.compressedSize in size
                    LOG4CXX_ERROR(logger, "Compression failed, or resulted in unexpected size. {"
                                         << " \"compressedSize\": "<< compressedSize
                                         << " \"expected_size\": " << bhcopy.compressedSize
                                         << " \"bhcopy\": " <<  bhcopy.toString());

                    // We need to free the memory allocated in allocBlockBase, because we
                    // haven't set the original _blockList[targetSlot].compressedBlockBase to
                    // point to the allocated space yet.
                    arena::destroy(*_arena, bhcopy.compressedBlockBase);
                    bhcopy.compressedBlockBase = nullptr;

                    throw SYSTEM_EXCEPTION_SUBCLASS(CompressionFailureException);
                }

                SCIDB_ASSERT(bhcopy.compressedBlockBase);

                // Updating the original _blockList[targetSlot] and counters needs to be
                // under Mutex lock.
                {
                    ScopedMutexLock sm(_blockMutex);
                    BlockHeader& bhead = _blockList[targetSlot];
                    bhead.compressedBlockBase = bhcopy.compressedBlockBase;

                    SCIDB_ASSERT(bhead.genCount == bhcopy.genCount);

                    // Update the Byte Counters: the standard freeing of the slot will
                    // need to decrement counters based upon compressedBlockBase which
                    // is no longer the nullptr.

                    // Space Used,
                    LOGCLAIM("SpaceUsed_compressed", bhcopy.size, bhcopy);
                    _claimSpaceUsed(bhcopy.size);

                    // Dirty
                    LOGCLAIM("Dirty_compressed", bhcopy.size, bhcopy);
                    _claimDirtyBytes(bhcopy.size);

                    // Pinned
                    // The blockbase is not pinned so no more pinned bytes

                    // Pending
                    LOGCLAIM("Pending_compressed", bhcopy.size, bhcopy);
                    _claimPendingBytes(bhcopy.size);
                }
            }
            SCIDB_ASSERT(bhcopy.compressedBlockBase);
            ds->writeData(bhcopy.offset,
                          bhcopy.compressedBlockBase,
                          bhcopy.compressedSize,
                          bhcopy.allocSize);
        }
    } catch (const SystemException& e) {
        // This does not need to worry about de-allocating the compressed block base
        // that may have been allocated by allocBlockBase in this function, because
        // the blockbase will have been set as if it had come into the function
        // already set. So let the system cleans up in same the way as it would normally.

        // Since we have a failure, we need to abort all the currently running queries -- even if
        // they aren't associated with the bad buffer. The bad buffer write could have been
        // for a buffer that was opened for WRITE when it after being opened READ-only, and now
        // it's getting flushed after the fact.
        Query::freeQueriesWithWarning(SCIDB_WARNING(SCIDB_LE_CORRUPT_DATA_ON_DISK));
    } catch (...) {
        LOG4CXX_ERROR(logger, "Unexected exception in BufferMgr::_writeSlotUnlocked. Canceling queries.");
        Query::freeQueriesWithWarning(SCIDB_WARNING(SCIDB_LE_CORRUPT_DATA_ON_DISK));
    }
}

/* Find at least sz bytes worth of dirty buffers and issue write
   requests for them if necessary
   pre: blockMutex is held
   returns: true if successful and sz bytes of dirty buffers are
            queued to write, false otherwise
 */
bool BufferMgr::_destageCache(size_t sz)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "BufferMgr::_destage cache."
                          << "{\"dirtybytes\": " << _dirtyBytes
                          << ", \"size_req\": "<< sz
                          << ", \"limit\": " << _usedMemLimit
                          << "}");
    // clang-format on

    /* If the excess pending bytes above and beyond the reserve
       request is greater than sz, we're done!
       The in-flight bytes (pending) will cover the number of needed bytes,
       so don't eject more than is necessary from the cache.
     */
    SCIDB_ASSERT(_pendingBytes >= _reserveReqBytes);
    if ((_pendingBytes - _reserveReqBytes) > sz) {
        LOG4CXX_TRACE(logger, "Sufficient Inflight bytes...returning. "
                              << "{"
                              << " \"size_req\": "<< sz
                              << " ,  \"_pendingBytes\": " << _pendingBytes
                              << " , \"_reserveReqBytes\": " << _reserveReqBytes
                              << " , \"inflightBytes\": " << (_pendingBytes - _reserveReqBytes)
                              << "}");
        // TODO: no coverage, sz bytes available
        return true;
    }

    /* Adjust the target by the excess
     */
    sz -= (_pendingBytes - _reserveReqBytes);

    /* Walk the lru list to find dirty buffers
     */
    BlockPos currentSlot = _lruBlock;
    while (sz > 0) {
        // clang-format off
            LOG4CXX_TRACE(logger, "BufferMgr::_destage cache "
                          << "swap out loop.  [sz=" << sz << "][currentslot="
                          << currentSlot << "]");
        // clang-format on

        static InjectedErrorListener s_injectErrBlockListEnd(InjectErrCode::BLOCK_LIST_END);
        bool injectErr = s_injectErrBlockListEnd.test(__LINE__, __FILE__);
        if (currentSlot >= _blockList.size() || injectErr) {
            if (injectErr) {
                LOG4CXX_WARN(logger, "BufferMgr::_destageCache, injected BLOCK_LIST_END");
            }
            // End of the lru list, return false
            SCIDB_ASSERT(sz > 0);
            return false;
        }

        BlockHeader& bhead = _blockList[currentSlot];

        if ((bhead.pinCount == 0) && (bhead.dirty) && (!bhead.pending)) {
            /* Send write request for dirty buffer
             */
            // clang-format off
            LOG4CXX_TRACE(logger, "BufferMgr::destageCache, pincount 0 on dirty buffer, "
                                  << "issuing write job: "
                                  << "{\"slot\":" << currentSlot
                                  << " , \"size\": " << bhead.size
                                  << " , \"csize\": " << bhead.compressedSize
                                  << " , \"bhead\":" << bhead.toString()
                                  << "}");
            // clang-format on
            SCIDB_ASSERT(bhead.blockBase);

            bhead.pending = true;
            LOGCLAIM("Pending", bhead.size, bhead);
            _claimPendingBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Pending_compressed", bhead.size, bhead);
                _claimPendingBytes(bhead.size);
            }
            std::shared_ptr<IoJob> job =
                make_shared<IoJob>(IoJob::IoType::Write, currentSlot, *this);
            _jobQueue->pushJob(job);
            sz = (bhead.size > sz) ? 0 : sz - bhead.size;
            if (bhead.compressedBlockBase) {
                sz = (bhead.size > sz) ? 0 : sz - bhead.size;
            }
        }
        currentSlot = bhead.next;
    }
    SCIDB_ASSERT(sz <= 0);
    return true;
}

/* Find at least sz bytes worth of unpinned clean buffers and
   eject them from the cache.
   pre: blockMutex is held
   returns: true if sz bytes were cleared, false otherwise
*/
bool BufferMgr::_ejectCache(size_t sz)
{
    // clang-format off
        LOG4CXX_TRACE(logger, "BufferMgr::_ejectCache " << "][size req=" << sz);
    // clang-format on

    /* Walk the lru list
     */
    BlockPos currentSlot = _lruBlock;
    while (sz > 0) {
        // clang-format off
            LOG4CXX_TRACE(logger, "BufferMgr::_ejectCache "
                          << "swap out loop.  [sz=" << sz << "][currentslot="
                          << currentSlot << "]");
        // clang-format on

        if (currentSlot >= _blockList.size()) {
            /* End of the lru list, return false
             */
            SCIDB_ASSERT(sz > 0);
            return false;
        }

        BlockHeader& bhead = _blockList[currentSlot];

        if ((bhead.pinCount == 0) && (!bhead.dirty)) {
            /* Eject clean, unpinned buffer
             */
            size_t amt_ejected = bhead.size;
            if (bhead.compressedBlockBase != nullptr) {
                amt_ejected += bhead.size;
                // note coverage of BUFFER_RESERVE_FAIL followed by clear_cache (BufferMgr_inject.test)
                LOG4CXX_WARN(logger, "BufferMgr::_ejectCache: compressedBlockBase on unpinned, clean buffer");
            }
            sz = (amt_ejected > sz) ? 0 : sz - amt_ejected;

            BlockPos next = bhead.next;
            BufferKey bk(bhead.dsk, bhead.offset, bhead.size, bhead.allocSize);
            _dsBufferMap.erase(bk);

            _freeSlot(currentSlot);  // factored from here and _removeBufferFromCache()
            _markSlotAsFree(currentSlot);
            currentSlot = next;
            LOG4CXX_TRACE(logger, "BufferMgr::_ejectCache {\"ejected\": " << amt_ejected
                                  << ", \"newsize\": " << sz
                                  << ", \"bhead\": "<< bhead.toString()
                                  << "}");
        } else {
            /* Skip entries that are pinned or dirty,
               go to the next in the LRU
             */
            LOG4CXX_TRACE(logger, "BufferMgr::_ejectCache Nothing to eject: "
                                  << "{\"bhead\": " << bhead.toString()
                                  << "}");
            currentSlot = bhead.next;
        }
    }

    SCIDB_ASSERT(sz == 0);
    return true;
}

/* Mark a certain amount of space as being used in the cache
   pre: blockMutex is held
 */
void BufferMgr::_claimSpaceUsed(size_t sz)
{
    _bytesCached += sz;
    // clang-format off
        LOG4CXX_TRACE(logger, "BufferMgr::_claimSpaceUsed request: "
                      << sz << " bytes cached(post): " << _bytesCached
                      << " limit: " << _usedMemLimit);
    // clang-format on
    ASSERT_EXCEPTION(_bytesCached <= _usedMemLimit, "Cache size exceeds limit in claim.");
}

/* Mark a certain amount of space as being un-used
   pre: blockMutex is held
 */
void BufferMgr::_releaseSpaceUsed(size_t sz)
{
    ASSERT_EXCEPTION(_bytesCached >= sz, "BufferMgr::_releaseSpaceUsed, more released than used");
    _bytesCached -= sz;
    // clang-format off
    LOG4CXX_TRACE(logger, "BufferMgr::_releaseSpaceUsed request: " << sz
                          << " bytes cached(post): " << _bytesCached
                          << " limit: " << _usedMemLimit);
    // clang-format on
    ASSERT_EXCEPTION(_bytesCached <= _usedMemLimit, "Cache size wrap-around in release.");
}

/* Mark a certain amount of space as being dirty in the cache
   pre: blockMutex is held
 */
void BufferMgr::_claimDirtyBytes(size_t sz)
{
    _dirtyBytes += sz;
    LOG4CXX_TRACE(logger,
                  "BufferMgr::_claimDirtyBytes request: " << sz << " dirty bytes(post): "
                                                          << _dirtyBytes);
    ASSERT_EXCEPTION(_dirtyBytes <= _usedMemLimit, "Dirty bytes exceeds limit in claim.");
}

/* Mark a certain amount of space as being not dirty in the cache
   pre: blockMutex is held
*/
void BufferMgr::_releaseDirtyBytes(size_t sz)
{
    _dirtyBytes -= sz;
    LOG4CXX_TRACE(logger,
                  "BufferMgr::_releaseDirtyBytes request: "
                      << sz << " dirty bytes(post): " << _dirtyBytes);
    ASSERT_EXCEPTION(_dirtyBytes <= _usedMemLimit, "Dirty bytes wrap-around in release.");
}

/* Mark a certain amount of space as being pinned in the cache
   pre: blockMutex is held
 */
void BufferMgr::_claimPinnedBytes(size_t sz)
{
    _pinnedBytes += sz;
    LOG4CXX_TRACE(logger, "BufferMgr::_claimPinnedBytes request: " << sz
                  << " pinned bytes(post): " << _pinnedBytes);
    ASSERT_EXCEPTION(_pinnedBytes <= _usedMemLimit, "Pinned bytes exceeds limit in claim.");
}

/* Mark a certain amount of space as being unpinned in the cache
   pre: blockMutex is held
 */
void BufferMgr::_releasePinnedBytes(size_t sz)
{
    _pinnedBytes -= sz;
    LOG4CXX_TRACE(logger,
                  "BufferMgr::_releasePinnedBytes request: "
                      << sz << " pinned bytes(post): " << _pinnedBytes);
    ASSERT_EXCEPTION(_pinnedBytes <= _usedMemLimit, "Pinned bytes wrap-around in release.");
}

/* Mark a certain amount of space as pending a write
   pre: blockMutex is held
 */
void BufferMgr::_claimPendingBytes(size_t sz)
{
    _pendingBytes += sz;
    // clang-format off
        LOG4CXX_TRACE(logger, "BufferMgr::_claimPendingBytes request: "
                      << sz << " bytes pending(post): " << _pendingBytes
                      << " reserve: " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqBytes <= _pendingBytes, "Reserve request exceeds pending in claim.");
}

/* Mark a certain amount of space as being no longer pending write
   pre: blockMutex is held
 */
void BufferMgr::_releasePendingBytes(size_t sz)
{
    _pendingBytes -= sz;
    // clang-format off
        LOG4CXX_TRACE(logger, "BufferMgr::_releasePendingBytes request: "
                      << sz << " bytes pending(post): " << _pendingBytes
                      << " reserve: " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_pendingBytes <= _usedMemLimit, "Pending bytes wrap-around in release.");
}

/* Record an increase in the requested reserve
   pre: blockMutex is held
 */
void BufferMgr::_claimReserveReqBytes(size_t sz)
{
    _reserveReqBytes += sz;
    // clang-format off
        LOG4CXX_TRACE(logger, "BufferMgr::_claimReserveReqBytes request: "
                      << sz << " bytes pending(post): " << _pendingBytes
                      << " reserve: " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqBytes <= _pendingBytes, "Reserve request exceeds pending in claim.");
}

/* Record a decrease in the requested reserve
   pre: blockMutex is held
*/
void BufferMgr::_releaseReserveReqBytes(size_t sz)
{
    _reserveReqBytes -= sz;
    // clang-format off
        LOG4CXX_TRACE(logger, "BufferMgr::_releaseReserveReqBytes request: "
                      << sz << " bytes pending(post): " << _pendingBytes
                      << " reserve: " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqBytes <= _pendingBytes, "Reserve request exceeds pending in release.");
}

/* Wait on the reserve queue until sz bytes have been written back
   to disk.
   pre: blockMutex is held
 */
void BufferMgr::_waitOnReserveQueue(size_t sz)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_waitOnReserveQueue request: " << sz << " bytes");

    auto req = std::make_shared<SpaceRequest>(sz);
    _reserveQueue.push(req);
    BlockHeader dummy;
    LOGCLAIM("ReserveReqBytes", sz, dummy);
    _claimReserveReqBytes(sz);

    while (!req->done) {
        req->ready.wait(_blockMutex, PTW_SWT_BUF_CACHE);
    }
}

/* Release waiters on reserve queue when sz bytes have been cleaned
   or unpinned.
   pre: blockMutex is held
 */
void BufferMgr::_signalReserveWaiters(size_t sz)
{
    /* Keep signalling waiters until none are left or
       we have run out of space
     */
    BlockHeader dummy;
    while (_reserveQueue.size() > 0 && sz > 0) {
        if (_reserveQueue.front()->size > sz) {
            /* The next waiter needs more space, reduce its
               claim and exit
             */
            _reserveQueue.front()->size -= sz;
            LOGRELEASE("ReserveReqBytes", sz, dummy);
            _releaseReserveReqBytes(sz);
            SCIDB_ASSERT(_reserveReqBytes > 0);
            break;
        } else {
            /* There is enough space to service the next
               waiter.  Signal it, remove it from the queue
               and reduce the remaining space
             */
            auto req = _reserveQueue.front();
            _reserveQueue.pop();
            sz -= req->size;
            SCIDB_ASSERT(_reserveReqBytes >= req->size);
            LOGRELEASE("ReserveReqBytes", req->size, dummy);
            _releaseReserveReqBytes(req->size);
            req->done = true;
            req->ready.signal();
        }
    }
}

void BufferMgr::BlockHeader::reset(const DataStore::DataStoreKey& dataStoreKey,
                                   char* blockBasePtr,
                                   size_t blockSize,
                                   size_t compressedBlockSize,
                                   CompressorType blockCompressorType)
{
    pinCount = 1;
    genCount++;

    blockBase = blockBasePtr;
    size = blockSize;
    compressedSize = compressedBlockSize;
    compressorType = blockCompressorType;
    dsk = dataStoreKey;
}

/**
 * @brief Allocate and initialize a block header from the free list.
 *
 * @param dsk The DataStoreKey used for lookup
 * @param size The size of the blockBase memory region
 * @param compressedSize The size of the compressedBlockBase region
 * @param compressorType The desired compression method for the block
 * @return the slot number of the corresponding block header.
 */
BufferMgr::BlockPos BufferMgr::_allocateBlockHeader(const DataStore::DataStoreKey& dsk,
                                                    size_t size,
                                                    size_t compressedSize,
                                                    CompressorType compressorType)
{
    size_t reserveSpace = size;
    if (compressorType != CompressorType::NONE) {
        // Allocate space for the "tag-along" compression buffer. It will be no larger
        // than the size of the "raw buffer." Do not use compressedSize because other
        // bookkeeping areas don't necessarily know what the compressed size is. All
        // counters increment/decrement bytecounts by bhead.size for the compression
        // buffer.
        reserveSpace += size;
    }
    // Make room in the cache if necessary
    if (!_reserveSpace(reserveSpace)) {
        LOG4CXX_WARN(logger, "BufferMgr::_allocateBlockHeader _reserveSpace fail, line "
                     << __LINE__ << " reached");
        throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
    }

    char* blockBasePtr = nullptr;
    try {
        // NOTE: Do not allocate memory for the compressionBuffer here.
        // The compressionBuffer will be created elsewhere in a just-in-time manner.
        blockBasePtr = allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_ABH>(*_arena, size, __LINE__, __FILE__);
    } catch (std::exception& ex) {
        LOG4CXX_WARN(logger, "BufferMgr::_allocateBlockHeader allocBlockBase failed: " << ex.what());
        throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
    }
    SCIDB_ASSERT(blockBasePtr);

    BlockPos slot = BufferHandle::INVALID_SLOT;
    try {
        slot = _allocateFreeSlot();
    } catch (...) {
        arena::destroy(*_arena, blockBasePtr);
        throw;
    }
    auto& bhead = _blockList[slot];
    bhead.reset(dsk, blockBasePtr, size, compressedSize, compressorType);

    // block is allocated when bhead.priority != 0
    SCIDB_ASSERT(bhead.priority);

    return slot;
}

/* Allocate a free slot in the block list and return its position, resize
   the list if necessary
   pre: blockMutex is held
 */
BufferMgr::BlockPos BufferMgr::_allocateFreeSlot()
{
    LOG4CXX_TRACE(logger, "BufferMgr::_allocateFreeSlot()");
    /* Find a spot on the free list
     */
    if (_freeHead >= _blockList.size()) {
        /* Need to expand the blocklist
         */
        BlockPos oldsize = _blockList.size();
        BlockPos newsize = oldsize * 2;

        LOG4CXX_TRACE(logger, "BufferMgr::_allocateFreeSlot() resizing to " << newsize);

        _blockList.resize(newsize);
        _freeHead = oldsize;
        for (size_t i = oldsize; i < newsize; i++) {
            _blockList[i].next = i + 1;
            SCIDB_ASSERT(_blockList[i].priority == 0);
        }
        _blockList[newsize - 1].next = MAX_BLOCK_POS;
    }

    ASSERT_EXCEPTION(_freeHead < _blockList.size(), "failed to create free block header");

    /* Remove from the freelist, reset to defaults, and mark as in-use.
     */
    BlockPos slot = _freeHead;
    BlockHeader& bhead = _blockList[slot];
    SCIDB_ASSERT(bhead.priority == 0);
    SCIDB_ASSERT(bhead.blockBase == nullptr);
    SCIDB_ASSERT(bhead.compressedBlockBase == nullptr);

    _freeHead = bhead.next;
    auto gencount = bhead.genCount;
    bhead = BlockHeader();      // reset to defaults
    bhead.priority = 1;         // mark as in-use
    bhead.genCount = gencount;

    /* Add to the lru
     */
    _addToLru(slot);

    // clang-format off
    LOG4CXX_TRACE(logger, "bufferMgr allocate free slot [slot=" << slot << "]"
                          << " ... [bhead: " << bhead.toString() << "]");
    // clang-format on
    return slot;
}

/* Mark the indicated slot as free
   pre: blockMutex is held
 */
void BufferMgr::_markSlotAsFree(BlockPos slot)
{
    LOG4CXX_TRACE(logger, "bufferMgr::_markSlotAsFree  [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot to mark as free");

    /* Remove from the lru
     */
    _removeFromLru(slot);

    BlockHeader& bhead = _blockList[slot];
    SCIDB_ASSERT(bhead.blockBase == nullptr);
    SCIDB_ASSERT(bhead.compressedBlockBase == nullptr);

    /* Mark free
     */
    SCIDB_ASSERT(bhead.pinCount == 0);
    bhead.priority = 0;
    bhead.dirty = false;

    /* Add to the free list
     */
    bhead.next = _freeHead;
    bhead.prev = MAX_BLOCK_POS;
    _freeHead = slot;
}

/* Remove slot from LRU
   pre: blockMutex is held
 */
void BufferMgr::_removeFromLru(BlockPos slot)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_removeFromLru [slot=" << slot << "]");

    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot to remove from lru");

    BlockHeader& bhead = _blockList[slot];

    /* Remove from the lru
     */
    if (_lruBlock == _mruBlock) {
        ASSERT_EXCEPTION(slot == _lruBlock, "corrupt lru list in mark as free");
        _lruBlock = _mruBlock = MAX_BLOCK_POS;
    } else if (_lruBlock == slot) {
        _lruBlock = bhead.next;
        _blockList[_lruBlock].prev = MAX_BLOCK_POS;
    } else if (_mruBlock == slot) {
        _mruBlock = bhead.prev;
        _blockList[_mruBlock].next = MAX_BLOCK_POS;
    } else {
        _blockList[bhead.prev].next = bhead.next;
        _blockList[bhead.next].prev = bhead.prev;
    }

    bhead.prev = MAX_BLOCK_POS;
    bhead.next = MAX_BLOCK_POS;
}

/* Move slot to the most recently used block
   pre: blockMutex is held
   pre: block is on the lru
 */
void BufferMgr::_updateLru(BlockPos slot)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_updateLru [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot in updateLru");

    _removeFromLru(slot);
    _addToLru(slot);
}

/* Add slot to end of LRU list (mru block)
   pre: blockMutex is held
   pre: block is not on the lru
 */
void BufferMgr::_addToLru(BlockPos slot)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_addToLru [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot in addToLru");

    BlockHeader& bhead = _blockList[slot];

    ASSERT_EXCEPTION(bhead.priority, "free slot in addToLru");

    bhead.next = MAX_BLOCK_POS;
    bhead.prev = MAX_BLOCK_POS;
    bhead.prev = _mruBlock;
    if (_mruBlock < _blockList.size()) {
        _blockList[_mruBlock].next = slot;
    } else {
        ASSERT_EXCEPTION(_lruBlock == MAX_BLOCK_POS, "corrupt lru list in _updateLru");
        _lruBlock = slot;
    }
    _mruBlock = slot;
}

/* Deallocate memory for a buffer, remove it from the indices, and
   mark the slot as free
   pre: blockMutex is held
   note: can drop and re-acquire blockMutex
   returns: true iff mutex was dropped
*/
bool BufferMgr::_removeBufferFromCache(DSBufferMap::iterator dbit)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());

    bool droppedMutex = false;
    BufferKey bk = dbit->first;
    BlockPos slot = dbit->second;

    LOG4CXX_TRACE(logger, "BufferMgr::_removeBufferFromCache [slot:" << slot << "]"
                  << ", " << _blockList[slot].toString());

    int retries = 0;
    do {
        BlockHeader& bhead = _blockList[slot];
        auto savedGenCount = bhead.genCount;

        // Wait for pending write...
        while (bhead.pending) {
            // TODO: no coverage, pending write?  store followed by clear_cache?
            // clang-format off
            LOG4CXX_DEBUG(logger, "BufferMgr::_removeBufferFromCache " <<
                          "waiting for pending write");
            // clang-format on
            droppedMutex = true;
            _pendingWriteCond.wait(_blockMutex, PTW_SWT_BUF_CACHE);
        }

        if (bhead.genCount == savedGenCount) {
            // Did not wait, or if we did, desired buffer is still in this slot.
            SCIDB_ASSERT(!bhead.pending);
            break;
        }
        SCIDB_ASSERT(droppedMutex);

        // TODO: no coverage
        // The slot contents changed while we were waiting, so we must
        // re-validate the buffer map iterator.
        dbit = _dsBufferMap.find(bk);
        if (dbit == _dsBufferMap.end()) {
            // Desired BufferKey is no longer in the cache, some other
            // thread did our work for us.
            LOG4CXX_TRACE(logger, "BufferMgr::_removeBufferFromCache " <<
                          "buffer removed while waiting for pending write");
            return droppedMutex;
        }

        // Refresh slot, the BufferKey we sought has moved.
        slot = dbit->second;
        LOG4CXX_WARN(logger, "BufferMgr::_removeBufferFromCache(): retrying due to slot change");
    } while (++retries < MAX_PIN_RETRIES); // TODO: not really a pinning retry

    // We tried to wait for the buffer to quiesce MAX_PIN_RETRIES times,
    // but it moved to a different slot each time!
    //
    // "Quiesce" means we're trying to "pin-for-remove": we want the
    // target buffer in a known slot, with _blockMutex held, no pending
    // writes, and zero pinCount.  Though we're not actually pinning,
    // the error text for RetryPinException is accurate so I didn't
    // bother to rename it (LostRaceException?).
    //
    if (retries == MAX_PIN_RETRIES) {
        // TODO: no coverage
        throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException);
    }

    BlockHeader& bhead = _blockList[slot];

    ASSERT_EXCEPTION(!bhead.pinCount, "pinned buffer in removeBufferFromCache");

    _dsBufferMap.erase(dbit);

    _freeSlot(slot);  // factored from here and _ejectCache

    _markSlotAsFree(slot);

    return droppedMutex;
}

/* Does the deallocation of the blocks allocated to the slot.
   The key should be erased from _dsBufferMap before calling this
   method (if it was ever entered... in allocation failures, that
   has typically not taken place yet).
   a. change the genCount (because we're modifying the slot)
   b. deallocate blockBase (and adjust bookkeeping)
   c. if present deallocate compressedBlockBase (and adjust bookkeeping)
   NOTE: its possible this should be merged with _markSlotAsFree
*/
void BufferMgr::_freeSlot(const BlockPos& slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());

    BlockHeader& bhead = _blockList[slot];
    ASSERT_EXCEPTION(!bhead.pinCount, "BufferMgr::_freeSlot(), buffer still pinned");

    // A. we are changing the slot, so up the gen count.
    bhead.genCount++;

    // B. deallocate bhead.blockBase
    arena::destroy(*_arena, bhead.blockBase);
    bhead.blockBase = nullptr;
    if (bhead.dirty) {
        LOGRELEASE("Dirty", bhead.size, bhead);
        _releaseDirtyBytes(bhead.size);
    }
    LOGRELEASE("SpaceUsed", bhead.size, bhead);
    _releaseSpaceUsed(bhead.size);

    // Destroy the compressed buffers as well  (Don't check
    // compressorType, it may have been set to NONE in unpin.)
    if (bhead.compressedBlockBase != nullptr) {
        arena::destroy(*_arena, bhead.compressedBlockBase);
        bhead.compressedBlockBase = nullptr;
        // When allocateBuffer was called, the compressedSize was unknown, so bhead.size
        // bytes were claimed. Release the same amount that was claimed.
        if (bhead.dirty) {
            LOGRELEASE("Dirty_compressed", bhead.size, bhead);
            _releaseDirtyBytes(bhead.size);
        }
        LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
        _releaseSpaceUsed(bhead.size);
    }
}

/* Return the pointer to the raw data. Buffer MUST be pinned.
   Throws ReadOnlyBufferException if buffer is not dirty and
   pinned by multiple callers.
   POST: Buffer is marked dirty.
*/
PointerRange<char> BufferMgr::_getRawData(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_getRawData for " << bh);

    ScopedMutexLock sm(_blockMutex);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in getRawData");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _getRawData");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _getRawData");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _getRawData");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _getRawData");

    if (!bhead.dirty) {
        if (bhead.pinCount == 1) {
            bhead.dirty = true;
            LOGCLAIM("Dirty", bhead.size, bhead);
            _claimDirtyBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Dirty_compressed", bhead.size, bhead);
                _claimDirtyBytes(bhead.size);
            }
        } else {
            // TODO: no coverage, want an ASSERT_UNREACHED(specific exception)
            assert(false);
            throw SYSTEM_EXCEPTION_SUBCLASS(ReadOnlyBufferException);
        }
    }

    LOG4CXX_TRACE(logger, "BufferMgr::_getRawData: [slot: " << bh._slot << " ]"
                          << "[bhead: " << bhead.toString() << ']');

    SCIDB_ASSERT(bhead.blockBase);
    return PointerRange<char>(bhead.size, bhead.blockBase);
}

/**
 * Return the pointer range to the buffer to be used for compression.
 * The (uncompressed) buffer MUST be pinned and dirty.
 *
 * @note This should only be called in the "write" path, since the compressed buffer is
 * needed until replication and the asynchronous write of the chunk in the buffer is
 * complete.
 */
PointerRange<char> BufferMgr::_getCompressionBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_getCompressionBuffer for" << bh);
    ScopedMutexLock sm(_blockMutex);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in _getCompressionBuffer");

    BlockHeader& bhead = _blockList[bh._slot];

    SCIDB_ASSERT(bhead.dirty);
    SCIDB_ASSERT(bhead.pinCount);

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _getCompressionBuffer");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.dirty, "Pristine buffer in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.compressorType != CompressorType::NONE, "Superfluous buffer in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.compressorType != CompressorType::UNKNOWN, "Bad cType in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.blockBase, "No raw buffer in _getCompressionBuffer");

    // See BufferMgr::allocateBuffer for how blockbase was allocated.  Create a buffer in which
    // to store the compressed buffer, which has the same size as the original raw data buffer
    // since we don't yet know how great or small the space savings will be.
    LOG4CXX_TRACE(logger, "Compression Type is " << static_cast<uint32_t>(bhead.compressorType));

    // Check if the CompressionBuffer already exists, and only allocate memory for a new one
    // when we need to. The compression buffer is created initially with the same size as the
    // raw data (uncompressed)  buffer.
    if (!bhead.compressedBlockBase) {
        SCIDB_ASSERT(bhead.size == bhead.compressedSize);
        size_t sz = bhead.size;

        try {
            bhead.compressedBlockBase =
                allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_GCB>(*_arena, sz, __LINE__, __FILE__);
        } catch (std::exception& ex) {
            LOG4CXX_WARN(logger, "BufferMgr::_getCompressionBuffer allocBlockBase failed at line " << __LINE__
                                 << " because " << ex.what());

            // Release bytes that were allocated for the compressionBuffer in allocateBuffer()
            LOGRELEASE("Dirty_compressed", bhead.size, bhead);
            LOGRELEASE("Pinned_compressed", bhead.size, bhead);
            LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
            _releaseDirtyBytes(bhead.size);
            _releasePinnedBytes(bhead.size);
            _releaseSpaceUsed(bhead.size);

            throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
        }
        SCIDB_ASSERT(bhead.compressedBlockBase);
    }

    // This will be returned to NewDbArray::unpinChunk() so that the compressed
    // information can be put into this buffer.  Note that once the compression happens
    // the compressedSize -will- change (<= otherwise we have yet another issue), and so
    // must be updated in the chunk descriptor metadata value.

    return PointerRange<char>(bhead.compressedSize, bhead.compressedBlockBase);
}

void BufferMgr::_setCompressedSize(BufferMgr::BufferHandle& bh, size_t sz)
{
    ScopedMutexLock sm(_blockMutex);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in _setCompressionBuffer");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _setCompressedSize");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _setCompressedSize");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _setCompressedSize");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _setCompressedSize");

    bhead.compressedSize = sz;
}

/* Return the pointer to the const raw data.  Buffer MUST be pinned.
 */
PointerRange<const char> BufferMgr::_getRawConstData(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_getRawConstData for " << bh);
    ScopedMutexLock sm(_blockMutex);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in getRawConstData");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _getRawConstData");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _getRawConstData");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _getRawConstData");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _getRawConstData");

    LOG4CXX_TRACE(logger, "BufferMgr::_getRawConstData: [slot: " << bh._slot << " ]"
                          << "[bhead: " << bhead.toString() << ']');

    SCIDB_ASSERT(bhead.blockBase);
    return PointerRange<const char>(bhead.size, bhead.blockBase);
}

/* Pin buffer again.
   Initial pin happens on allocation.
 */
void BufferMgr::_pinBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_pinBuffer for " << bh);

    ScopedMutexLock sm(_blockMutex);

    /* Verify that the slot in the BufferHandle is still valid
       and matches the slot in the DSBufferMap.
     */
    BlockPos dsbmSlot = BufferHandle::INVALID_SLOT;
    DSBufferMap::iterator dbit = _dsBufferMap.find(bh.getKey());
    if (dbit != _dsBufferMap.end()) {
        dsbmSlot = dbit->second;
    }

    if (bh._slot < _blockList.size() && bh._slot == dsbmSlot) {
        BlockHeader& bhead = _blockList[bh._slot];

        if (bhead.genCount != BufferHandle::NULL_GENCOUNT && bhead.genCount == bh._gencount) {
            SCIDB_ASSERT(bh.getCompressorType() == bhead.compressorType);
            SCIDB_ASSERT(bhead.compressorType != CompressorType::UNKNOWN);
            SCIDB_ASSERT(bhead.blockBase);
            SCIDB_ASSERT(bhead.priority);
            while (bhead.pinCount == 0 && bhead.pending) {
                // TODO: no coverage, unpinned pending write?
                // clang-format off
                LOG4CXX_DEBUG(logger, "BufferMgr::_pinBuffer " << "waiting for pending write");
                // clang-format on
                _pendingWriteCond.wait(_blockMutex, PTW_SWT_BUF_CACHE);
            }

            // Waiting on _pendingWriteCond gives up the mutex, so
            // the handle may have become stale.
            if (bhead.genCount == bh._gencount) {
                // Nope, handle still good.  Pin it!
                bhead.pinCount++;
                if (bhead.pinCount == 1) {
                    LOGCLAIM("Pinned", bhead.size, bhead);
                    _claimPinnedBytes(bhead.size);
                    if (bhead.compressedBlockBase) {
                        LOGCLAIM("Pinned_compressed", bhead.size, bhead);
                        _claimPinnedBytes(bhead.size);
                    }
                }
                return;
            }

            // TODO: no coverage
            LOG4CXX_DEBUG(logger, "BufferMgr::_pinBuffer handle went stale waiting on pending write");
        }

        // Still here?  Handle was stale.
        LOG4CXX_TRACE(logger,
                      "Stale Handle. Re-pin the buffer: "
                      << "{ \"bh\": "<< bh
                      << ", \"bhead\": " << bhead.toString()
                      << "}");
    }

    /* Stale handle, re-pin the buffer
     */
    CompressorType pinCType = bh.getCompressorType();
    SCIDB_ASSERT(pinCType != CompressorType::UNKNOWN);
    bh = _pinBufferLockedWithRetry(bh._key, pinCType);
}

/* Unpin the buffer.  Cannot use raw pointer after this unless pinBuffer()
   is called.
 */
void BufferMgr::_unpinBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_unpinBuffer for " << bh);

    ScopedMutexLock sm(_blockMutex);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in unpinBuffer");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _unpinBuffer");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _unpinBuffer");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _unpinBuffer");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _unpinBuffer");

    bhead.pinCount--;

    if (bhead.compressorType != bh.getCompressorType()) {
        // The blockheader had the "desired compression type" for the chunk when it was
        // initialized, but it is possible that compression failed. When compression
        // fails, the bufferhandle is updated, so the blockheader needs to be updated
        // accordingly. The value in the blockheader is used when writing to the datastore
        // to determine whether to write the compressed or uncompressed buffer of the chunk.
        // (see NewDBArray::unpinChunk.)
        // @see SDB-5964 for a better proposal to handle this.
        LOG4CXX_TRACE(logger, "BufferMgr::_unpinBuffer updating blockheader's compressorType"
                              << " from " << bhead.compressorType
                              << " to " << bh.getCompressorType()
                              << "{bhead: " << bhead.toString()
                              << "}, "
                              << "{bhand: " << bh << "}");

        bhead.compressorType = bh.getCompressorType();
    }

    if (bhead.pinCount == 0) {
        /* Pin count going to zero.  Update pinned bytes, schedule
           an i/o if required
         */
        LOGRELEASE("Pinned", bhead.size, bhead);
        _releasePinnedBytes(bhead.size);
        if (bhead.compressedBlockBase) {
            LOGRELEASE("Pinned_compressed", bhead.size, bhead);
            _releasePinnedBytes(bhead.size);
        }

        if (bhead.dirty && bhead.wp == WritePriority::Immediate && !bhead.pending) {
            // TODO: no coverage, WritePriority::Immediate
            // clang-format off
                LOG4CXX_TRACE(logger, "bufferMgr: unpin buffer, " <<
                              "pincount 0 on Immediate priority buffer," <<
                              "issuing write job" <<
                              "[slot=" << bh._slot << "]");
            // clang-format on

            bhead.pending = true;
            LOGCLAIM("Pending", bhead.size, bhead);
            _claimPendingBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Pending_compressed", bhead.size, bhead);
                _claimPendingBytes(bhead.size);
            }
            SCIDB_ASSERT(bhead.blockBase);
            std::shared_ptr<IoJob> job =
                make_shared<IoJob>(IoJob::IoType::Write, bh._slot, *this);
            _jobQueue->pushJob(job);
        }

        /* If the buffer was not dirty (we are unpinning clean data),
           then try to signal reserve waiters, they can use this space
         */
        if (!bhead.dirty) {
            size_t reserveRelease = bhead.size;
            if (bhead.compressedBlockBase) {
                reserveRelease += bhead.size;
            }
            _signalReserveWaiters(reserveRelease);
        }
    }
}

}  // namespace scidb
