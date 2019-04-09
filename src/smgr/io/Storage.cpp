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
 * @file
 *
 * @brief Storage implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 * @author sfridella@paradigm4.com
 */

// stdc++lib
#include <limits>
#include <map>
#include <memory>
#include <unordered_set>

// linux and packages
#include <sys/time.h>
#include <inttypes.h>
#include <log4cxx/logger.h>
#include <city.h>

// scidb
#include <array/Metadata.h>
#include <array/ArrayDistribution.h>
#include <array/CompressedBuffer.h>
#include <array/TileIteratorAdaptors.h>

#include <monitor/InstanceStats.h>
#include <monitor/MonitorConfig.h>
#include <network/MessageDesc.h>
#include <network/MessageUtils.h>
#include <query/Operator.h>

#include <smgr/io/InternalStorage.h>
#include <system/Cluster.h>
#include <system/Utils.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>

#include <util/FileIO.h>
#include <util/OnScopeExit.h>
#include <util/PerfTime.h>
#include <util/Platform.h>


namespace scidb
{

using namespace boost;
using namespace std;

///////////////////////////////////////////////////////////////////
/// Constants and #defines
///////////////////////////////////////////////////////////////////

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr"));
static log4cxx::LoggerPtr chunkLogger(log4cxx::Logger::getLogger("scidb.smgr.chunk"));

const size_t DEFAULT_TRANS_LOG_LIMIT = 1024; // default limit of transaction log file (in mebibytes)
const size_t MAX_CFG_LINE_LENGTH = 1*KiB;
const int MAX_INSTANCE_BITS = 10; // 2^MAX_INSTANCE_BITS = max number of instances

///////////////////////////////////////////////////////////////////
/// Static helper functions
///////////////////////////////////////////////////////////////////

/**
 * Fibonacci hash for a 64 bit key
 * @param key to hash
 * @param fib_B = log2(max_num_of_buckets)
 * @return hash = bucket index
 */
static uint64_t fibHash64(const uint64_t key, const uint64_t fib_B)
{
    assert(fib_B < 64);
    const uint64_t fib_A64 = (uint64_t) 11400714819323198485U;
    return (key * fib_A64) >> (64 - fib_B);
}

inline static char* strtrim(char* buf)
{
    char* p = buf;
    char ch;
    while ((unsigned char) (ch = *p) <= ' ' && ch != '\0')
    {
        p += 1;
    }
    char* q = p + strlen(p);
    while (q > p && (unsigned char) q[-1] <= ' ')
    {
        q -= 1;
    }
    *q = '\0';
    return p;
}

inline static string relativePath(const string& dir, const string& file)
{
    return file[0] == '/' ? file : dir + file;
}

inline static double getTimeSecs()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return (((double) tv.tv_sec) * 1000000 + ((double) tv.tv_usec)) / 1000000;
}

/* Accumulate the uaid/aid (base and version array ids) of a version that
   should be rolled back.
 */
static void collectArraysToRollback(
    std::shared_ptr<Storage::RollbackMap>& arrsToRollback,
    const VersionID& lastVersion,
    const ArrayID& baseArrayId,
    const ArrayID& versionArrayId)
{
    assert(arrsToRollback);
    assert(baseArrayId>0);
    (*arrsToRollback.get())[baseArrayId] = std::make_pair(versionArrayId, lastVersion);
}

VersionControl* VersionControl::instance;

///////////////////////////////////////////////////////////////////
/// ChunkInitializer
///////////////////////////////////////////////////////////////////

CachedStorage::ChunkInitializer::~ChunkInitializer()
{
    ScopedMutexLock cs(storage._mutex, PTW_SML_STOR_V);
    storage.notifyChunkReady(chunk);
}

///////////////////////////////////////////////////////////////////
/// CachedStorage class
///////////////////////////////////////////////////////////////////

/* Constructor
 */
CachedStorage::CachedStorage()
:
    InjectedErrorListener(InjectErrCode::WRITE_CHUNK),
    _loadEvent(),
    _cacheOverflowEvent(),
    _replicationManager(NULL)
{}

/* Initialize/read the Storage Description file on startup
 */
void
CachedStorage::initStorageDescriptionFile(const std::string& storageDescriptorFilePath)
{
    InjectedErrorListener::start();
    char buf[MAX_CFG_LINE_LENGTH];
    char const* descPath = storageDescriptorFilePath.c_str();
    size_t pathEnd = storageDescriptorFilePath.find_last_of('/');
    _databasePath = "";
    if (pathEnd != string::npos)
    {
        _databasePath = storageDescriptorFilePath.substr(0, pathEnd + 1);
    }
    FILE* f = scidb::fopen(descPath, "r");
    if (f == NULL)
    {
        f = scidb::fopen(descPath, "w");
        if (!f)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << descPath << ferror(f);
        size_t fileNameBeg = (pathEnd == string::npos) ? 0 : pathEnd + 1;
        size_t fileNameEnd = storageDescriptorFilePath.find_last_of('.');
        if (fileNameEnd == string::npos || fileNameEnd < fileNameBeg)
        {
            fileNameEnd = storageDescriptorFilePath.size();
        }
        string databaseName = storageDescriptorFilePath.substr(fileNameBeg, fileNameEnd - fileNameBeg);
        _databaseHeader = _databasePath + databaseName + ".header";
        _databaseLog = _databasePath + databaseName + ".log";
        scidb::fprintf(f, "%s.header\n", databaseName.c_str());
        scidb::fprintf(f, "%ld %s.log\n", (long) DEFAULT_TRANS_LOG_LIMIT, databaseName.c_str());
        _logSizeLimit = (uint64_t) DEFAULT_TRANS_LOG_LIMIT * MiB;
    }
    else
    {
        int pos;
        long sizeMb;
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        _databaseHeader = relativePath(_databasePath, strtrim(buf));
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        if (sscanf(buf, "%ld%n", &sizeMb, &pos) != 1)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        _databaseLog = relativePath(_databasePath, strtrim(buf + pos));
        _logSizeLimit = (uint64_t) sizeMb * MiB;
    }
    scidb::fclose(f);
}

/* Record an extent in the extent map
 */
void
CachedStorage::recordExtent(Extents& extents,
                            std::shared_ptr<PersistentChunk>& chunk)
{
    if (_skipChunkmapIntegrityCheck)
    {
        return;
    }

    ChunkExtent ext;
    const ChunkHeader& hdr = chunk->getHeader();

    ext = std::make_tuple(hdr.pos.dsid,
                          hdr.pos.offs,
                          hdr.allocatedSize,
                          hdr.pos.hdrPos);
    if (!extents.insert(ext).second)
    {
        assert(false);
    }
}

/* Erase an extent from the extent map
 */
void
CachedStorage::eraseExtent(Extents& extents,
                           std::shared_ptr<PersistentChunk>& chunk)
{
    if (_skipChunkmapIntegrityCheck)
    {
        return;
    }

    ChunkExtent ext;
    const ChunkHeader& hdr = chunk->getHeader();

    ext = std::make_tuple(hdr.pos.dsid,
                          hdr.pos.offs,
                          hdr.allocatedSize,
                          hdr.pos.hdrPos);
    extents.erase(ext);
}

/* Check extent map for overlaps on disk.  If in "recovery mode"
   replace overlapping chunks with tombstones.  If not in "recovery
   mode" throw exception.  Delete extent map when done.
 */
void
CachedStorage::checkExtentsForOverlaps(Extents& extents)
{
    if (_skipChunkmapIntegrityCheck)
    {
        return;
    }

    Extents::iterator ext_it = extents.begin();
    bool hasCurrent = false;
    ChunkExtent currExt;
    set<uint64_t> overlaps;

    /* Process the extents and check for overlaps
     */
    while (ext_it != extents.end())
    {
        if (!hasCurrent)
        {
            /* Starting a new extent
             */
            currExt = *ext_it;
            hasCurrent = true;
            extents.erase(ext_it);
            ext_it = extents.begin();
        }
        else
        {
            /* Still working on an extent
             */
            DataStore::DsId currDsid = std::get<0>(currExt);
            DataStore::DsId extDsid = std::get<0>(*ext_it);
            off_t currOff = std::get<1>(currExt);
            off_t extOff = std::get<1>(*ext_it);
            size_t currLen = std::get<2>(currExt);
            size_t extLen = std::get<2>(*ext_it);

            if (currDsid != extDsid ||
                extOff >= currOff + (off_t)currLen)
            {
                /* Found a new extent
                 */
                hasCurrent = false;
            }
            else
            {
                /* Found an overlap
                 */
                overlaps.insert(std::get<3>(currExt));
                overlaps.insert(std::get<3>(*ext_it));
                if (currOff + currLen < extOff + extLen)
                {
                    currExt = *ext_it;
                }
                extents.erase(ext_it);
                ext_it = extents.begin();
            }
        }
    }

    /* If overlaps were present log them and decide what to do
     */
    if (overlaps.size())
    {
        LOG4CXX_ERROR(logger, "smgr open:  found overlapping chunks in chunkmap: ");

        set<uint64_t>::iterator over_it;
        for (over_it = overlaps.begin();
             over_it != overlaps.end();
             ++over_it)
        {
            ChunkDescriptor desc;
            std::stringstream ss;

            size_t rc = _hd->read(&desc, sizeof(ChunkDescriptor), *over_it);
            SCIDB_ASSERT(rc == sizeof(ChunkDescriptor));

            ss<< "    [dsguid=" << desc.hdr.pos.dsid <<
                "] [offset=" << desc.hdr.pos.offs <<
                "] [hdrpos=" << desc.hdr.pos.hdrPos <<
                "] [len=" << desc.hdr.allocatedSize <<
                "] [arrayid=" << desc.hdr.arrId <<
                "] [attrid=" << desc.hdr.attId <<
                "] [coords=";
            for (uint16_t i=0;
                 (i < desc.hdr.nCoordinates) &&
                 (i < MAX_NUM_DIMS_SUPPORTED);
                 ++i)
            {
                ss << desc.coords[i] << " ";
            }
            ss << "]";
            LOG4CXX_ERROR(logger, ss.str());

            if (_enableChunkmapRecovery)
            {
                /* In reovery mode, mark all attribute chunks at this position
                   as a tombstone
                 */
                LOG4CXX_ERROR(logger,
                    "    marking position for overlapping chunk as tombstone.");

                ChunkMap::iterator cmiter = _chunkMap.find(desc.hdr.pos.dsid);
                ASSERT_EXCEPTION((cmiter != _chunkMap.end()),
                                 "Attempt to create tombstone for unkown array");
                std::shared_ptr<InnerChunkMap> inner = cmiter->second;
                InnerChunkMap::iterator mapiter;
                StorageAddress addr;

                desc.getAddress(addr);

                for (addr.attId = 0;
                     (mapiter = inner->find(addr)) != inner->end();
                     addr.attId++)
                {
                    mapiter->second.getChunk().reset();
                    mapiter->second.setTombstonePos(InnerChunkMapEntry::INVALID,
                                                    desc.hdr.pos.hdrPos);
                }
            }
        }

        if (!_enableChunkmapRecovery)
        {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_DATABASE_HEADER_CORRUPTED);
        }
    }
}

/* Initialize the chunk map from on-disk store
 */
void
CachedStorage::initChunkMap()
{
    LOG4CXX_TRACE(logger, "smgr open:  reading chunk map, nchunks " << _hdr.nChunks);

    _redundancyEnabled = true;
    _syncReplication = !Config::getInstance()->getOption<bool> (CONFIG_ASYNC_REPLICATION);
    _enableChunkmapRecovery =
        Config::getInstance()->getOption<bool> (CONFIG_ENABLE_CHUNKMAP_RECOVERY);
    _skipChunkmapIntegrityCheck =
        Config::getInstance()->getOption<bool> (CONFIG_SKIP_CHUNKMAP_INTEGRITY_CHECK);

    ChunkDescriptor desc;
    uint64_t chunkPos = HEADER_SIZE;
    StorageAddress addr;
    set<ArrayID> removedArrays;
    typedef map<ArrayID, ArrayID> ArrayMap;
    ArrayMap oldestVersions;
    typedef map<ArrayID, std::shared_ptr<ArrayDesc> > ArrayDescCache;
    ArrayDescCache existentArrays;
    Extents extents;

    for (size_t i = 0; i < _hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor))
    {
        size_t rc = _hd->read(&desc, sizeof(ChunkDescriptor), chunkPos);
        if (rc != sizeof(ChunkDescriptor))
        {
            LOG4CXX_ERROR(logger, "Inconsistency in storage header: rc="
                          << rc << ", chunkPos="
                          << chunkPos << ", i="
                          << i << ", hdr.nChunks="
                          << _hdr.nChunks << ", hdr.currPos="
                          << _hdr.currPos);
            _hdr.currPos = chunkPos;
            _hdr.nChunks = i;
            break;
        }
        if (desc.hdr.pos.hdrPos != chunkPos)
        {
            LOG4CXX_ERROR(logger, "Invalid chunk header " << i << " at position " << chunkPos
                          << " desc.hdr.pos.hdrPos=" << desc.hdr.pos.hdrPos
                          << " arrayID=" << desc.hdr.arrId
                          << " hdr.nChunks=" << _hdr.nChunks);
            _freeHeaders.insert(chunkPos);
        }
        else
        {
            assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);

            if (desc.hdr.arrId != 0)
            {
                /* Check if unversioned array exists
                 */
                ArrayDescCache::iterator it = existentArrays.find(desc.hdr.pos.dsid);
                if (it == existentArrays.end())
                {
                    if (removedArrays.count(desc.hdr.pos.dsid) == 0)
                    {
                        try
                        {
                            std::shared_ptr<ArrayDesc> ad =
                                SystemCatalog::getInstance()->getArrayDescForDsId(desc.hdr.pos.dsid);
                            it = existentArrays.insert(
                                ArrayDescCache::value_type(desc.hdr.pos.dsid, ad)
                                ).first;
                        }
                        catch (SystemException const& x)
                        {
                            if (x.getLongErrorCode() == SCIDB_LE_ARRAYID_DOESNT_EXIST)
                            {
                                removedArrays.insert(desc.hdr.pos.dsid);
                            }
                            else
                            {
                                throw x;
                            }
                        }
                    }
                }

                /* If the unversioned array does not exist... wipe the chunk
                 */
                if (it == existentArrays.end())
                {
                    desc.hdr.arrId = 0;
                    LOG4CXX_TRACE(chunkLogger,
                                  "chunkl: initchunkmap: remove chunk desc "
                                  << "for non-existant array at position "
                                  << chunkPos);
                    _hd->writeAll(&desc.hdr, sizeof(ChunkHeader), chunkPos);
                    assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                    _freeHeaders.insert(chunkPos);
                    continue;
                }

                /* Else add chunk to map (if it is live)
                 */
                else
                {
                    /* Init array descriptor
                     */
                    ArrayDesc& adesc = *it->second;
                    assert(adesc.getUAId() == desc.hdr.pos.dsid);

                    /* Find/init the inner chunk map
                     */
                    ChunkMap::iterator iter = _chunkMap.find(adesc.getUAId());
                    if (iter == _chunkMap.end())
                    {
                        iter = _chunkMap.insert(make_pair(adesc.getUAId(),
                                                          make_shared <InnerChunkMap> ())).first;
                    }
                    std::shared_ptr<InnerChunkMap>& innerMap = iter->second;

                    /* Find the oldest version of array, and the storage address
                       of the chunk currently in use by this version
                    */
                    ArrayMap::iterator oldest_it = oldestVersions.find(adesc.getUAId());
                    if (oldest_it == oldestVersions.end())
                    {
                        oldestVersions[adesc.getUAId()] =
                            SystemCatalog::getInstance()->getOldestArrayVersion(adesc.getUAId());
                    }
                    desc.getAddress(addr);
                    StorageAddress oldestVersionAddr = addr;
                    oldestVersionAddr.arrId = oldestVersions[adesc.getUAId()];
                    StorageAddress oldestLiveChunkAddr;
                    InnerChunkMap::iterator oldestLiveChunk =
                        innerMap->lower_bound(oldestVersionAddr);
                    if (oldestLiveChunk == innerMap->end() ||
                        oldestLiveChunk->first.coords != oldestVersionAddr.coords ||
                        oldestLiveChunk->first.attId != oldestVersionAddr.attId)
                    {
                        oldestLiveChunkAddr = oldestVersionAddr;
                        oldestLiveChunkAddr.arrId = 0;
                    }
                    else
                    {
                        oldestLiveChunkAddr = oldestLiveChunk->first;
                    }

                    /* Chunk is live if and only if arrayID of chunk is > arrayID of chunk
                       currently pointed to by oldest version
                    */
                    if (desc.hdr.arrId > oldestLiveChunkAddr.arrId)
                    {
                        /* Chunk is live, put it in the map
                         */
                        std::shared_ptr<PersistentChunk>& chunk =(*innerMap)[addr].getChunk();
                        ASSERT_EXCEPTION((!chunk), "smgr open: NOT unique chunk");
                        if (!desc.hdr.is<ChunkHeader::TOMBSTONE>())
                        {
                            onCreateChunk(adesc, desc, addr);
                            chunk.reset(new PersistentChunk());
                            chunk->setAddress(adesc, desc);
                            recordExtent(extents, chunk);
                        }
                        else
                        {
                            (*innerMap)[addr].setTombstonePos(
                                    InnerChunkMapEntry::TOMBSTONE,
                                    desc.hdr.pos.hdrPos);
                        }

                        /* Now check if by inserting this chunk we made the previous one dead...
                         */
                        if (oldestLiveChunkAddr.arrId &&
                            desc.hdr.arrId <= oldestVersionAddr.arrId)
                        {
                            /* The oldestLiveChunk is now dead... wipe it out
                             */
                            DataStore::DataStoreKey dsk(_dsnsid,
                                                        desc.hdr.pos.dsid);
                            std::shared_ptr<DataStore> ds =
                                _datastores->getDataStore(dsk);
                            if (!oldestLiveChunk->second.isTombstone())
                            {
                                eraseExtent(extents,
                                            oldestLiveChunk->second.getChunk());
                            }
                            markChunkAsFree(oldestLiveChunk->second, ds);
                            innerMap->erase(oldestLiveChunk);
                        }
                    }
                    else
                    {
                        /* Chunk is dead, wipe it out
                         */
                        DataStore::DataStoreKey dsk(_dsnsid,
                                                    desc.hdr.pos.dsid);
                        std::shared_ptr<DataStore> ds =
                            _datastores->getDataStore(dsk);
                        desc.hdr.arrId = 0;
                        LOG4CXX_TRACE(chunkLogger, "chunkl: initchunkmap: "
                                      << "remove dead chunk desc for non-existent "
                                      << "array version at position " << chunkPos);
                        _hd->writeAll(&desc.hdr, sizeof(ChunkHeader), chunkPos);
                        assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                        _freeHeaders.insert(chunkPos);
                        ds->freeChunk(desc.hdr.pos.offs, desc.hdr.allocatedSize);
                    }
                }
            }
            else
            {
                _freeHeaders.insert(chunkPos);
            }
        }
    }

    /* Perform some simple validation for storage header
     */
    if (chunkPos != _hdr.currPos)
    {
        LOG4CXX_ERROR(logger, "Storage header is not consistent: " << chunkPos << " vs. " << _hdr.currPos);
        _hdr.currPos = chunkPos;
    }

    /* Run through removed arrays and try to remove the datastores (if they
       exist)
     */
    set<ArrayID>::iterator remit = removedArrays.begin();
    while (remit != removedArrays.end())
    {
        DataStore::DataStoreKey dsk(_dsnsid, *remit);
        _datastores->closeDataStore(dsk,
                                    true /* remove from disk */);
        ++remit;
    }

    /* Check chunkmap for overlaps...
     */
    checkExtentsForOverlaps(extents);
}

/* Read the storage description file to find path for chunk map file.
   Iterate the chunk map file and build the chunk map in memory.
 */
void
CachedStorage::open(const string& storageDescriptorFilePath, size_t cacheSizeBytes)
{
    /* read/create the storage description file
     */
    initStorageDescriptionFile(storageDescriptorFilePath);

    /* init cache
     */
    _cacheSize = cacheSizeBytes;
    _compressors = CompressorFactory::getInstance().getCompressors();
    _cacheUsed = 0;
    _strictCacheLimit = Config::getInstance()->getOption<bool> (CONFIG_STRICT_CACHE_LIMIT);
    _cacheOverflowFlag = false;
    _timestamp = 1;
    _lru.prune();

    /* Open metadata (chunk map) file and transcation log file
     */
    int flags = O_LARGEFILE | O_RDWR | O_CREAT;
    _hd = FileManager::getInstance()->openFileObj(_databaseHeader.c_str(), flags);
    if (!_hd) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) <<
            _databaseHeader << ::strerror(errno) << errno;
    }

    struct flock flc;
    flc.l_type = F_WRLCK;
    flc.l_whence = SEEK_SET;
    flc.l_start = 0;
    flc.l_len = 1;

    if (_hd->fsetlock(&flc))
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_LOCK_DATABASE);

    bool isLogEnabled = Config::getInstance()->getOption<bool>(CONFIG_STORAGE_LOG_ENABLE);
    if (isLogEnabled) {
        _log[0] = FileManager::getInstance()->openFileObj((_databaseLog + "_1").c_str(),
                                                          O_LARGEFILE | O_SYNC | O_RDWR | O_CREAT);
        if (!_log[0]) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) <<
                (_databaseLog + "_1") << ::strerror(errno) << errno;
        }

        _log[1] = FileManager::getInstance()->openFileObj((_databaseLog + "_2").c_str(),
                                                          O_LARGEFILE | O_SYNC | O_RDWR | O_CREAT);
        if (!_log[1]) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) <<
                (_databaseLog + "_2") << ::strerror(errno) << errno;
        }
    }

    _logSize = 0;
    _currLog = 0;

    /* Initialize the data stores
     */
    _datastores = DataStores::getInstance();
    string dataStoresBase = _databasePath + "/datastores";
    _datastores->initDataStores(dataStoresBase.c_str());
    _dsnsid = _datastores->openNamespace("persistent");

    /* Read/initialize metadata header
     */
    size_t rc = _hd->read(&_hdr, sizeof(_hdr), 0);
    if (rc != 0 && rc != sizeof(_hdr)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "read" << ::strerror(errno) << errno;
    }

    _writeLogThreshold = Config::getInstance()->getOption<int> (CONFIG_IO_LOG_THRESHOLD);
    _enableDeltaEncoding = Config::getInstance()->getOption<bool> (CONFIG_ENABLE_DELTA_ENCODING);

    // disable replication during rollback: each instance is perfroming rollback locally
    _redundancyEnabled = false;

    if (rc == 0 || (_hdr.magic == SCIDB_STORAGE_HEADER_MAGIC && _hdr.currPos < HEADER_SIZE))
    {
        LOG4CXX_TRACE(logger, "smgr open:  initializing storage header");

        /* Database is not initialized
         */
        ::memset(&_hdr, 0, sizeof(_hdr));
        _hdr.magic = SCIDB_STORAGE_HEADER_MAGIC;
        _hdr.versionLowerBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.versionUpperBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.currPos = HEADER_SIZE;
        _hdr.instanceId = INVALID_INSTANCE;
        _hdr.nChunks = 0;
    }
    else
    {
        LOG4CXX_TRACE(logger, "smgr open:  opening storage header");

        /* Check for corrupted metadata file
         */
        if (_hdr.magic != SCIDB_STORAGE_HEADER_MAGIC)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_STORAGE_HEADER);
        }

        /* At the moment, both upper and lower bound versions in the file must equal to the
           current version in the code.
         */
        if (_hdr.versionLowerBound != SCIDB_STORAGE_FORMAT_VERSION ||
            _hdr.versionUpperBound != SCIDB_STORAGE_FORMAT_VERSION)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_MISMATCHED_STORAGE_FORMAT_VERSION)
                  << _hdr.versionLowerBound
                  << _hdr.versionUpperBound
                  << SCIDB_STORAGE_FORMAT_VERSION;
        }

        /* Rollback uncommitted changes
         */
        doTxnRecoveryOnStartup();

        /* Database is initialized: read information about all locally available chunks in map
         */
        initChunkMap();

        /* Flush the datastores to capture freelist changes
         */
        _datastores->flushAllDataStores();
    }
}


/* Cleanup and close smgr
 */
void
CachedStorage::close()
{
    InjectedErrorListener::stop();

    for (ChunkMap::iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        std::shared_ptr<InnerChunkMap> & innerMap = i->second;
        for (InnerChunkMap::iterator j = innerMap->begin(); j != innerMap->end(); ++j)
        {
            if (j->second.getChunk() && j->second.getChunk()->_accessCount != 0)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_PIN_UNPIN_DISBALANCE);
        }
    }
    _chunkMap.clear();

    _hd.reset();

    bool isLogEnabled = Config::getInstance()->getOption<bool>(CONFIG_STORAGE_LOG_ENABLE);
    if(isLogEnabled) {
        _log[0].reset();
        _log[1].reset();
    }
}

void CachedStorage::notifyChunkReady(PersistentChunk& chunk)
{
    // This method is invoked with storage mutex locked
    chunk._raw = false;
    if (chunk._waiting)
    {
        chunk._waiting = false;
        _loadEvent.signal(); // wakeup all threads waiting for this chunk
    }
}

void CachedStorage::pinChunk(PersistentChunk const* aChunk)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_B);
    PersistentChunk& chunk = *const_cast<PersistentChunk*>(aChunk);
    LOG4CXX_TRACE(logger, "CachedStorage::pinChunk =" << &chunk << ", accessCount = "<<chunk._accessCount);
    chunk.beginAccess();
}

void CachedStorage::unpinChunk(PersistentChunk const* aChunk)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_B);
    PersistentChunk& chunk = *const_cast<PersistentChunk*>(aChunk);
    LOG4CXX_TRACE(logger, "CachedStorage::unpinChunk =" << &chunk << ", accessCount = "<<chunk._accessCount);
    assert(chunk._accessCount > 0);
    if (--chunk._accessCount == 0)
    {
        // Chunk is not accessed any more by any thread, unpin it and include in LRU list
        _lru.link(&chunk);
    }
}

void CachedStorage::addChunkToCache(PersistentChunk& chunk)
{
    // Check amount of memory used by cached chunks and discard least recently used
    // chunks from the cache
    _mutex.checkForDeadlock();
    while (_cacheUsed + chunk.getSize() > _cacheSize)
    {
        if (_lru.isEmpty())
        {
            if (_strictCacheLimit && _cacheUsed != 0)
            {
                Event::ErrorChecker noopEc;
                _cacheOverflowFlag = true;
                _cacheOverflowEvent.wait(_mutex, noopEc, PTW_EVENT_SM_CMEM); // wait for cache mem to free up
            }
            else
            {
                break;
            }
        }
        internalFreeChunk(*_lru._prev);
    }

    LOG4CXX_TRACE(logger, "CachedStorage::addChunkToCache chunk=" << &chunk
                      << ", size = "<< chunk.getSize() << ", accessCount = "<<chunk._accessCount
                      << ", cacheUsed="<<_cacheUsed);

    _cacheUsed += chunk.getSize();
}

std::shared_ptr<PersistentChunk>
CachedStorage::lookupChunk(ArrayDesc const& desc, StorageAddress const& addr)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_C);
    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter != _chunkMap.end())
    {
        std::shared_ptr<InnerChunkMap>& innerMap = iter->second;
        InnerChunkMap::iterator innerIter = innerMap->find(addr);
        if (innerIter != innerMap->end())
        {
            std::shared_ptr<PersistentChunk>& chunk = innerIter->second.getChunk();
            if (chunk)
            {
                chunk->beginAccess();
                return chunk;
            }
        }
    }
    std::shared_ptr<PersistentChunk> emptyChunk;
    return emptyChunk;
}

void CachedStorage::decompressChunk(ArrayDesc const& desc, PersistentChunk* chunk, CompressedBuffer const& src)
{
    chunk->allocate(src.getDecompressedSize());

    DBArrayChunkInternal intChunk(desc, chunk);
    if (src.getSize() != src.getDecompressedSize())
    {
        _compressors[src.getCompressionMethod()]->decompress(intChunk, src.getConstData(), src.getSize());
    }
    else
    {
        assert(chunk->getHeader().pos.hdrPos == 0);
        memcpy(intChunk.getDataForLoad(), src.getConstData(), src.getSize());
    }
}

void CachedStorage::compressChunk(ArrayDesc const& desc, PersistentChunk const* aChunk, CompressedBuffer& dst)
{
    assert(aChunk);
    PersistentChunk& chunk = *const_cast<PersistentChunk*>(aChunk);
    DataStore::DataStoreKey dsk(_dsnsid, desc.getUAId());
    std::shared_ptr<DataStore> ds = _datastores->getDataStore(dsk);
    CompressorType compressionMethod = chunk.getCompressionMethod();
    // CachedStorage is deprecated by the new storage manager.
    // Fixing this just so it compiles is a waste of time.
    // if (compressionMethod < 0) {
    //     throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_COMPRESS_METHOD_NOT_DEFINED);
    // }
    dst.setDecompressedSize(chunk.getSize());
    dst.setCompressionMethod(compressionMethod);
    {
        ScopedMutexLock cs(_mutex, PTW_SML_STOR_D);
        if (!chunk.isRaw() && chunk._data != NULL)
        {
            PersistentChunk::Pinner scope(&chunk);
            dst.allocate(chunk.getCompressedSize() != 0 ? chunk.getCompressedSize() : chunk.getSize());
            DBArrayChunkInternal intChunk(desc, &chunk);
            size_t compressedSize = _compressors[compressionMethod]->compress(dst.getWriteData(),
                                                                              intChunk,
                                                                              intChunk.getSize());
            if (compressedSize == chunk.getSize())
            {
                memcpy(dst.getWriteData(), chunk._data, compressedSize);
            }
            else if (compressedSize != dst.getSize())
            {
                dst.reallocate(compressedSize);
            }
        }
    }

    if (dst.getConstData() == NULL)
    { // chunk data is not present in the cache so read compressed data from the disk
        if (aChunk->_hdr.pos.hdrPos == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_ACCESS_TO_RAW_CHUNK) << aChunk->getHeader().arrId;
        }
        dst.allocate(aChunk->getCompressedSize());
        readChunkFromDataStore(*ds, *aChunk, dst.getWriteData());
    }
}

inline bool CachedStorage::isResponsibleFor(ArrayDesc const& desc,
                                            PersistentChunk const& chunk,
                                            std::shared_ptr<Query> const& query)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_E);
    Query::validateQueryPtr(query);

    size_t redundancy = desc.getDistribution()->getRedundancy();

    if (chunk._hdr.instanceId == _hdr.instanceId)
    {
        return true;
    }
    if (!query->isPhysicalInstanceDead(chunk._hdr.instanceId))
    {
        return false;
    }

    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, desc, chunk.getAddress());
    for (size_t i = 1; i <= redundancy; ++i)
    {
        if (replicas[i] == _hdr.instanceId)
        {
            return true;
        }
        if (!query->isPhysicalInstanceDead(replicas[i]))
        {
            // instance with this replica is alive
            return false;
        }
    }
    return false;
}

std::shared_ptr<PersistentChunk> CachedStorage::createChunk(ArrayDesc const& desc,
                                                              StorageAddress const& addr,
                                                              int compressionMethod,
                                                              const std::shared_ptr<Query>& query)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_F);
    Query::validateQueryPtr(query);

    assert(desc.getUAId()!=0);
    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        iter = _chunkMap.insert(make_pair(desc.getUAId(), make_shared <InnerChunkMap> ())).first;
    }
    else if (iter->second->find(addr) != iter->second->end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(addr.coords);
    }

    std::shared_ptr<PersistentChunk>& chunk = (*(iter->second))[addr].getChunk();
    chunk.reset(new PersistentChunk());
    chunk->setAddress(desc, addr, compressionMethod);
    LOG4CXX_TRACE(chunkLogger, "chunkl: createchunk: chunk created for addr "
                  << "[attrid=" << addr.attId << "][arrid=" << addr.arrId
                  << "]" << CoordsToStr(addr.coords));
    chunk->_accessCount = 1; // newly created chunk is pinned
    chunk->_timestamp = ++_timestamp;
    return chunk;
}

void CachedStorage::deleteChunk(ArrayDesc const& desc, PersistentChunk& victim)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_G);

    ChunkMap::const_iterator iter = _chunkMap.find(desc.getUAId());
    if (iter != _chunkMap.end())
    {
        iter->second->erase(victim._addr);
    }
}

void CachedStorage::freeChunk(PersistentChunk* victim)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_H);
    internalFreeChunk(*victim);
}

void CachedStorage::internalFreeChunk(PersistentChunk& victim)
{
    if (victim._data != NULL && victim._hdr.pos.hdrPos != 0)
    {
        LOG4CXX_TRACE(logger, "CachedStorage::internalFreeChunk chunk=" << &victim
                      << ", size = "<< victim.getSize() << ", accessCount = "<<victim._accessCount
                      << ", cacheUsed="<<_cacheUsed);

        _cacheUsed -= victim.getSize();
        if (_cacheOverflowFlag)
        {
            _cacheOverflowFlag = false;
            _cacheOverflowEvent.signal();
        }
    }
    if (victim._next != NULL)
    {
        victim.unlink();
    }
    victim.free();
}

/*
 Remove all versions prior to lastLiveArrId from the unversioned
 array uaId in the storage. If lastLiveArrId is 0, removes all
 versions. Does nothing if the specified array is not present.
*/
void CachedStorage::removeVersions(QueryID queryId,
                                   ArrayUAID uaId,
                                   ArrayID lastLiveArrId)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_I);
    std::shared_ptr<InnerChunkMap> innerMap;
    ChunkMap::const_iterator iter = _chunkMap.find(uaId);
    if (iter == _chunkMap.end())
    {
        return;
    }
    innerMap = iter->second;

    DataStore::DataStoreKey dsk(_dsnsid, uaId);
    std::shared_ptr<DataStore> ds = _datastores->getDataStore(dsk);
    set<StorageAddress> victims;
    StorageAddress currentChunkAddr;
    bool currentChunkIsLive = true;
    for (InnerChunkMap::iterator i = innerMap->begin(); i != innerMap->end(); ++i)
    {
        StorageAddress const& address = i->first;

        /* If lastLiveArrId is non-zero, we must determine if the chunk is live.
           If lastLiveArrId is zero, then we proceed immediately to remove chunk.
        */
        if (lastLiveArrId)
        {
            if (!address.sameBaseAddr(currentChunkAddr))
            {
                /* Move on to next coordinate
                 */
                currentChunkAddr = address;
                currentChunkIsLive = true;
            }
            if (address.arrId > lastLiveArrId)
            {
                /* Chunk was added after oldest version
                   so it is still live
                */
                continue;
            }
            else if (address.arrId == lastLiveArrId)
            {
                /* Chunk was added in oldest version so it is
                   still live, but any older chunks are not
                */
                currentChunkIsLive = false;
                continue;
            }
            else if (address.arrId < lastLiveArrId)
            {
                /* Chunk was added prior to oldest version
                 */
                if (currentChunkIsLive)
                {
                    /* Chunk is still live, but older chunks are not
                     */
                    currentChunkIsLive = false;
                        continue;
                }
            }
        }

        /* Chunk should be removed
         */
        markChunkAsFree(i->second, ds);
        victims.insert(address);
    }
    _hd->writeAll(&_hdr, HEADER_SIZE, 0);
    for(set<StorageAddress>::iterator i = victims.begin(); i != victims.end(); ++i)
    {
        StorageAddress const& address = *i;
        innerMap->erase(address);
    }
    flush(uaId);
    if (!lastLiveArrId)
    {
        assert(innerMap->size() == 0);
        _chunkMap.erase(uaId);
        DataStore::DataStoreKey dsk(_dsnsid, uaId);
        _datastores->closeDataStore(dsk,
                                    true /* remove from disk */);
    }
}

void CachedStorage::removeVersionFromMemory(ArrayUAID uaId, ArrayID arrId)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_J);
    std::shared_ptr<InnerChunkMap> innerMap;
    ChunkMap::const_iterator iter = _chunkMap.find(uaId);
    if (iter == _chunkMap.end())
    {
        return;
    }
    else
    {
        innerMap = iter->second;
    }
    vector<StorageAddress> victims;
    for (InnerChunkMap::iterator i = innerMap->begin(); i != innerMap->end(); ++i)
    {
        StorageAddress const& addr = i->first;
        if (addr.arrId != arrId)
        {
            continue;
        }
        victims.push_back(addr);
    }
    for(vector<StorageAddress>::iterator i = victims.begin(); i != victims.end(); ++i)
    {
       StorageAddress const& address = *i;
       innerMap->erase(address);
    }
    if (innerMap->size() == 0)
    {
       _chunkMap.erase(uaId);
    }
}

InstanceID getPrimaryInstanceIdPosition(ArrayDesc const& desc, StorageAddress const& address)
{
    const size_t nInstances = desc.getResidency()->size();
    const InstanceID logicalInstancePos = desc.getPrimaryInstanceId(address.coords, nInstances);
    return logicalInstancePos;
}

/// @return the physical instance ID of the primary chunk copy (i.e. of the zeroth replica)
/// @param desc chunk array descriptor
/// @param address chunk storage address containing the chunk coordinates
InstanceID CachedStorage::getPrimaryInstanceId(ArrayDesc const& desc, StorageAddress const& address) const
{
      InstanceID logicalInstancePos = getPrimaryInstanceIdPosition(desc, address);
      InstanceID physicalInstance = desc.getResidency()->getPhysicalInstanceAt(logicalInstancePos);
      return physicalInstance;
}


typedef std::pair<size_t,size_t> IndexRange;

/// Assert that the ranges are ordered, not overlapping, and dont cover maxSize elements
void validateRangesInDebug(const IndexRange* ranges,
                             const size_t nRanges, const size_t maxSize)
{
    if (isDebug()) {
        size_t used=0;
        for (size_t i=0; i < nRanges; ++i)  {
            SCIDB_ASSERT(ranges[i].second >= ranges[i].first);
            if (i>0) {
                SCIDB_ASSERT(ranges[i].first > ranges[i-1].second);
            }
            used += ranges[i].second-ranges[i].first+1;
        }
        SCIDB_ASSERT(nRanges < 1 || ranges[nRanges-1].second < maxSize);
        SCIDB_ASSERT(used < maxSize);
    }
}

/// Given an index into the elements not covered by  unavailableRanges,
/// map that index into the index in [0,maxSize-1].
///
/// @param index essentially, it is the 0-based index as if the elements in unavailable ranges did not exist.
/// @param unavailableRanges an array of IndexRange's
/// @param nRanges number of ranges in unavailableRanges
/// @param maxSize max possible index + 1
/// @return index in [0,maxSize-1] which is not in any of unavailableRanges
size_t findIndexInResidency(const size_t index,
                            const IndexRange* unavailableRanges,
                            const size_t nRanges,
                            const size_t maxSize)
{
    SCIDB_ASSERT(index<maxSize);
    validateRangesInDebug(unavailableRanges, nRanges, maxSize);

    size_t available=0; // running count of indeces not coverred by unavailableRanges
    size_t afterLastRange=0;
    size_t rIndex = maxSize;
    for (size_t i=0; i < nRanges; ++i)
    {
        size_t newAvailable = available + (unavailableRanges[i].first-afterLastRange);

        if (newAvailable >= index+1) {
            rIndex = afterLastRange + (index-available);
            ASSERT_EXCEPTION(rIndex < maxSize, "Unreachable code");
            return rIndex;
        } else {
            available = newAvailable;
            afterLastRange = unavailableRanges[i].second+1;
            SCIDB_ASSERT(afterLastRange < maxSize);
        }
    }
    SCIDB_ASSERT(available < index+1);
    SCIDB_ASSERT(afterLastRange < maxSize);

    rIndex = afterLastRange + (index - available);
    ASSERT_EXCEPTION(rIndex < maxSize, "Unreachable code");
    return rIndex;
}

/// Make sure the range @ curIndex is inserted in ascending order
void insertLastInOrder(IndexRange* ranges, size_t curIndex)
{
    SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
    while (curIndex > 0) {
        if (ranges[curIndex].first < ranges[curIndex-1].first) {
            ranges[curIndex].swap(ranges[curIndex-1]);
            SCIDB_ASSERT(ranges[curIndex-1].second < ranges[curIndex].first);
            SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
            --curIndex;
        } else {
            SCIDB_ASSERT(ranges[curIndex-1].second < ranges[curIndex].first);
            SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
            break;
        }
    }
}

/// Finds the instance for the next replica and updates replicas[nReplicas] with that value.
/// It also adds all the instances running on the server of replicas[nReplicas] to unavailableRanges.
/// @param res array residency
/// @param targetIndex index of the instance ignoring unavailableRanges
/// @param [in,out] unavailableRanges ranges of indices to be excluded from the residency
///        for the purposes of locating the instance @ targetInstance
/// @param replicas the current list of replica targets of length nReplicas
/// @param nReplicas the current length of replicas and unavaliableRanges
/// @param resSize the number of instances in the residency
/// @return number of instances no longer available for replication
size_t computeNextReplicaTarget(const ArrayResPtr& res, const size_t targetIndex,
                                IndexRange* unavailableRanges, InstanceID* replicas,
                                const size_t nReplicas, const size_t resSize)
{
    SCIDB_ASSERT(resSize == res->size());
    size_t i = findIndexInResidency(targetIndex, unavailableRanges, nReplicas, resSize);
    SCIDB_ASSERT(i < resSize);

    // add a new replica target
    InstanceID iid = res->getPhysicalInstanceAt(i);
    const size_t serverId = getServerId(iid);
    replicas[nReplicas] = iid;

    // add all the instances on serverId to unavailableRanges
    size_t end=i+1;
    for (; end < resSize; ++end) {
        if (getServerId(res->getPhysicalInstanceAt(end)) != serverId) {
            break; /* residency must be sorted*/
        }
    }
    ssize_t begin=i-1;
    for (; begin >= 0; --begin) {
        if (getServerId(res->getPhysicalInstanceAt(begin)) != serverId) {
            break; /* residency must be sorted*/
        }
    }

    unavailableRanges[nReplicas] = IndexRange(begin+1,end-1);
    SCIDB_ASSERT(end>size_t(begin+1));
    insertLastInOrder(unavailableRanges, nReplicas);
    validateRangesInDebug(unavailableRanges, nReplicas+1, resSize+1);

    // the size of the newly used up range = #instances on serverId
    const size_t numInstancesOnServerId = end-(begin+1);
    return numInstancesOnServerId;
}

void CachedStorage::getReplicasInstanceId(InstanceID* replicas,
                                          ArrayDesc const& desc,
                                          StorageAddress const& address) const
{
    const size_t redundancy = desc.getDistribution()->getRedundancy();
    const size_t resSize = desc.getResidency()->size();

    if (redundancy > MAX_REDUNDANCY ||
        resSize <= redundancy) {
        ServerCounter sc;
        for (size_t i=0; i < resSize; ++i) {
            sc(desc.getResidency()->getPhysicalInstanceAt(i));
        }
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY)
              << redundancy << sc.getCount() << MAX_REDUNDANCY;
    }

    IndexRange unavailableRanges[MAX_REDUNDANCY + 1];
    const size_t nReplicas = (redundancy + 1);

    size_t primaryInstanceIndex = getPrimaryInstanceIdPosition(desc, address);
    size_t nUnavailable = computeNextReplicaTarget(desc.getResidency(), primaryInstanceIndex,
                                                   unavailableRanges, replicas,
                                                   0, resSize);
    SCIDB_ASSERT(nUnavailable <= resSize);
    size_t nInstancesRemaining = resSize - nUnavailable;

    for (size_t i = 0; i < redundancy; ++i)
    {
        const size_t curReplica = (i + 1);
        if (nInstancesRemaining <= 0) {
            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY)
                    << redundancy << curReplica << MAX_REDUNDANCY;
        }

        // NOTICE: Currently, getHashedChunkNumber() hashes the chunk numbers along each dimension
        //         rather than the coordinates. Originally getHashedChunkNumber() returned
        //         the chunk number in the row-major ordering. After hashing was introduced
        //         (to accommodate unbound dimensions)
        //         the domain of fibHash64() changed potentially resulting in worse performance.
        //         Separately, the code has been improved to stop assuming that array
        //         distribution uses a hash, and to instead rely only on
        //         getPrimaryInstanceIdPosition() which delegates the mapping to the descriptor so
        //         So again the domain of fibHash64 has changed.  There's some evidence
        //         that the distribution of the redundant chunks is now much less level
        //         than the distribution of the primary chunks, so the TODO below is
        //         now perhaps even more important.
        const uint64_t chunkId = primaryInstanceIndex * nReplicas + curReplica;

        // XXX TODO: this can be replaced with CityHash without computing chunkId
        size_t instanceIndex = fibHash64(chunkId, MAX_INSTANCE_BITS) % nInstancesRemaining;

        nUnavailable = computeNextReplicaTarget(desc.getResidency(), instanceIndex,
                                                unavailableRanges, replicas,
                                                curReplica, resSize);
        SCIDB_ASSERT(nUnavailable <= nInstancesRemaining);
        nInstancesRemaining -= nUnavailable;

        LOG4CXX_TRACE(logger, "Coords="<< CoordsToStr(address.coords)
                      << " primaryInstanceIndex=" << primaryInstanceIndex
                      << " currReplica=" << curReplica
                      << " instanceIndex=" << instanceIndex
                      << " nInstancesRemaining=" << nInstancesRemaining
                      << " target instance ["<<curReplica<<"]="<< replicas[curReplica]);
    }

    // post-condition, checks that the result does not return
    // two instances from the same server.
    // note that primary instance is replicas[0], not a redundant chunk.
    // note that there's no check here on the uniformity of the distribution
    // from fibHash64 at this time.
    if (isDebug()) {
        for (size_t i = 0; i < redundancy; ++i) {
            for (size_t j = i+1; j < redundancy+1; ++j) {
                SCIDB_ASSERT(getServerId(replicas[i]) != getServerId(replicas[j]));
            }
        }
    }
}

void CachedStorage::replicate(ArrayDesc const& desc,
                              StorageAddress const& addr,
                              PersistentChunk* chunk,
                              void const* data,
                              size_t compressedSize,
                              size_t decompressedSize,
                              std::shared_ptr<Query> const& query,
                              vector<std::shared_ptr<ReplicationManager::Item> >& replicasVec)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_K);
    Query::validateQueryPtr(query);

    size_t redundancy = desc.getDistribution()->getRedundancy();

    if ((!_redundancyEnabled) || // in recovery ?
        redundancy <= 0 ||       // no replication ?
        (chunk && !isPrimaryReplica(chunk, redundancy))) // replica chunk ?
    {
        return;
    }
    replicasVec.reserve(redundancy);
    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, desc, addr);

    QueryID queryId = query->getQueryID();
    SCIDB_ASSERT(queryId.isValid());

    std::shared_ptr<MessageDesc> chunkMsg;
    if (chunk && data)
    {
        std::shared_ptr<CompressedBuffer> buffer = std::make_shared<CompressedBuffer>();
        buffer->allocate(compressedSize);
        memcpy(buffer->getWriteData(), data, compressedSize);
        chunkMsg = std::make_shared<MessageDesc>(mtChunkReplica, buffer);
    }
    else
    {
        chunkMsg = std::make_shared<MessageDesc>(mtChunkReplica);
    }
    chunkMsg->setQueryID(queryId);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_attribute_id(addr.attId);
    chunkRecord->set_array_id(addr.arrId);
    for (size_t k = 0; k < addr.coords.size(); k++)
    {
        chunkRecord->add_coordinates(addr.coords[k]);
    }
    chunkRecord->set_eof(false);

    if(chunk)
    {
        chunkRecord->set_compression_method(chunk->getCompressionMethod());
        chunkRecord->set_decompressed_size(decompressedSize);
        chunkRecord->set_count(0);
        LOG4CXX_TRACE(logger, "Replicate chunk of array ID=" << addr.arrId << " attribute ID=" << addr.attId);
        assert(data != NULL); //TODO: need an exception ?
    }
    else
    {
        chunkRecord->set_tombstone(true);
    }

    for (size_t i = 1; i <= redundancy; ++i)
    {
        std::shared_ptr<ReplicationManager::Item> item = make_shared <ReplicationManager::Item>(replicas[i], chunkMsg, query);
        assert(_replicationManager);
        _replicationManager->send(item);
        replicasVec.push_back(item);
    }
}

void CachedStorage::abortReplicas(vector<std::shared_ptr<ReplicationManager::Item> >* replicasVec)
{
    assert(replicasVec);
    for (size_t i = 0; i < replicasVec->size(); ++i)
    {
        const std::shared_ptr<ReplicationManager::Item>& item = (*replicasVec)[i];
        assert(_replicationManager);
        _replicationManager->abort(item);
        assert(item->isDone());
    }
}

void CachedStorage::waitForReplicas(vector<std::shared_ptr<ReplicationManager::Item> >& replicasVec)
{
    // _mutex must NOT be locked
    for (size_t i = 0; i < replicasVec.size(); ++i)
    {
        const std::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationManager);
        _replicationManager->wait(item);
        assert(item->isDone());
        assert(item->validate(false));
    }
}

/* Write bytes to DataStore indicated by pos
 * @param pos DataStore and offset to which to write
 * @param data Bytes to write
 * @param len Number of bytes to write
 * @pre position in DataStore must be previously allocated
 * @throws userException if an error occurs
 */
void
CachedStorage::writeBytesToDataStore(DiskPos const& pos,
                                     void const* data,
                                     size_t len,
                                     size_t allocated)
{
    double t0 = 0, t1 = 0, writeTime = 0;
    DataStore::DataStoreKey dsk(_dsnsid, pos.dsid);
    std::shared_ptr<DataStore> ds = _datastores->getDataStore(dsk);

    if (!ds)
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATASTORE_NOT_FOUND);
    }

    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    ds->writeData(pos.offs, data, len, allocated);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        writeTime = t1 - t0;
    }

    if (_writeLogThreshold >= 0 && writeTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pwrite ds " << ds << " time " << writeTime);
    }
}

/* Force writing of chunk data to data store
   Exception is thrown if write failed
*/
void
CachedStorage::writeChunkToDataStore(DataStore& ds, PersistentChunk& chunk, void const* data)
{
    double t0 = 0, t1 = 0, writeTime = 0;

    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    ds.writeData(chunk._hdr.pos.offs,
                 data,
                 chunk._hdr.compressedSize,
                 chunk._hdr.allocatedSize);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        writeTime = t1 - t0;
    }

    if (_writeLogThreshold >= 0 && writeTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pwrite ds chunk "<< chunk.getHeader() <<" time "<< writeTime);
    }

    if(MonitorConfig::getInstance()->isEnabled())
    {
        std::shared_ptr<Query> query = Query::getQueryPerThread();
        if(query)
        {
            query->getStats().addToDBChunkWrite(chunk._hdr.compressedSize, chunk._hdr.size);
        }

        InstanceStats::getInstance()->addToDBChunkWrite(
            chunk._hdr.compressedSize, chunk._hdr.size);
    }
}

/* Read chunk data from the disk
   Exception is thrown if read failed
*/
void
CachedStorage::readChunkFromDataStore(DataStore& ds, PersistentChunk const& chunk, void* data)
{
    double t0 = 0, t1 = 0, readTime = 0;
    if (_writeLogThreshold >= 0)
    {
        t0 = getTimeSecs();
    }
    ds.readData(chunk._hdr.pos.offs, data, chunk._hdr.compressedSize);
    if (_writeLogThreshold >= 0)
    {
        t1 = getTimeSecs();
        readTime = t1 - t0;
    }
    if (_writeLogThreshold >= 0 && readTime * 1000 > _writeLogThreshold)
    {
        LOG4CXX_DEBUG(logger, "CWR: pread ds chunk "<< chunk.getHeader() <<" time "<< readTime);
    }

    if(MonitorConfig::getInstance()->isEnabled())
    {
        std::shared_ptr<Query> query = Query::getQueryPerThread();
        if(query)
        {
            query->getStats().addToDBChunkRead(chunk._hdr.compressedSize, chunk._hdr.size);
        }

        InstanceStats::getInstance()->addToDBChunkRead(
            chunk._hdr.compressedSize, chunk._hdr.size);
    }
}

RWLock& CachedStorage::getChunkLatch(PersistentChunk* chunk)
{
    return _latches[(size_t) chunk->_hdr.pos.offs % N_LATCHES];
}

void CachedStorage::getChunkPositions(ArrayDesc const& desc, std::shared_ptr<Query> const& query, CoordinateSet& chunkPositions)
{
    StorageAddress readAddress (desc.getId(), 0, Coordinates());
    while(findNextChunk(desc, query, readAddress))
    {
        chunkPositions.insert(readAddress.coords);
    }
}

bool CachedStorage::findNextChunk(ArrayDesc const& desc,
                                  std::shared_ptr<Query> const& query,
                                  StorageAddress& address)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_L);
    assert(address.attId < desc.getAttributes().size() && address.arrId <= desc.getId());
    Query::validateQueryPtr(query);

    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        address.coords.clear();
        return false;
    }
    std::shared_ptr<InnerChunkMap> const& innerMap = iter->second;
    if(address.coords.size())
    {
        address.coords[address.coords.size()-1] += desc.getDimensions()[desc.getDimensions().size() - 1].getChunkInterval();
    }
    address.arrId = desc.getId();
    InnerChunkMap::iterator innerIter = innerMap->lower_bound(address);
    while (true)
    {
        if (innerIter == innerMap->end() || innerIter->first.attId != address.attId)
        {
            address.coords.clear();
            return false;
        }
        if(innerIter->first.arrId <= desc.getId())
        {
            if(innerIter->second.getChunk() && isResponsibleFor( desc, *(innerIter->second.getChunk()), query))
            {
                address.arrId = innerIter->first.arrId;
                address.coords = innerIter->first.coords;
                return true;
            }
            else
            {
                address.arrId = desc.getId();
                address.coords = innerIter->first.coords;
                address.coords[address.coords.size()-1] += desc.getDimensions()[desc.getDimensions().size() - 1].getChunkInterval();
                innerIter = innerMap->lower_bound(address);
            }
        }
        while(innerIter != innerMap->end() && innerIter->first.arrId > address.arrId && innerIter->first.attId == address.attId)
        {
            ++innerIter;
        }
    }
}

bool CachedStorage::findChunk(ArrayDesc const& desc, std::shared_ptr<Query> const& query, StorageAddress& address)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_M);
    Query::validateQueryPtr(query);

    ChunkMap::iterator iter = _chunkMap.find(desc.getUAId());
    if (iter == _chunkMap.end())
    {
        address.coords.clear();
        return false;
    }
    std::shared_ptr<InnerChunkMap> const& innerMap = iter->second;
    address.arrId = desc.getId();
    InnerChunkMap::iterator innerIter = innerMap->lower_bound(address);
    if (innerIter == innerMap->end() || innerIter->first.coords != address.coords || innerIter->first.attId != address.attId)
    {
        address.coords.clear();
        return false;
    }

    assert(innerIter->first.arrId <= address.arrId && innerIter->first.coords == address.coords);
    // XXX empty query used? to represent what ? NID chunk ?
    if(innerIter->second.getChunk() && (!query || isResponsibleFor(desc, *(innerIter->second.getChunk()), query)))
    {
        address.arrId = innerIter->first.arrId;
        return true;
    }
    else
    {
        address.coords.clear();
        return false;
    }
}

void CachedStorage::cleanChunk(PersistentChunk* chunk)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_N);
    LOG4CXX_TRACE(logger, "CachedStorage::cleanChunk =" << chunk << ", accessCount = "<<chunk->_accessCount);
    assert(chunk->_accessCount>0);
    --chunk->_accessCount;
    // Free the chunk regardless of _accessCount to avoid incorrect
    // _cacheUsed accounting done in internalFreeChunk()
    // (_accessCount can be >1 because we are double pinning sometimes,
    // e.g. in ArrayIterator::newChunk & ChunkIterator::ChunkIterator).
    // If we are here, we have failed to writeChunk() and the chunk is invalid
    chunk->free();
    notifyChunkReady(*chunk);
}

/* Write new chunk into the smgr.
 */
void
CachedStorage::writeChunk(ArrayDesc const& adesc,
                          PersistentChunk* newChunk,
                          const std::shared_ptr<Query>& query)
{
    /* XXX TODO: consider locking mutex here to avoid writing replica chunks for a rolled-back query
     */
    PersistentChunk& chunk = *newChunk;

    /* To deal with exceptions: unpin and free
     */
    OnScopeExit chunkCleaner([this, &chunk] () { cleanChunk(&chunk); });

    Query::validateQueryPtr(query);

    /* Update value count in Chunk Header
     */
    const AttributeDesc& attrDesc = adesc.getAttributes()[chunk.getAddress().attId];

    if (attrDesc.isEmptyIndicator()) {
        ConstRLEEmptyBitmap bitmap(static_cast<const char*>(chunk._data));
        chunk._hdr.nElems = bitmap.count();
    } else {
        ConstRLEPayload payload(static_cast<const char*>(chunk._data));
        chunk._hdr.nElems = payload.count();
    }

    /* Grab buffer to use for compressing chunk data and try to compress
     */
    const size_t bufSize = chunk.getSize();
    boost::scoped_array<char> buf(new char[bufSize]);
    if (!buf) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
    }
    setToZeroInDebug(buf.get(), bufSize);

    VersionID dstVersion = adesc.getVersionId();
    void const* deflated = buf.get();
    size_t nCoordinates = chunk._addr.coords.size();
    DBArrayChunkInternal intChunk(adesc, &chunk);
    size_t compressedSize =
        _compressors[static_cast<size_t>(chunk.getCompressionMethod())]->compress(buf.get(),
                                                                                  intChunk,
                                                                                  intChunk.getSize());
    assert(compressedSize <= chunk.getSize());
    if (compressedSize == chunk.getSize())
    { // no compression
        deflated = chunk._data;
    }

    /* Replicate chunk data to other instances
     */
    vector<std::shared_ptr<ReplicationManager::Item> > replicasVec;
    OnScopeExit replicasCleaner([this, &replicasVec] () { abortReplicas(&replicasVec); });
    replicate(adesc, chunk._addr, &chunk, deflated,
              compressedSize, chunk.getSize(), query, replicasVec);

    /* Write chunk locally into storage
     */
    {
        ScopedMutexLock cs(_mutex, PTW_SML_STOR_O);
        assert(chunk.isRaw()); // new chunk is raw
        Query::validateQueryPtr(query);
        DataStore::DataStoreKey dsk(_dsnsid, adesc.getUAId());
        std::shared_ptr<DataStore> ds = _datastores->getDataStore(dsk);

        /* Fill in the chunk descriptor
         */
        chunk._hdr.compressedSize = compressedSize;
        chunk._hdr.pos.dsid = adesc.getUAId();
        chunk._hdr.pos.offs = ds->allocateSpace(compressedSize,
                                                chunk._hdr.allocatedSize);

        /* Locate spot for chunk descriptor
         */
        if (_freeHeaders.empty())
        {
            chunk._hdr.pos.hdrPos = _hdr.currPos;
            _hdr.currPos += sizeof(ChunkDescriptor);
            _hdr.nChunks += 1;
        }
        else
        {
            set<uint64_t>::iterator i = _freeHeaders.begin();
            chunk._hdr.pos.hdrPos = *i;
            assert(chunk._hdr.pos.hdrPos != 0);
            _freeHeaders.erase(i);
        }

        /* Write ahead UNDO log
         */
        if (dstVersion != 0)
        {
            // Second entry in this array is the end-of-record sentinel.
            TransLogRecord transLogRecord[2];
            setToZeroInDebug(transLogRecord, sizeof(transLogRecord));

            transLogRecord->arrayUAID = adesc.getUAId();
            transLogRecord->arrayId = chunk._addr.arrId;
            transLogRecord->version = dstVersion;
            transLogRecord->hdr = chunk._hdr;
            transLogRecord->oldSize = 0;
            transLogRecord->hdrCRC = calculateCRC32(transLogRecord,
                                                    sizeof(TransLogRecordHeader));

            if (_logSize + sizeof(TransLogRecord) > _logSizeLimit)
            {
                _logSize = 0;
                _currLog ^= 1;
            }

            /* Write the transaction... log is opened O_SYNC so no flush is necessary
             */
            bool isLogEnabled = Config::getInstance()->getOption<bool>(CONFIG_STORAGE_LOG_ENABLE);
            if (isLogEnabled) {
                LOG4CXX_TRACE(logger, "CachedStorage::writeChunk: write log entry chunk pos "
                              << transLogRecord->hdr.pos.offs << " at log pos " << _logSize);
                _log[_currLog]->writeAll(transLogRecord, sizeof(TransLogRecord) * 2, _logSize);
                _logSize += sizeof(TransLogRecord);
            }

        }

        /* Write chunk data
         */
        writeChunkToDataStore(*ds, chunk, deflated);
        buf.reset();

        /* Write chunk descriptor in storage header
         */
        ChunkDescriptor cdesc;
        cdesc.hdr = chunk._hdr;
        for (size_t i = 0; i < nCoordinates; i++)
        {
            cdesc.coords[i] = chunk._addr.coords[i];
        }
        assert(chunk._hdr.pos.hdrPos != 0);

        LOG4CXX_TRACE(chunkLogger, "chunkl: writechunk: write chunk desc at pos "
                      << chunk._hdr.pos.hdrPos);
        LOG4CXX_TRACE(chunkLogger, "chunkl: writechunk: desc: "
                      << cdesc.toString());

        _hd->writeAll(&cdesc, sizeof(ChunkDescriptor), chunk._hdr.pos.hdrPos);

        /* Update storage header (for nchunks field)
         */
        _hd->writeAll(&_hdr, HEADER_SIZE, 0);

        InjectedErrorListener::throwif(__LINE__, __FILE__);

        if (isPrimaryReplica(&chunk,
                             adesc.getDistribution()->getRedundancy())) {
            chunkCleaner.cancel();
            chunk.unPin();
            notifyChunkReady(chunk);
            addChunkToCache(chunk);
        } // else chunkCleaner will dec accessCount and free
    }

    /* Wait for replication to complete
     */
    waitForReplicas(replicasVec);
    replicasCleaner.cancel();
}

/* Mark a chunk as free in the on-disk and in-memory chunk map.  Also mark it as free
   in the datastore.
 */
void CachedStorage::markChunkAsFree(InnerChunkMapEntry& entry, std::shared_ptr<DataStore>& ds)
{
    ChunkHeader header;
    std::shared_ptr<PersistentChunk>& chunk = entry.getChunk();

    if (!chunk)
    {
        /* Handle tombstone chunks
         */
        size_t rc = _hd->read(&header, sizeof(ChunkHeader), entry.getTombstonePos());
        if (rc != 0 && rc != sizeof(ChunkHeader)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
                << "read" << ::strerror(errno) << errno;
        }
    }
    else
    {
        /* Handle live chunks
         */
        memcpy(&header, &(chunk->_hdr), sizeof(ChunkHeader));
        if (ds)
            ds->freeChunk(chunk->_hdr.pos.offs, chunk->_hdr.allocatedSize);
    }

    /* Update header as free and write back to storage header file
     */
    header.arrId = 0;
    LOG4CXX_TRACE(chunkLogger,
                  "chunkl: markchunkasfree: free chunk descriptor at position "
                  << header.pos.hdrPos);
    _hd->writeAll(&header, sizeof(ChunkHeader), header.pos.hdrPos);
    assert(header.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
    _freeHeaders.insert(header.pos.hdrPos);
}

void CachedStorage::removeDeadChunks(ArrayDesc const& arrayDesc,
                                     set<Coordinates, CoordinatesLess> const& liveChunks,
                                     std::shared_ptr<Query> const& query)
{
    typedef set<Coordinates, CoordinatesLess> DeadChunks;
    DeadChunks deadChunks;
    {
        ScopedMutexLock cs(_mutex, PTW_SML_STOR_P);
        Query::validateQueryPtr(query);

        StorageAddress readAddress (arrayDesc.getId(), 0, Coordinates());
        while(findNextChunk(arrayDesc, query, readAddress))
        {
            if(liveChunks.count(readAddress.coords) == 0)
            {
                SCIDB_ASSERT( getPrimaryInstanceId(arrayDesc, readAddress) == _hdr.instanceId );
                deadChunks.insert(readAddress.coords);
            }
        }
    }
    for (DeadChunks::const_iterator i=deadChunks.begin(); i!=deadChunks.end(); ++i) {
        Coordinates const& coords = *i;
        // relication done inside removeChunkVersion() must be done with _mutex UNLOCKED
        removeChunkVersion(arrayDesc, coords, query);
    }
}

void CachedStorage::removeChunkVersion(ArrayDesc const& arrayDesc,
                                       Coordinates const& coords,
                                       std::shared_ptr<Query> const& query)
{
    vector<std::shared_ptr<ReplicationManager::Item> > replicasVec;
    OnScopeExit replicasCleaner([this, &replicasVec] () { abortReplicas(&replicasVec); });
    StorageAddress addr(arrayDesc.getId(), 0, coords);
    replicate(arrayDesc, addr, NULL, NULL, 0, 0, query, replicasVec);
    removeLocalChunkVersion(arrayDesc, coords, query);
    waitForReplicas(replicasVec);
    replicasCleaner.cancel();
}

void CachedStorage::removeLocalChunkVersion(ArrayDesc const& arrayDesc,
                                            Coordinates const& coords,
                                            std::shared_ptr<Query> const& query)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_Q);
    Query::validateQueryPtr(query);

    assert(arrayDesc.getUAId() != arrayDesc.getId()); //Immutable arrays NEVER have tombstones
    VersionID dstVersion = arrayDesc.getVersionId();
    ChunkDescriptor tombstoneDesc;

    setToZeroInDebug(&tombstoneDesc, sizeof(tombstoneDesc));    // not a good idea to vary across builds

    tombstoneDesc.hdr.storageVersion = SCIDB_STORAGE_FORMAT_VERSION;
    tombstoneDesc.hdr.flags = 0;
    tombstoneDesc.hdr.set<ChunkHeader::TOMBSTONE>(true);
    tombstoneDesc.hdr.arrId = arrayDesc.getId();
    tombstoneDesc.hdr.nCoordinates = safe_static_cast<uint16_t>(coords.size());
    tombstoneDesc.hdr.instanceId = getPrimaryInstanceId(arrayDesc, StorageAddress(arrayDesc.getId(), 0, coords));
    tombstoneDesc.hdr.allocatedSize = 0;
    tombstoneDesc.hdr.compressedSize = 0;
    tombstoneDesc.hdr.size = 0;
    tombstoneDesc.hdr.nElems = 0;
    tombstoneDesc.hdr.compressionMethod = 0;
    tombstoneDesc.hdr.pos.dsid = arrayDesc.getUAId();
    tombstoneDesc.hdr.pos.offs = 0;
    for (int i = 0; i <  tombstoneDesc.hdr.nCoordinates; i++)
    {
        tombstoneDesc.coords[i] = coords[i];
    }
    //WAL
    TransLogRecord transLogRecord[2];
    setToZeroInDebug(transLogRecord, sizeof(transLogRecord));
    transLogRecord->arrayUAID = arrayDesc.getUAId();
    transLogRecord->arrayId = arrayDesc.getId();
    transLogRecord->version = dstVersion;
    transLogRecord->oldSize = 0;
    ::memset(&transLogRecord[1], 0, sizeof(TransLogRecord)); // end of log marker
    ChunkMap::iterator iter = _chunkMap.find(arrayDesc.getUAId());
    if(iter == _chunkMap.end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Attempt to create tombstone for unexistent array";
    }
    std::shared_ptr<InnerChunkMap> inner = iter->second;
    for (AttributeID i =0; i<arrayDesc.getAttributes().size(); i++)
    {
        query->validate();

        tombstoneDesc.hdr.attId = i;
        StorageAddress addr (arrayDesc.getId(), i, coords);
        if( (*inner)[addr].getChunk() != NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
            << CoordsToStr(addr.coords);
        }
        if (_freeHeaders.empty())
        {
            tombstoneDesc.hdr.pos.hdrPos = _hdr.currPos;
            _hdr.currPos += sizeof(ChunkDescriptor);
            _hdr.nChunks += 1;
        }
        else
        {
            set<uint64_t>::iterator i = _freeHeaders.begin();
            tombstoneDesc.hdr.pos.hdrPos = *i;
            assert( tombstoneDesc.hdr.pos.hdrPos != 0);
            _freeHeaders.erase(i);
        }
        (*inner)[addr].setTombstonePos(InnerChunkMapEntry::TOMBSTONE,
                                       tombstoneDesc.hdr.pos.hdrPos);
        transLogRecord->hdr = tombstoneDesc.hdr;
        transLogRecord->hdrCRC = calculateCRC32(transLogRecord, sizeof(TransLogRecordHeader));
        if (_logSize + sizeof(TransLogRecord) > _logSizeLimit)
        {
            _logSize = 0;
            _currLog ^= 1;
        }


        bool isLogEnabled = Config::getInstance()->getOption<bool>(CONFIG_STORAGE_LOG_ENABLE);
        if (isLogEnabled) {
            LOG4CXX_TRACE(logger, "CachedStorage::removeLocalChunkVersion: "
                          << " write log entry chunk tombstone pos " << transLogRecord->hdr.pos.offs
                          << " at log pos " << _logSize);
            _log[_currLog]->writeAll(transLogRecord, sizeof(TransLogRecord) * 2, _logSize);
            _logSize += sizeof(TransLogRecord);
        }

        LOG4CXX_TRACE(chunkLogger, "chunkl: removelocalchunkversion: "
                      << "write chunk tombstone at pos " <<  tombstoneDesc.hdr.pos.hdrPos);
        LOG4CXX_TRACE(chunkLogger, "chunkl: removelocalchunkversion: "
                      << "tombstone to write: " << tombstoneDesc.toString());

        _hd->writeAll(&tombstoneDesc, sizeof(ChunkDescriptor), tombstoneDesc.hdr.pos.hdrPos);
    }
    _hd->writeAll(&_hdr, HEADER_SIZE, 0);
    InjectedErrorListener::throwif(__LINE__, __FILE__);
}

///
/// @note rollback must be called only when the query calling it is in error state
///       thus, before performing any updates under THE _mutex, the query context must be validated
///       to avoid leaving chunks behind
void CachedStorage::rollback(RollbackMap const& undoUpdates)
{
    LOG4CXX_DEBUG(logger, "Performing rollback");

    ScopedMutexLock cs(_mutex, PTW_SML_STOR_R);
    for (int i = 0; i < 2; i++)
    {
        uint64_t pos = 0;
        TransLogRecord transLogRecord;
        setToZeroInDebug(&transLogRecord, sizeof(transLogRecord));

        bool isLogEnabled = Config::getInstance()->getOption<bool>(CONFIG_STORAGE_LOG_ENABLE);
        while (isLogEnabled)
        {
            // read txn log record
            size_t rc = _log[i]->read(&transLogRecord, sizeof(TransLogRecord), pos);
            if (rc != sizeof(TransLogRecord) || transLogRecord.arrayUAID == 0)
            {
                LOG4CXX_DEBUG(logger, "End of log at position " << pos << " rc=" << rc);
                break;
            }
            uint32_t crc = calculateCRC32(&transLogRecord, sizeof(TransLogRecordHeader));
            if (crc != transLogRecord.hdrCRC)
            {
                LOG4CXX_ERROR(logger, "CRC doesn't match for log record: "
                              << crc << " vs. expected " << transLogRecord.hdrCRC);
                break;
            }
            pos += sizeof(TransLogRecord);
            RollbackMap::const_iterator it = undoUpdates.find(transLogRecord.arrayUAID);
            VersionID lastVersionID = -1;
            if (it != undoUpdates.end() && (it->second.first <= transLogRecord.arrayId))
            {
                lastVersionID = it->second.second;

                // this version is to be un-done
                assert(transLogRecord.oldSize == 0);
                assert(lastVersionID == transLogRecord.version - 1);

                transLogRecord.hdr.arrId = 0; // mark chunk as free
                assert(transLogRecord.hdr.pos.hdrPos != 0);
                LOG4CXX_TRACE(chunkLogger, "chunkl: rollback: undo chunk descriptor creation at position "
                              << transLogRecord.hdr.pos.hdrPos);
                LOG4CXX_TRACE(chunkLogger, "chunkl: rollback: hdr: "
                              << transLogRecord.hdr.toString());
                _hd->writeAll(&transLogRecord.hdr, sizeof(ChunkHeader), transLogRecord.hdr.pos.hdrPos);
                _freeHeaders.insert(transLogRecord.hdr.pos.hdrPos);

                /* Update the free list for the data store
                 */
                if (!transLogRecord.hdr.is<ChunkHeader::TOMBSTONE>() &&
                    lastVersionID > 0)
                {
                    DataStore::DataStoreKey dsk(_dsnsid, transLogRecord.hdr.pos.dsid);
                    std::shared_ptr<DataStore> ds =
                        _datastores->getDataStore(dsk);
                    ds->freeChunk(transLogRecord.hdr.pos.offs,
                                  transLogRecord.hdr.allocatedSize);
                }
            }
            pos += transLogRecord.oldSize;
        }
    }
    flush();

    for(RollbackMap::const_iterator it = undoUpdates.begin();
        it != undoUpdates.end();
        ++it)
    {
        // If we rolled back the first version, delete the datastore
        if (it->second.second == 0)
        {
            DataStore::DataStoreKey dsk(_dsnsid, it->first);
            _datastores->closeDataStore(dsk,
                                        true /* remove from disk */);
        }
        LOG4CXX_DEBUG(logger, "Rolling back arrId = "
                      << it->first << ", version = " << it->second.second);
    }

    LOG4CXX_DEBUG(logger, "Rollback complete");
}

void CachedStorage::doTxnRecoveryOnStartup()
{
    list<std::shared_ptr<LockDesc> > coordLocks;
    list<std::shared_ptr<LockDesc> > workerLocks;

    SystemCatalog::getInstance()->readArrayLocks(getInstanceId(), coordLocks, workerLocks);
    std::shared_ptr<RollbackMap> arraysToRollback =
        make_shared <RollbackMap> ();
    UpdateErrorHandler::RollbackWork collector = bind(&collectArraysToRollback, arraysToRollback, _1, _2, _3);

    { // Deal with the  LockDesc::COORD type locks first

        for (list<std::shared_ptr<LockDesc> >::const_iterator iter = coordLocks.begin();
             iter != coordLocks.end();
             ++iter)
        {
            const std::shared_ptr<LockDesc>& lock = *iter;

            if (lock->getLockMode() == LockDesc::RM)
            {
                const bool checkLock = false;
                RemoveErrorHandler::handleRemoveLock(lock, checkLock);
            }
            else if (lock->getLockMode() == LockDesc::CRT ||
                     lock->getLockMode() == LockDesc::WR)
            {
                UpdateErrorHandler::handleErrorOnCoordinator(lock, collector);
            }
            else
            {
                ASSERT_EXCEPTION((lock->getLockMode() == LockDesc::RNF ||
                                  lock->getLockMode() == LockDesc::XCL ||
                                  lock->getLockMode() == LockDesc::RD),
                                 string("Unrecognized array lock on recovery: ")+lock->toString());
            }
        }

        // Do the rollback
        rollback(*arraysToRollback.get());

        // NOTE: All transient arrays are invalidated on (re)start in the catalog

        SystemCatalog::getInstance()->deleteCoordArrayLocks(getInstanceId());
    }

    { // Deal with the worker locks next

        arraysToRollback->clear();

        for (list<std::shared_ptr<LockDesc> >::const_iterator iter = workerLocks.begin();
             iter != workerLocks.end(); ++iter)
        {
            const std::shared_ptr<LockDesc>& lock = *iter;

            if (lock->getLockMode() == LockDesc::CRT ||
                lock->getLockMode() == LockDesc::WR)
            {
                const bool checkCoordinatorLock = true;
                UpdateErrorHandler::handleErrorOnWorker(lock, checkCoordinatorLock, collector);
            }
            else
            {
                ASSERT_EXCEPTION(lock->getLockMode() == LockDesc::XCL,
                                 string("Unrecognized array lock on recovery: ")+lock->toString());
            }
        }

        // Do the rollback
        rollback(*arraysToRollback.get());

        // NOTE: All transient arrays are invalidated on (re)start in the catalog

        SystemCatalog::getInstance()->deleteWorkerArrayLocks(getInstanceId());
    }
}

/* Flush all changes to the physical device(s) for the indicated array.
   (optionally flush data for all arrays, if uaId == INVALID_ARRAY_ID).
*/
void
CachedStorage::flush(ArrayUAID uaId)
{
    int rc;

    /* flush the chunk map file
     */
    rc = _hd->fsync();
    if (rc != 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "fsync" << ::strerror(errno) << errno;
    }

    /* flush the data store for the indicated array (or flush all datastores)
     */
    if (uaId != INVALID_ARRAY_ID)
    {
        DataStore::DataStoreKey dsk(_dsnsid, uaId);
        std::shared_ptr<DataStore> ds = _datastores->getDataStore(dsk);
        ds->flush();
    }
    else
    {
        _datastores->flushAllDataStores();
    }
}

std::shared_ptr<ArrayIterator> CachedStorage::getArrayIterator(std::shared_ptr<const Array>& arr,
                                                                 AttributeID attId,
                                                                 std::shared_ptr<Query>& query)
{
    return std::shared_ptr<ArrayIterator>(new DBArrayIterator(this, arr, attId, query, true));
}

std::shared_ptr<ConstArrayIterator> CachedStorage::getConstArrayIterator(std::shared_ptr<const Array>& arr,
                                                                           AttributeID attId,
                                                                           std::shared_ptr<Query>& query)
{
    return std::shared_ptr<ConstArrayIterator>(new DBArrayIterator(this, arr, attId, query, false));
}

void CachedStorage::fetchChunk(ArrayDesc const& desc, PersistentChunk& chunk)
{
    ChunkInitializer guard(this, chunk);
    DataStore::DataStoreKey dsk(_dsnsid, desc.getUAId());
    std::shared_ptr<DataStore> ds = _datastores->getDataStore(dsk);
    if (chunk._hdr.pos.hdrPos == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                               SCIDB_LE_ACCESS_TO_RAW_CHUNK) << chunk.getHeader().arrId;
    }
    size_t chunkSize = chunk.getSize();
    chunk.allocate(chunkSize);
    if (chunk.getCompressedSize() != chunkSize)
    {
        const size_t bufSize = chunk.getCompressedSize();
        boost::scoped_array<char> buf(new char[bufSize]);
        if (!buf) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        readChunkFromDataStore(*ds, chunk, buf.get());
        DBArrayChunkInternal intChunk(desc, &chunk);
        size_t rc = _compressors[chunk.getCompressionMethod()]->decompress(intChunk,
                                                                           buf.get(),
                                                                           chunk.getCompressedSize());
        if (rc != chunk.getSize())
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_DECOMPRESS_CHUNK);
        buf.reset();
    }
    else
    {
        readChunkFromDataStore(*ds, chunk, chunk._data);
    }
}

void CachedStorage::loadChunk(ArrayDesc const& desc, PersistentChunk* aChunk)
{
    PersistentChunk& chunk = *aChunk;
    {
        ScopedMutexLock cs(_mutex, PTW_SML_STOR_S);
        if (chunk._accessCount < 2)
        { // Access count>=2 means that this chunk is already pinned and loaded by some upper frame so access to it may not cause deadlock
            _mutex.checkForDeadlock();
        }
        if (chunk._raw)
        {
            // Some other thread is already loading the chunk: just wait until it completes
            do
            {
                chunk._waiting = true;
                Semaphore::ErrorChecker ec;
                std::shared_ptr<Query> query = Query::getQueryPerThread();
                if (query) // in what use case is query unset?
                {
                    ec = bind(&Query::validate, query);
                }
                _loadEvent.wait(_mutex, ec, PTW_EVENT_SM_LOAD);
            } while (chunk._raw);

            if (chunk._data == NULL)
            {
                chunk._raw = true;
            }
        }
        else
        {
            if (chunk._data == NULL)
            {
                _mutex.checkForDeadlock();
                chunk._raw = true;
                addChunkToCache(chunk);
            }
        }
    }

    if (chunk._raw)
    {
        fetchChunk(desc, chunk);
    }
}

std::shared_ptr<PersistentChunk>
CachedStorage::readChunk(ArrayDesc const& desc,
                         StorageAddress const& addr,
                         const std::shared_ptr<Query>& query)
{
    std::shared_ptr<PersistentChunk> chunk = CachedStorage::lookupChunk(desc, addr);
    if (!chunk) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
    }
    loadChunk(desc, chunk.get());
    return chunk;
}

InstanceID CachedStorage::getInstanceId() const
{
    return _hdr.instanceId;
}

void CachedStorage::setInstanceId(InstanceID id)
{
    _hdr.instanceId = id;
    _hd->writeAll(&_hdr, HEADER_SIZE, 0);
}

void CachedStorage::getDiskInfo(DiskInfo& info)
{
    ::memset(&info, 0, sizeof info);
}

void CachedStorage::visitChunkDescriptors(const ChunkDescriptorVisitor& visit) const
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_T);
    ChunkDescriptor cd;
    uint64_t chunkPos = HEADER_SIZE;
    for (size_t i = 0; i < _hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor))
    {
        _hd->readAll(&cd, sizeof(ChunkDescriptor), chunkPos);
        visit(cd, _freeHeaders.count(chunkPos));
    }
}

void CachedStorage::visitChunkMap(const ChunkMapVisitor& visit) const
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_U);
    for (ChunkMap::const_iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        for (InnerChunkMap::const_iterator j = i->second->begin(); j != i->second->end(); ++j)
        {
            uint64_t tombstonePos = 0;

            if (j->second.isTombstone())
            {
                tombstonePos = j->second.getTombstonePos();
            }
            visit(i->first,
                  j->first,
                  j->second.getChunk().get(),
                  tombstonePos,
                  j->second.isValid());
        }
    }
}

///////////////////////////////////////////////////////////////////
/// DBArrayIterator
///////////////////////////////////////////////////////////////////

CachedStorage::DBArrayIterator::DBArrayIterator(CachedStorage* storage,
                                                std::shared_ptr<const Array>& array,
                                                AttributeID attId, std::shared_ptr<Query>& query,
                                                bool writeMode)
  : _currChunk(NULL),
    _storage(storage),
    _attrDesc(array->getArrayDesc().getAttributes()[attId]),
    _address(array->getArrayDesc().getId(), attId, Coordinates()),
    _query(query),
    _writeMode(writeMode),
    _array(array)
{
    restart();
}


CachedStorage::DBArrayIterator::~DBArrayIterator()
{}

CachedStorage::DBArrayChunk* CachedStorage::DBArrayIterator::getDBArrayChunk(std::shared_ptr<PersistentChunk>& dbChunk)
{
    assert(dbChunk);
    DBArrayMap::iterator iter = _dbChunks.find(dbChunk);
    if (iter == _dbChunks.end()) {
        std::shared_ptr<DBArrayChunk> dbac(new DBArrayChunk(*this, dbChunk.get()));
        std::pair<DBArrayMap::iterator, bool> res = _dbChunks.insert(DBArrayMap::value_type(dbChunk, dbac));
        assert(res.second);
        iter = res.first;
    }
    assert(iter != _dbChunks.end());
    assert(iter->first == dbChunk);
    assert(iter->second->getPersistentChunk() == dbChunk.get());
    LOG4CXX_TRACE(logger, "DBArrayIterator::getDBArrayChunk this=" << this
                  << ", dbChunk=" << dbChunk.get()
                  << ", dbArrayChunk=" << iter->second.get());

    return iter->second.get();
}


ConstChunk const& CachedStorage::DBArrayIterator::getChunk()
{
    getQuery();
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    if (_currChunk == NULL)
    {
        std::shared_ptr<PersistentChunk> chunk = _storage->lookupChunk(getArrayDesc(), _address);
        if (!chunk) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
        }
        PersistentChunk::UnPinner scope(chunk.get());
        DBArrayChunk *dbChunk = getDBArrayChunk(chunk);
        _currChunk = dbChunk;
        assert(_currChunk);
    }
    return *_currChunk;
}

bool CachedStorage::DBArrayIterator::end()
{
    return _address.coords.size() == 0;
}

void CachedStorage::DBArrayIterator::operator ++()
{
    std::shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    bool ret = _storage->findNextChunk(getArrayDesc(), query, _address);
    if (_writeMode)
    {   //in _writeMode we iterate only over chunks from this exact version
        while (ret && _address.arrId != getArrayDesc().getId())
        {
            ret = _storage->findNextChunk(getArrayDesc(), query, _address);
        }
    }
}

Coordinates const& CachedStorage::DBArrayIterator::getPosition()
{
    if (end())
    {
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    return _address.coords;
}

bool CachedStorage::DBArrayIterator::setPosition(Coordinates const& pos)
{
    std::shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    _address.coords = pos;
    getArrayDesc().getChunkPositionFor(_address.coords);

    bool ret = _storage->findChunk(getArrayDesc(), query, _address);
    if ( !ret || (_writeMode && _address.arrId != getArrayDesc().getId()))
    {
        _address.coords.clear();
        return false;
    }
    return true;
}

void CachedStorage::DBArrayIterator::restart()
{
    std::shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    _address.coords.clear();

    bool ret = _storage->findNextChunk(getArrayDesc(), query, _address);
    if (_writeMode)
    {   //in _writeMode we iterate only over chunks from this exact version
        while ( ret && _address.arrId != getArrayDesc().getId())
        {
            ret = _storage->findNextChunk(getArrayDesc(), query, _address);
        }
    }
}

Chunk& CachedStorage::DBArrayIterator::newChunk(Coordinates const& pos, CompressorType compressionMethod)
{
    ASSERT_EXCEPTION_FALSE("DBArrayIterator::newChunk(pos, compressionMethod)");
}

Chunk& CachedStorage::DBArrayIterator::newChunk(Coordinates const& pos)
{
    assert(_writeMode);

    int compressionMethod = getAttributeDesc().getDefaultCompressionMethod();
    std::shared_ptr<Query> query = getQuery();
    _currChunk = NULL;
    _address.coords = pos;
    if (!getArrayDesc().contains(_address.coords))
    {
        _address.coords.clear();
        throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
            << CoordsToStr(pos) << getArrayDesc().getDimensions();
    }
    getArrayDesc().getChunkPositionFor(_address.coords);

    bool ret = _storage->findChunk(getArrayDesc(), query, _address);
    if(ret && _address.arrId == getArrayDesc().getId())
    {
        stringstream ss; ss << CoordsToStr(_address.coords);
        _address.coords.clear();
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << ss.str();
    }
    _address.arrId = getArrayDesc().getId();
    _address.coords = pos;
    getArrayDesc().getChunkPositionFor(_address.coords);
    std::shared_ptr<PersistentChunk> chunk =
        _storage->createChunk(getArrayDesc(), _address, compressionMethod, query);
    assert(chunk);
    DBArrayChunk *dbChunk = getDBArrayChunk(chunk);
    _currChunk = dbChunk;
    return *_currChunk;
}

void CachedStorage::DBArrayIterator::deleteChunk(Chunk& chunk) //XXX TODO: consider removing this method altogether
{
    DBArrayChunk* dbaChunk = dynamic_cast<DBArrayChunk*>(&chunk);
    if (dbaChunk==NULL || chunk.getArrayDesc() != getArrayDesc()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
               << "chunk(not persistent)");
    }
    assert(_writeMode);
    _currChunk = NULL;
    _address.coords.clear();

    PersistentChunk* dbChunk = dbaChunk->getPersistentChunk();
    LOG4CXX_TRACE(logger, "DBArrayIterator::deleteChunk this="
                  << this << ", dbChunk=" << dbChunk << ", dbArrayChunk?=" << &chunk);
    _storage->deleteChunk(getArrayDesc(),*dbChunk);
    _dbChunks.erase(dbChunk->shared_from_this());
}

Chunk& CachedStorage::DBArrayIterator::copyChunk(ConstChunk const& srcChunk,
                                                 std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap)
{
    assert(_writeMode);
    std::shared_ptr<Query> query = getQuery();
    _address.coords = srcChunk.getFirstPosition(false);
    if(getArrayDesc().getVersionId() > 1)
    {
        if(_storage->findChunk(getArrayDesc(), query, _address))
        {
            if(_address.arrId == getArrayDesc().getId())
            {
                stringstream ss; ss << CoordsToStr(_address.coords);
                _address.coords.clear();
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
                << ss.str();
            }
            else
            {
                assert(_address.arrId < getArrayDesc().getId());
                std::shared_ptr<PersistentChunk> dstChunk = _storage->lookupChunk(getArrayDesc(), _address);
                if (!dstChunk) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_NOT_FOUND);
                }
                PersistentChunk::UnPinner scope(dstChunk.get());
                DBArrayChunk const* dbaChunk = dynamic_cast<DBArrayChunk const*>(&srcChunk);
                if (dbaChunk && dbaChunk->getPersistentChunk() == dstChunk.get())
                {
                    // Original chunk was not changed: no need to do anything!
                    DBArrayChunk *dbChunk = getDBArrayChunk(dstChunk);
                    _currChunk = dbChunk;
                    assert(_currChunk);
                    return *_currChunk;
                }
                //else new delta code goes here!
            }
        }
    }
    std::shared_ptr<ConstRLEEmptyBitmap> nullEmptyBitmap; // to avoid attaching EBM to the chunk
    _currChunk = &ArrayIterator::copyChunk(srcChunk, nullEmptyBitmap);

    assert(dynamic_cast<DBArrayChunk*>(_currChunk));
    _address.arrId = getArrayDesc().getId();

    return *_currChunk;
}

CachedStorage CachedStorage::instance;
Storage* StorageMan::instance = &CachedStorage::instance;

///////////////////////////////////////////////////////////////////
/// DBArrayChunk
///////////////////////////////////////////////////////////////////

CachedStorage::DBArrayChunk::DBArrayChunk(DBArrayIterator& iterator, PersistentChunk* chunk) :
DBArrayChunkBase(chunk), _arrayIter(iterator), _nWriters(0)
{
}

CachedStorage::DBArrayChunkBase::DBArrayChunkBase(PersistentChunk* chunk)
:_inputChunk(chunk)
{
    assert(chunk);
}

const Array& CachedStorage::DBArrayChunkBase::getArray() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getArray");
}

const Array& CachedStorage::DBArrayChunk::getArray() const
{
    return _arrayIter.getArray();
}

const ArrayDesc& CachedStorage::DBArrayChunkBase::getArrayDesc() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getArrayDesc");
}

const ArrayDesc& CachedStorage::DBArrayChunk::getArrayDesc() const
{
    return _arrayIter.getArrayDesc();
}

const AttributeDesc& CachedStorage::DBArrayChunkBase::getAttributeDesc() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getAttributeDesc");
}

const AttributeDesc& CachedStorage::DBArrayChunk::getAttributeDesc() const
{
    return _arrayIter.getAttributeDesc();
}

CompressorType CachedStorage::DBArrayChunkBase::getCompressionMethod() const
{
    return _inputChunk->getCompressionMethod();
}

bool CachedStorage::DBArrayChunkBase::pin() const
{
    LOG4CXX_TRACE(logger, "DBArrayChunkBase::pin() this=" << this << ", _inputChunk=" << _inputChunk);
    return _inputChunk->pin();
}

void CachedStorage::DBArrayChunkBase::unPin() const
{
    LOG4CXX_TRACE(logger, "DBArrayChunkBase::unPin() this=" << this << ", _inputChunk=" << _inputChunk);
    _inputChunk->unPin();
}

Coordinates const& CachedStorage::DBArrayChunkBase::getFirstPosition(bool withOverlap) const
{
    return _inputChunk->getFirstPosition(withOverlap);
}

Coordinates const& CachedStorage::DBArrayChunkBase::getLastPosition(bool withOverlap) const
{
    return _inputChunk->getLastPosition(withOverlap);
}

std::shared_ptr<ConstChunkIterator> CachedStorage::DBArrayChunkBase::getConstIterator(int iterationMode) const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getConstIterator");
}

std::shared_ptr<ConstChunkIterator> CachedStorage::DBArrayChunk::getConstIterator(int iterationMode) const
{
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    Chunk* bitmap(NULL);
    PersistentChunk::UnPinner bitmapScope(NULL);
    std::shared_ptr<Query> query(_arrayIter.getQuery());

    if (bitmapAttr != NULL && bitmapAttr->getId() != DBArrayChunkBase::getAttributeId())
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), DBArrayChunkBase::getCoordinates());
        _arrayIter._storage->findChunk(getArrayDesc(), query, bitmapAddr);
        std::shared_ptr<PersistentChunk> bitmapChunk = _arrayIter._storage->readChunk(getArrayDesc(), bitmapAddr, query);
        bitmapScope.set(bitmapChunk.get());

        DBArrayChunk *dbChunk = _arrayIter.getDBArrayChunk(bitmapChunk);
        assert(dbChunk);

        bitmap = dbChunk;
    }

    PersistentChunk* dbChunk = getPersistentChunk();

    assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
    assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

    dbChunk->pin();

    PersistentChunk::UnPinner selfScope(dbChunk);

    _arrayIter._storage->loadChunk(getArrayDesc(), dbChunk);
    if (getAttributeDesc().isEmptyIndicator()) {
        return std::make_shared<RLEBitmapChunkIterator>(getArrayDesc(),
                                                          DBArrayChunkBase::getAttributeId(),
                                                          (Chunk*) this, bitmap, iterationMode, query);
    } else if ((iterationMode & ConstChunkIterator::INTENDED_TILE_MODE) ||
               (iterationMode & ConstChunkIterator::TILE_MODE)) { //old tile mode

        return std::make_shared<RLEConstChunkIterator>(getArrayDesc(),
                                                         DBArrayChunkBase::getAttributeId(),
                                                         (Chunk*) this, bitmap, iterationMode, query);
    }

    // non-tile mode, but using the new tiles for read-ahead buffering
    std::shared_ptr<RLETileConstChunkIterator> tiledIter =
        std::make_shared<RLETileConstChunkIterator>(getArrayDesc(),
                                                      DBArrayChunkBase::getAttributeId(),
                                                      (Chunk*) this,
                                                      bitmap,
                                                      iterationMode,
                                                      query);
    return std::make_shared< BufferedConstChunkIterator< std::shared_ptr<RLETileConstChunkIterator> > >(tiledIter, query);
    // deprecated formats
}

std::shared_ptr<ChunkIterator>
CachedStorage::DBArrayChunkBase::getIterator(std::shared_ptr<Query> const& query,
                                             int iterationMode)
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getIterator");
}

std::shared_ptr<ChunkIterator>
CachedStorage::DBArrayChunk::getIterator(std::shared_ptr<Query> const& query,
                                         int iterationMode)
{
    if (query != _arrayIter.getQuery()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query");
    }
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    Chunk* bitmap(NULL);
    PersistentChunk::UnPinner bitmapScope(NULL);
    if (bitmapAttr != NULL && bitmapAttr->getId() != DBArrayChunkBase::getAttributeId()
        && !(iterationMode & ConstChunkIterator::NO_EMPTY_CHECK))
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(),  DBArrayChunkBase::getCoordinates());
        std::shared_ptr<PersistentChunk> bitmapChunk = _arrayIter._storage->createChunk(getArrayDesc(),
                                                                                   bitmapAddr,
                                                                                   bitmapAttr->getDefaultCompressionMethod(),query);
        assert(bitmapChunk);
        bitmapScope.set(bitmapChunk.get());

        DBArrayChunk *dbChunk = _arrayIter.getDBArrayChunk(bitmapChunk);
        assert(dbChunk);
        bitmap = dbChunk;
    }
    _nWriters += 1;

    // we should not be storing in sparse format, but
    // there are operators that
    // still generate sparse chunks

    std::shared_ptr<ChunkIterator> iterator =
        std::shared_ptr<ChunkIterator>(new RLEChunkIterator(getArrayDesc(),
                                         DBArrayChunkBase::getAttributeId(),
                                                              this, bitmap,
                                                              iterationMode, query));
    return iterator;
}

std::shared_ptr<ConstRLEEmptyBitmap> CachedStorage::DBArrayChunkBase::getEmptyBitmap() const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::getEmptyBitmap");
}

void CachedStorage::DBArrayChunk::showEmptyBitmap(const std::string & strPrefix) const
{
    if (!isDebug()) { return; } //debug build only

    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    if (bitmapAttr != NULL && bitmapAttr->getId() == DBArrayChunkBase::getAttributeId())
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), DBArrayChunkBase::getCoordinates());

        std::shared_ptr<Query> query(_arrayIter.getQuery());

        _arrayIter._storage->findChunk(getArrayDesc(), query, bitmapAddr);
        std::shared_ptr<scidb::PersistentChunk> bitmapChunk = _arrayIter._storage->readChunk(getArrayDesc(), bitmapAddr, query);

        PersistentChunk::UnPinner scope(bitmapChunk.get());

        DBArrayChunk *dbChunk = _arrayIter.getDBArrayChunk(bitmapChunk);
        assert(dbChunk);

        if(dbChunk->pin()) {
            char const* src = (char const*)dbChunk->getConstData();
            if (src != NULL) {
                ConstRLEEmptyBitmap::Header const* hdr = (ConstRLEEmptyBitmap::Header const*)src;
                assert(hdr->_magic == RLE_EMPTY_BITMAP_MAGIC);

                // uint64_t nNonEmptyElements = hdr->_nNonEmptyElements;
                size_t nSegs = hdr->_nSegs;
                ConstRLEEmptyBitmap::Segment const* seg = (ConstRLEEmptyBitmap::Segment const*)(hdr+1);

                for(size_t k = 0;  k < nSegs; k++)
                {

                    stringstream ss;
                    ss  << strPrefix
                        << " " << getArrayDesc().getName()
                        << " segment["
                        << "  _lPosition="   << seg[k]._lPosition
                        << "  _length="      << seg[k]._length
                        << "  _pPosition="   << seg[k]._pPosition
                        << "]";

                    LOG4CXX_DEBUG(logger, ss.str());
                }
            }

            dbChunk->unPin();
        }
    }
}

std::shared_ptr<ConstRLEEmptyBitmap> CachedStorage::DBArrayChunk::getEmptyBitmap() const
{
    const AttributeDesc* bitmapAttr = getArrayDesc().getEmptyBitmapAttribute();
    std::shared_ptr<ConstRLEEmptyBitmap> bitmap;
    if (bitmapAttr != NULL && bitmapAttr->getId() != DBArrayChunkBase::getAttributeId())
    {
        StorageAddress bitmapAddr(getArrayDesc().getId(), bitmapAttr->getId(), DBArrayChunkBase::getCoordinates());

        std::shared_ptr<Query> query(_arrayIter.getQuery());

        _arrayIter._storage->findChunk(getArrayDesc(), query, bitmapAddr);
        std::shared_ptr<scidb::PersistentChunk> bitmapChunk = _arrayIter._storage->readChunk(getArrayDesc(), bitmapAddr, query);

        PersistentChunk::UnPinner scope(bitmapChunk.get());

        DBArrayChunk *dbChunk = _arrayIter.getDBArrayChunk(bitmapChunk);
        assert(dbChunk);

        bitmap = make_shared<ConstRLEEmptyBitmap>(*dbChunk);
    }
    else
    {
        //XXX shouldn't we just return a NULL ptr ?
        bitmap = ConstChunk::getEmptyBitmap();
    }
    return bitmap;
}

size_t CachedStorage::DBArrayChunkBase::count() const
{
    assert(!isMaterializedChunkPresent());
    if (getArrayDesc().hasOverlap()) {
        // XXX HACK: It appears that the element count stored on disk includes the overlap region.
        // This violates(?) the ConstChunk::count() contract (inferred from implementation),
        // so we fall back to the "canonical" count() if the overlap is present.
        // ArrayIterator::copyChunk() might be the code to blame for incorrectly(?)
        // setting the count on persistent chunks
        return ConstChunk::count();
    }
    const size_t c = _inputChunk->count();

    return (c!=0) ? c : ConstChunk::count();
}

bool CachedStorage::DBArrayChunkBase::isCountKnown() const
{
    assert(!isMaterializedChunkPresent());
    if (!getArrayDesc().hasOverlap() && _inputChunk->isCountKnown()) {
        return true;
    }
    return ConstChunk::isCountKnown();
}

void CachedStorage::DBArrayChunkBase::setCount(size_t count)
{
    _inputChunk->setCount(count);
}

void CachedStorage::DBArrayChunkBase::truncate(Coordinate lastCoord)
{
    _inputChunk->truncate(lastCoord);
}

void CachedStorage::DBArrayChunkBase::merge(ConstChunk const& with, std::shared_ptr<Query>& query)
{
    /* Trying to merge into a DB Chunk indicates an error
     */
    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(getFirstPosition(false));
}

void CachedStorage::DBArrayChunkBase::aggregateMerge(ConstChunk const& with,
                                                     std::shared_ptr<Aggregate> const& aggregate,
                                                     std::shared_ptr<Query>& query)
{
    /* Trying to merge into a DB Chunk indicates an error
     */
    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(getFirstPosition(false));
}

void CachedStorage::DBArrayChunkBase::nonEmptyableAggregateMerge(ConstChunk const& with,
                                                                 std::shared_ptr<Aggregate> const& aggregate,
                                                                 std::shared_ptr<Query>& query)
{
    /* Trying to merge into a DB Chunk indicates an error
     */
    throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
        << CoordsToStr(getFirstPosition(false));
}

void CachedStorage::DBArrayChunkBase::write(const std::shared_ptr<Query>& query)
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::write");
}

void CachedStorage::DBArrayChunk::write(const std::shared_ptr<Query>& query)
{
    if (query != _arrayIter.getQuery()) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query");
    }

    const size_t bitmapSize = getBitmapSize();
    if(bitmapSize != 0) {
        const size_t chunkSize = getSize();
        LOG4CXX_TRACE(logger, "CachedStorage::DBArrayChunk::write =" << this
                      << ", size = "<< chunkSize
                      << ", bitmapsize = "<< bitmapSize);
        assert(chunkSize>bitmapSize);
        reallocate(chunkSize-bitmapSize);
    }
    assert(getBitmapSize() == 0);

    PersistentChunk* dbChunk = getPersistentChunk();

    assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
    assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

    if (--_nWriters <= 0)
    {
        _arrayIter._storage->writeChunk(getArrayDesc(), dbChunk, query);
        _nWriters = 0;
    }
}

void* CachedStorage::DBArrayChunkBase::getWriteData()
{
    return _inputChunk->getData(getArrayDesc());
}

void* CachedStorage::DBArrayChunkBase::getDataForLoad()
{
    return _inputChunk->getDataForLoad();
}

size_t CachedStorage::DBArrayChunkBase::getSize() const
{
    return _inputChunk->getSize();
}

void CachedStorage::DBArrayChunkBase::allocate(size_t size)
{
    _inputChunk->allocate(size);
}

void  CachedStorage::DBArrayChunkBase::reallocate(size_t size)
{
    _inputChunk->reallocate(size);
}

void CachedStorage::DBArrayChunkBase::free()
{
    _inputChunk->free();
}

void
CachedStorage::DBArrayChunkBase::compress(CompressedBuffer& buf,
                                          std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
{
    ASSERT_EXCEPTION_FALSE("DBArrayChunkBase::compress");
}

void
CachedStorage::DBArrayChunk::compress(CompressedBuffer& buf,
                                      std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap) const
{
    if (emptyBitmap)
    {
        MemChunk closure;
        closure.initialize(*this);
        makeClosure(closure, emptyBitmap);
        closure.compress(buf, emptyBitmap);
    }
    else
    {
        PersistentChunk* dbChunk = getPersistentChunk();

        assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
        assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

        PersistentChunk::Pinner scope(dbChunk);
        _arrayIter._storage->compressChunk(getArrayDesc(), dbChunk, buf);
    }
}

void CachedStorage::DBArrayChunk::decompress(CompressedBuffer const& buf)
{
    PersistentChunk* dbChunk = getPersistentChunk();

    assert(dbChunk->getAddress().attId  == DBArrayChunkBase::getAttributeId());
    assert(dbChunk->getAddress().coords == DBArrayChunkBase::getCoordinates());

    _arrayIter._storage->decompressChunk(getArrayDesc(), dbChunk, buf);
}

bool CachedStorage::isPrimaryReplica(PersistentChunk const* chunk, size_t redundancy)
{
    assert(chunk);
    bool res = (chunk->getHeader().instanceId == _hdr.instanceId);
    if ( logger->isTraceEnabled() && (! (res || (redundancy > 0) ))) {
        LOG4CXX_TRACE(logger, "isPrimaryReplica: chunk->getHeader().instanceId "
                      << chunk->getHeader().instanceId );
        LOG4CXX_TRACE(logger, "isPrimaryReplica: _hdr.instanceId " << _hdr.instanceId );
    }
    ASSERT_EXCEPTION((res || (redundancy > 0)),
                     "cannot store replica chunk when redundancy==0");
    return res;
}

}
