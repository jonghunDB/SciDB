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
 * @file PersistentIndexMap.cpp
 *
 * @author Steve Fridella
 */

#include <storage/PersistentIndexMap.h>
#include <array/AddressMeta.h>
#include <system/Config.h>
#include <log4cxx/logger.h>
#include <rocksdb/env.h>

namespace scidb {

static log4cxx::LoggerPtr
    logger(log4cxx::Logger::getLogger("scidb.storage.diskindex"));

template<class KeyMeta>
typename IndexMap<KeyMeta>::Iterator& RocksIndexMap<KeyMeta>::RocksIterator::operator++()
{
    if (!isEnd()) {
        _iter->Next();
        _isBegin = false;
    }
    return *this;
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::RocksIterator::operator==(const BaseIterator& other) const
{
    // method unused, addressed by https://rbcommons.com/s/paradigm4/r/3408/
    SCIDB_ASSERT(false);

    RocksIterator const* basicOther = dynamic_cast<RocksIterator const*>(&other);
    if (!basicOther)
        return false;
    else
        return true;
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::RocksIterator::operator!=(const BaseIterator& other) const
{
    // method unused, addressed by https://rbcommons.com/s/paradigm4/r/3408/
    SCIDB_ASSERT(false);

    return !((*this) == other);
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::RocksIterator::isBegin() const
{
    // method unused, addressed by https://rbcommons.com/s/paradigm4/r/3408/
    SCIDB_ASSERT(false);

    return _isBegin;
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::RocksIterator::isEnd() const
{
    KeyMeta km;
    typename KeyMeta::Key const& currentKey = KeyComp::SliceAsKey(_iter->key());
    bool result = !km.keyLess(&currentKey, _map->_keyMax);

    // clang-format off
    LOG4CXX_TRACE(logger, "RocksIterator::isEnd " << (result ? "true" : "false")
                  << ", currentKey= " << km.keyToString(&currentKey)
                  << " keyMax= " << km.keyToString(_map->_keyMax));
    // clang-format on

    return result;
}

template<class KeyMeta>
size_t RocksIndexMap<KeyMeta>::RocksIterator::valueSize() const
{
    Entry const* e = reinterpret_cast<Entry const*>(_iter->value().data());
    return e->getBufHandle().size();
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::RocksIterator::isNullValue() const
{
    Entry const* e = reinterpret_cast<Entry const*>(_iter->value().data());
    return e->getBufHandle().isNull();
}

template<class KeyMeta>
typename KeyMeta::Key const* RocksIndexMap<KeyMeta>::RocksIterator::getKey()
{
    return reinterpret_cast<Key const*>(_iter->key().data());
}

template<class KeyMeta>
BufferMgr::BufferHandle& RocksIndexMap<KeyMeta>::RocksIterator::getBufHandle()
{
    Entry const* e = reinterpret_cast<Entry const*>(_iter->value().data());
    return const_cast<BufferMgr::BufferHandle&>(e->getBufHandle());
}

template<class KeyMeta>
PointerRange<const char> RocksIndexMap<KeyMeta>::RocksIterator::getAuxMeta()
{
    Entry const* e = reinterpret_cast<Entry const*>(_iter->value().data());
    return e->getAuxMeta();
}

template<class KeyMeta>
typename RocksIndexMap<KeyMeta>::Entry*
RocksIndexMap<KeyMeta>::Entry::makeEntry(arena::ArenaPtr arena,
                                         BufferMgr::BufferHandle const& value,
                                         PointerRange<const char> auxMeta)
{
    /* Allocate enough contiguous memory to hold an Entry with a stat
       buffer of the given size.
     */
    Entry* entry = reinterpret_cast<Entry*>(
        arena::newVector<char>(*arena, sizeof(Entry)));

    /* Fill in the entry.
     */
    entry->_value = value;
    memcpy(&(entry->_auxMetaData), auxMeta.begin(), auxMeta.size());

    return entry;
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::Entry::EntryAsSlice(Entry& entry, rocksdb::Slice& slice)
{
    entry._value.resetSlotGenCount();
    rocksdb::Slice newSlice(reinterpret_cast<char const*>(&entry), entry.size());
    slice = newSlice;
}

template<class KeyMeta>
RocksIndexMap<KeyMeta>::ScidbCompactFactory::ScidbCompactFactory(RocksDbManager* dbm,
                                                                 arena::ArenaPtr& arena)
    : _dbm(dbm)
    , _arena(arena)
{}

template<class KeyMeta>
RocksIndexMap<KeyMeta>::ScidbCompactFactory::~ScidbCompactFactory()
{}

template<class KeyMeta>
std::unique_ptr<rocksdb::CompactionFilter>
RocksIndexMap<KeyMeta>::ScidbCompactFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context)
{
    // clang-format off
    LOG4CXX_DEBUG(logger, "ScidbCompactFactory::CreateCompactionFilter");
    // clang-format on

    std::unique_ptr<rocksdb::CompactionFilter> result(new ScidbCompact(this));
    return result;
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::ScidbCompactFactory::addDeletedIndex(DataStore::DataStoreKey& dsk)
{
    ScopedMutexLock sm(_deleteMutex);

    // clang-format off
    LOG4CXX_DEBUG(logger, "ScidbCompactFactory::addDeletedIndex(): dsk: " <<
                  dsk.toString());
    // clang-format on

    _deletedIndexes.insert(dsk);
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::ScidbCompactFactory::isDeletedIndex(DataStore::DataStoreKey& dsk)
{
    ScopedMutexLock sm(_deleteMutex);
    return (_deletedIndexes.find(dsk) != _deletedIndexes.end());
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::ScidbCompactFactory::checkDeletedSet()
{
    ScopedMutexLock sm(_deleteMutex);

    // clang-format off
    LOG4CXX_DEBUG(logger, "ScidbCompactFactory::checkDeletedSet()");
    // clang-format on

    /* Iterate the elements of the deleted index set...
     */
    std::set<DataStore::DataStoreKey>::iterator it = _deletedIndexes.begin();

    while (it != _deletedIndexes.end()) {
        /* Determine if the dsk is still anywhere in the md db.
           To do this we seek to the greatest lower bound of all
           keys that share the dsk, and see if any such keys exist
           at all.
         */
        std::set<DataStore::DataStoreKey>::iterator deleteIt = _deletedIndexes.end();
        rocksdb::Slice targetSlice;
        typename KeyMeta::Key* keyTarget = _keyMeta.keyMax(_arena, *it);
        rocksdb::DB* dbconn = _dbm->getDbConnection(keyTarget->dsk());
        rocksdb::Iterator* rocksIt = dbconn->NewIterator(rocksdb::ReadOptions());

        keyTarget->makePredecessor();
        KeyComp::KeyAsSlice(*keyTarget, targetSlice);
        rocksIt->Seek(targetSlice);

        if (rocksIt->Valid()) {
            typename KeyMeta::Key const& currentKey = KeyComp::SliceAsKey(rocksIt->key());

            if (currentKey.dsk() != *it) {
                /* This index is completely deleted from the map
                 */
                deleteIt = it;
            }
        } else {
            deleteIt = it;
        }

        delete rocksIt;
        arena::destroy(*_arena, keyTarget);
        ++it;
        if (deleteIt != _deletedIndexes.end()) {
            // clang-format off
            LOG4CXX_DEBUG(logger, "ScidbCompactFactory::checkDeletedSet()," <<
                          "found deleted index: " << deleteIt->toString());
            // clang-format on
            _deletedIndexes.erase(deleteIt);
        }
    }
}

template<class KeyMeta>
RocksIndexMap<KeyMeta>::ScidbCompact::~ScidbCompact()
{
    // clang-format off
    LOG4CXX_DEBUG(logger, "ScidbCompact::~ScidbCompact, " <<
                  "finished compaction, examined: " << _nKeyExamined <<
                  " filtered: " << _nKeyFiltered);
    // clang-format on

    _fac->checkDeletedSet();
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::ScidbCompact::Filter(int level,
                                                  const rocksdb::Slice& key,
                                                  const rocksdb::Slice& existing_value,
                                                  std::string* new_value,
                                                  bool* value_changed) const
{
    /* Return true if the key/value should be removed during the compaction.
     */
    typename KeyMeta::Key const& targetKey = KeyComp::SliceAsKey(key);
    DataStore::DataStoreKey targetDsk = targetKey.dsk();

    ++_nKeyExamined;
    *value_changed = false;
    if (targetDsk != _savedDsk) {
        _savedDsk = targetDsk;
        _savedRes = _fac->isDeletedIndex(targetDsk);
    }
    if (_savedRes) {
        ++_nKeyFiltered;
    }
    return _savedRes;
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::RocksDbManager::clearDbCb(std::string dbPath, struct dirent* de)
{
    // method unused, addressed by https://rbcommons.com/s/paradigm4/r/3408/
    SCIDB_ASSERT(false);

    std::string fullpath = dbPath + "/" + de->d_name;
    File::remove(fullpath.c_str(), false);
}

template<class KeyMeta>
RocksIndexMap<KeyMeta>::RocksDbManager::RocksDbManager()
{
    // clang-format off
    LOG4CXX_DEBUG(logger, "Calling RocksDbManager constructor!");
    // clang-format on

    /* Set up background threads for compaction (low) and flushing (high)
     */
    auto env = rocksdb::Env::Default();
    env->SetBackgroundThreads(2, rocksdb::Env::LOW);
    env->SetBackgroundThreads(1, rocksdb::Env::HIGH);

    /* Create a compaction filter factory
     */
    arena::ArenaPtr arena = BufferMgr::getInstance()->getArena();

    _dbCompact = std::make_shared<ScidbCompactFactory>(this, arena);

    /* Determine the path for the metadata db
     */
    std::string dbPath = "";
    const std::string& configPath =
        Config::getInstance()->getOption<std::string>(CONFIG_STORAGE);

    size_t pathEnd = configPath.find_last_of('/');
    if (pathEnd != std::string::npos) {
        dbPath = configPath.substr(0, pathEnd + 1);
    }
    dbPath += "/metadata";

    /* Create the db from scratch or open it.
     */
    rocksdb::Options options;
    options.create_if_missing = true;
    options.comparator = &_kc;
    options.env = env;
    options.max_background_compactions = 2;          // will use low-pri thread pool
    options.max_background_flushes = 1;              // will use high-pri thread pool
    options.level0_file_num_compaction_trigger = 1;  // compact with > 1 files
    options.compaction_options_universal.max_size_amplification_percent = 10;
    options.compaction_filter_factory = _dbCompact;
    options.compaction_style = rocksdb::kCompactionStyleUniversal;

    rocksdb::Status status = rocksdb::DB::Open(options, dbPath, &_dbConn);
    SCIDB_ASSERT(status.ok());
}

template<class KeyMeta>
RocksIndexMap<KeyMeta>::RocksDbManager::~RocksDbManager()
{
    // clang-format off
    LOG4CXX_DEBUG(logger, "Calling RocksDbManager destructor!");
    // clang-format on
    delete _dbConn;
}

template<class KeyMeta>
rocksdb::DB*
RocksIndexMap<KeyMeta>::RocksDbManager::getDbConnection(const DataStore::DataStoreKey& dsk)
{
    return _dbConn;
}

template<class KeyMeta>
typename RocksIndexMap<KeyMeta>::ScidbCompactFactory&
RocksIndexMap<KeyMeta>::RocksDbManager::getCompactFac(const DataStore::DataStoreKey& dsk)
{
    return *_dbCompact;
}

template<class KeyMeta>
RocksIndexMap<KeyMeta>::RocksIndexMap(DataStore::DataStoreKey const& dsk,
                                      KeyMeta const& keyMeta,
                                      arena::ArenaPtr const& arena)
    : _dsk(dsk)
    , _keyMeta(keyMeta)
    , _arena(arena)
{
    /* Open a connection to the database
     */
    _dbConn = RocksDbManager::getInstance()->getDbConnection(_dsk);
    SCIDB_ASSERT(_dbConn);

    /* Get the end-of-index marker and insert it
     */
    _keyMax = keyMeta.keyMax(_arena, _dsk);
    rocksdb::Slice keyMaxSlice;
    rocksdb::Slice valueMaxSlice;
    KeyComp::KeyAsSlice(*_keyMax, keyMaxSlice);
    _dbConn->Put(rocksdb::WriteOptions(), keyMaxSlice, valueMaxSlice);

    /* Create _keyMaxPred ---> greatest lower bound for all keys in this index
     */
    size_t keyMaxSize = _keyMeta.keySize(_keyMax);
    _keyMaxPred = reinterpret_cast<Key*>(arena::newVector<char>(*_arena, keyMaxSize));
    memcpy(_keyMaxPred, _keyMax, keyMaxSize);
    _keyMaxPred->makePredecessor();
}

template<class KeyMeta>
RocksIndexMap<KeyMeta>::~RocksIndexMap()
{
    /* Free the end-of-index marker and predecessor
     */
    arena::destroy(*_arena, _keyMax);
    arena::destroy(*_arena, _keyMaxPred);
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::clear()
{
    KeyMeta km;

    // clang-format off
    LOG4CXX_DEBUG(logger, "RocksIndexMap::clear(): max " <<
                  km.keyToString(_keyMax) << " min " <<
                  km.keyToString(_keyMaxPred));
    // clang-format on

    /* Mark this index as being deleted.  It will be cleaned in the
       compaction filter.
    */
    RocksDbManager* dbMan = RocksDbManager::getInstance();
    ScidbCompactFactory& dbCompact = dbMan->getCompactFac(_dsk);

    dbCompact.addDeletedIndex(_dsk);

    _dbConn->CompactRange(rocksdb::CompactRangeOptions(), NULL, NULL);
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> RocksIndexMap<KeyMeta>::find(Key const* key)
{
    /* Search the map for element.  If not found, return iterator
       for which isEnd() == true.
     */
    rocksdb::Slice keySlice;
    rocksdb::Iterator* rocksIt = _dbConn->NewIterator(rocksdb::ReadOptions());
    std::shared_ptr<RocksIterator> basicIt = std::make_shared<RocksIterator>(rocksIt, this);

    KeyComp::KeyAsSlice(*key, keySlice);
    rocksIt->Seek(keySlice);
    SCIDB_ASSERT(rocksIt->Valid());

    typename KeyMeta::Key const& foundkey = KeyComp::SliceAsKey(rocksIt->key());
    if (!(_keyMeta.keyEqual(key, &foundkey))) {
        LOG4CXX_TRACE(logger, "RocksIndexMap()::find key = "
                      << _keyMeta.keyToString(key) << " -- NOT FOUND");
        /* Key was not found, seek to the end-of-index marker
         */
        KeyComp::KeyAsSlice(*_keyMax, keySlice);
        rocksIt->Seek(keySlice);
        SCIDB_ASSERT(rocksIt->Valid());
    } else {
        LOG4CXX_TRACE(logger, "RocksIndexMap()::find key = "
                              << _keyMeta.keyToString(key) << " --> " << basicIt->getBufHandle());
    }
    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> RocksIndexMap<KeyMeta>::begin()
{
    /* Search the map for the first element beyond the predecessor's max
       key.
     */
    rocksdb::Slice keySlice;
    rocksdb::Iterator* rocksIt = _dbConn->NewIterator(rocksdb::ReadOptions());
    std::shared_ptr<RocksIterator> basicIt = std::make_shared<RocksIterator>(rocksIt, this);

    KeyComp::KeyAsSlice(*_keyMaxPred, keySlice);
    rocksIt->Seek(keySlice);

    typename KeyMeta::Key const& foundkey = KeyComp::SliceAsKey(rocksIt->key());
    if (_keyMeta.keyEqual(_keyMaxPred, &foundkey)) {
        rocksIt->Next();
    }

    // Looking at valid entry because keyMax sentinel must still exist.
    SCIDB_ASSERT(rocksIt->Valid());

    // If no chunks yet, this iterator can also be isEnd().
    basicIt->_isBegin = true;

    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> RocksIndexMap<KeyMeta>::end()
{
    // method unused, addressed by https://rbcommons.com/s/paradigm4/r/3408/
    SCIDB_ASSERT(false);

    /* Search the map for the index's max key.
     */
    rocksdb::Slice keySlice;
    rocksdb::Iterator* rocksIt = _dbConn->NewIterator(rocksdb::ReadOptions());
    std::shared_ptr<RocksIterator> basicIt = std::make_shared<RocksIterator>(rocksIt, this);

    KeyComp::KeyAsSlice(*_keyMax, keySlice);
    rocksIt->Seek(keySlice);
    SCIDB_ASSERT(rocksIt->Valid());
    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator>
RocksIndexMap<KeyMeta>::leastUpper(Key const* key)
{
    /* Search the map for first element greater or equal to key
     */
    rocksdb::Slice keySlice;
    rocksdb::Iterator* rocksIt = _dbConn->NewIterator(rocksdb::ReadOptions());
    std::shared_ptr<RocksIterator> basicIt = std::make_shared<RocksIterator>(rocksIt, this);

    KeyComp::KeyAsSlice(*key, keySlice);
    rocksIt->Seek(keySlice);
    SCIDB_ASSERT(rocksIt->Valid());

    return basicIt;
}

template<class KeyMeta>
bool RocksIndexMap<KeyMeta>::insert(Key const* key,
                                    BufferMgr::BufferHandle& value,
                                    PointerRange<const char> auxMeta)
{
    Entry* entry = Entry::makeEntry(_arena, value, auxMeta);
    rocksdb::Slice keySlice, entrySlice;
    KeyMeta km;
    LOG4CXX_TRACE(logger, "RocksIndexMap::insert, Key= "
                  << km.keyToString(key) << " value=" << value)


    KeyComp::KeyAsSlice(*key, keySlice);
    Entry::EntryAsSlice(*entry, entrySlice);
    rocksdb::Status s;

    std::string existValue;

    /* Find the key in the map.  If it exists, return false
     */
    s = _dbConn->Get(rocksdb::ReadOptions(), keySlice, &existValue);
    if (s.ok() || hasInjectedError(ROCKS_GET_FAIL, __LINE__, __FILE__)) {
        arena::destroy(*_arena, entry);
        return false;
    }
    SCIDB_ASSERT(s.IsNotFound());

    /* Insert the entry
     */
    s = _dbConn->Put(rocksdb::WriteOptions(), keySlice, entrySlice);
    SCIDB_ASSERT(s.ok());
    arena::destroy(*_arena, entry);

    return true;
}

template<class KeyMeta>
BufferMgr::BufferHandle RocksIndexMap<KeyMeta>::update(Key const* key,
                                                       BufferMgr::BufferHandle& value,
                                                       PointerRange<const char> auxMeta)
{
    KeyMeta km;
    LOG4CXX_TRACE(logger, "RocksIndexMap::update, Key= "
                  << km.keyToString(key) << " value=" << value)

    BufferMgr::BufferHandle result;
    Entry* entry = Entry::makeEntry(_arena, value, auxMeta);
    rocksdb::Slice keySlice, entrySlice;

    KeyComp::KeyAsSlice(*key, keySlice);
    Entry::EntryAsSlice(*entry, entrySlice);
    rocksdb::Status s;

    std::string existValue;

    /* Find the key in the map.  If it exists, save the
       value.
     */
    s = _dbConn->Get(rocksdb::ReadOptions(), keySlice, &existValue);
    if (s.ok()) {
        rocksdb::Slice existEntrySlice(existValue);
        Entry const* e = reinterpret_cast<Entry const*>(existEntrySlice.data());
        result = e->getBufHandle();
    }

    /* Insert the entry
     */
    s = _dbConn->Put(rocksdb::WriteOptions(), keySlice, entrySlice);
    SCIDB_ASSERT(s.ok());
    arena::destroy(*_arena, entry);

    return result;
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::erase(BaseIterator& it)
{
    RocksIterator* basicIt = dynamic_cast<RocksIterator*>(&it);
    SCIDB_ASSERT(basicIt);

    rocksdb::Slice keySlice;

    KeyComp::KeyAsSlice(*(basicIt->getKey()), keySlice);

    rocksdb::Status s = _dbConn->Delete(rocksdb::WriteOptions(), keySlice);
    SCIDB_ASSERT(s.ok());
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::erase(Key const* key)
{
    rocksdb::Slice keySlice;

    KeyComp::KeyAsSlice(*key, keySlice);

    rocksdb::Status s = _dbConn->Delete(rocksdb::WriteOptions(), keySlice);
    SCIDB_ASSERT(s.ok());
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::flush()
{
    /* It suffices to sync the write-ahead-log for rocksdb11.
       This flushes all meta-data changes for ALL persistent
       index maps so is a big hammer.  Its not clear whether
       a more granular approach is possible, as long as all
       maps share the same rocksdb11 table.
     */
    rocksdb::Status s = _dbConn->SyncWAL();
    SCIDB_ASSERT(s.ok());
}

template<class KeyMeta>
void RocksIndexMap<KeyMeta>::rollbackVersion(size_t version)
{
    LOG4CXX_DEBUG(logger, "RocksIndexMap::rollbackVersion version=" << version);

    /* Delete all keys associated with this version
     */
    rocksdb::WriteBatch batch;
    rocksdb::Status s;
    std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> ip = begin();
    std::shared_ptr<RocksIterator> rp = std::dynamic_pointer_cast<RocksIterator>(ip);

    while (!rp->isEnd()) {
        if (_keyMeta.keyVersion(rp->getKey()) == version) {
            batch.Delete(rp->_iter->key());
        }
        ++(*rp);
    }
    s = _dbConn->Write(rocksdb::WriteOptions(), &batch);
    SCIDB_ASSERT(s.ok());
}

template class RocksIndexMap<MemAddressMeta>;
template class RocksIndexMap<DbAddressMeta>;
}  // namespace scidb
