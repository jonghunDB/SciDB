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

/*
 * IndexMap.cpp
 *
 * @author Steve Fridella
 */

#include <storage/IndexMap.h>
#include <array/AddressMeta.h>

namespace scidb {

template<class KeyMeta>
typename IndexMap<KeyMeta>::Iterator& BasicIndexMap<KeyMeta>::BasicIterator::operator++()
{
    ++_it;
    return *this;
}

template<class KeyMeta>
bool BasicIndexMap<KeyMeta>::BasicIterator::operator==(const BaseIterator& other) const
{
    // method unused, to be removed under child of SDB-5924
    SCIDB_ASSERT(false);

    BasicIterator const* basicOther = dynamic_cast<BasicIterator const*>(&other);
    if (!basicOther)
        return false;
    else
        return _it == basicOther->_it;
}

template<class KeyMeta>
bool BasicIndexMap<KeyMeta>::BasicIterator::operator!=(const BaseIterator& other) const
{
    // method unused, to be removed under child of SDB-5924
    SCIDB_ASSERT(false);

    return !((*this) == other);
}

template<class KeyMeta>
void BasicIndexMap<KeyMeta>::clear()
{
    _inMemoryMap.clear();
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> BasicIndexMap<KeyMeta>::find(Key const* key)
{
    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.find(key), _inMemoryMap);
    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> BasicIndexMap<KeyMeta>::begin()
{
    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.begin(), _inMemoryMap);
    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> BasicIndexMap<KeyMeta>::end()
{
    // method unused, to be removed under child of SDB-5924
    SCIDB_ASSERT(false);

    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.end(), _inMemoryMap);
    return basicIt;
}

template<class KeyMeta>
std::shared_ptr<typename IndexMap<KeyMeta>::Iterator>
BasicIndexMap<KeyMeta>::leastUpper(Key const* key)
{
    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.lower_bound(key), _inMemoryMap);
    return basicIt;
}

template<class KeyMeta>
bool BasicIndexMap<KeyMeta>::insert(Key const* key,
                                    BufferMgr::BufferHandle& value,
                                    PointerRange<const char> auxMeta)
{
    bool success = false;

    /* Create a new BasicEntry and try to insert it
     */
    std::shared_ptr<BasicEntry> entry;
    std::pair<Key const*, std::shared_ptr<BasicEntry>> mapentry;

    size_t sizeKey = _keyMeta.keySize(key);
    size_t sizeAuxMetadata = auxMeta.size();

    Key* entrykey = reinterpret_cast<Key*>(arena::newVector<char>(*_arena, sizeKey));
    PointerRange<char> entryAuxMeta =
        PointerRange<char>(sizeAuxMetadata, arena::newVector<char>(*_arena, sizeAuxMetadata));

    ASSERT_EXCEPTION((entrykey || sizeKey == 0) && (entryAuxMeta.begin() || sizeAuxMetadata == 0),
                     "In BasicIndexMap::insert(), memory allocation failed.");

    memcpy(reinterpret_cast<char*>(entrykey), reinterpret_cast<char const*>(key), sizeKey);
    memcpy(entryAuxMeta.begin(), auxMeta.begin(), sizeAuxMetadata);

    entry = std::make_shared<BasicEntry>(entrykey, value, entryAuxMeta, _arena);
    mapentry = std::make_pair(entry->getKey(), entry);

    std::pair<typename EntryMap::iterator, bool> insertResult;

    insertResult = _inMemoryMap.insert(mapentry);
    success = insertResult.second;

    return success;
}

template<class KeyMeta>
BufferMgr::BufferHandle BasicIndexMap<KeyMeta>::update(Key const* key,
                                                       BufferMgr::BufferHandle& value,
                                                       PointerRange<const char> auxMeta)
{
    BufferMgr::BufferHandle retVal;

    /* Create a new BasicEntry and try to insert it
     */
    std::shared_ptr<BasicEntry> entry;
    std::pair<Key const*, std::shared_ptr<BasicEntry>> mapentry;

    size_t sizeKey = _keyMeta.keySize(key);
    size_t sizeAuxMetadata = auxMeta.size();

    Key* entrykey = reinterpret_cast<Key*>(arena::newVector<char>(*_arena, sizeKey));
    PointerRange<char> entryAuxMeta =
        PointerRange<char>(sizeAuxMetadata, arena::newVector<char>(*_arena, sizeAuxMetadata));

    ASSERT_EXCEPTION((entrykey || sizeKey == 0) && (entryAuxMeta.begin() || sizeAuxMetadata == 0),
                     "In BasicIndexMap::update(), memory allocation failed.");

    memcpy(reinterpret_cast<char*>(entrykey), reinterpret_cast<char const*>(key), sizeKey);
    memcpy(entryAuxMeta.begin(), auxMeta.begin(), sizeAuxMetadata);

    entry = std::make_shared<BasicEntry>(entrykey, value, entryAuxMeta, _arena);
    mapentry = std::make_pair(entry->getKey(), entry);

    std::pair<typename EntryMap::iterator, bool> insertResult;

    insertResult = _inMemoryMap.insert(mapentry);

    if (!insertResult.second) {
        /* Insert failed due to existing key so save the existing
           value, and update the entry with new value/auxiliary chunk metadata.
        */
        BufferMgr::BufferHandle nullhandle;
        entry->update(nullhandle, PointerRange<char>());

        BasicEntry* target = insertResult.first->second.get();

        arena::destroy(*_arena, target->getAuxMeta().begin());
        retVal = target->getBufHandle();
        target->update(value, entryAuxMeta);
    }

    return retVal;
}

template<class KeyMeta>
void BasicIndexMap<KeyMeta>::erase(BaseIterator& it)
{
    BasicIterator* basicIt = dynamic_cast<BasicIterator*>(&it);
    SCIDB_ASSERT(basicIt);
    _inMemoryMap.erase(basicIt->_it);
}

template<class KeyMeta>
void BasicIndexMap<KeyMeta>::erase(Key const* key)
{
    // method unused, to be removed under child of SDB-5924
    SCIDB_ASSERT(false);

    std::shared_ptr<BasicIterator> basicIt =
        std::make_shared<BasicIterator>(_inMemoryMap.find(key), _inMemoryMap);
    SCIDB_ASSERT(basicIt);
    _inMemoryMap.erase(basicIt->_it);
}

template<class KeyMeta>
void BasicIndexMap<KeyMeta>::rollbackVersion(size_t version)
{
    typename EntryMap::iterator it = _inMemoryMap.begin();
    while (it != _inMemoryMap.end()) {
        typename EntryMap::iterator toRemove = it;
        ++it;
        if (_keyMeta.keyVersion(it->first) == version) {
            _inMemoryMap.erase(toRemove);
        }
    }
}

template class BasicIndexMap<MemAddressMeta>;
template class BasicIndexMap<DbAddressMeta>;
}  // namespace scidb
