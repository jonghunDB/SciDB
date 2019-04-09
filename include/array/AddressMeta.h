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
 * @file AddressMeta.h
 *
 * @brief Class suitable to be used as unique key for array chunk
 * mapping.
 */

#ifndef ADDRESS_META_H_
#define ADDRESS_META_H_

#include <array/Address.h>
#include <storage/IndexMgr.h>
#include <util/DataStore.h>

namespace scidb {

/**
 * MemAddressMeta is a helper class suitable for instantiating
 * a DiskIndex for non-persistent arrays.
 */
class MemAddressMeta : public DiskIndexKeyMetaBase
{
public:
    /**
     * The structure of a key
     */
    struct Key : public DiskIndexKeyMetaBase::KeyBase
    {
        Key(DataStore::DataStoreKey const& dsk)
            : DiskIndexKeyMetaBase::KeyBase(dsk)
        {}

        // clang-format off
        AttributeID             _attId;
        size_t                  _nDims;
        Coordinate              _coords[1];
        // clang-format on
    };

    /**
     * A less-than function object over two keys (given pointers to them).
     */
    struct KeyLess
    {
        bool operator()(Key const* key1, Key const* key2) const
        {
            DiskIndexKeyMetaBase kmb;

            if (!kmb.keyEqualBase(key1, key2)) {
                return kmb.keyLessBase(key1, key2);
            }
            if (key1->_nDims != key2->_nDims) {
                return key1->_nDims > key2->_nDims;
            }
            if (key1->_attId != key2->_attId) {
                return key1->_attId < key2->_attId;
            }
            for (size_t i = 0; i < key1->_nDims; ++i) {
                if (key1->_coords[i] != key2->_coords[i]) {
                    return key1->_coords[i] < key2->_coords[i];
                }
            }
            return false;
        }
    };

    /**
     * A equal-to function object over two keys (given pointers to them).
     */
    struct KeyEqual
    {
        bool operator()(Key const* key1, Key const* key2) const
        {
            DiskIndexKeyMetaBase kmb;

            if (!kmb.keyEqualBase(key1, key2)) {
                return false;
            }
            if (key1->_nDims != key2->_nDims) {
                return false;
            }
            if (key1->_attId != key2->_attId) {
                return false;
            }
            for (size_t i = 0; i < key1->_nDims; ++i) {
                if (key1->_coords[i] != key2->_coords[i]) {
                    return false;
                }
            }
            return true;
        }
    };

    /**
     * A function object that returns the version of a key (given a pointer to it).
     * For MemArray, every key has the same version (=0).
     */
    struct KeyVersion
    {
        size_t operator()(Key const* key) const { return 0; }
    };

    /**
     * A function object returning the size of a key (given a pointer to it).
     */
    struct KeySize
    {
        size_t operator()(Key const* key) const
        {
            return sizeof(Key) +
                sizeof(Coordinate) * (key->_nDims - 1);  // extra _coords beyond 1
        }
    };

    /**
     * A function object returning a string serialization of the key
     */
    struct KeyToString
    {
        std::string operator()(Key const* key) const
        {
            DiskIndexKeyMetaBase kmb;
            std::stringstream ss;
            ss << "{ \"keybase\": " << kmb.keyToStringBase(key)
               << " , \"ndims\": " << key->_nDims
               << " , \"attid\": " << key->_attId
               << " , \"coords\": [";
            for (size_t i = 0; i < key->_nDims;) {
                ss << key->_coords[i];
                if (++i < key->_nDims)
                    ss << ", ";
            }
            ss << "] }";
            return ss.str();
        }
    };

    /**
     * A function object which returns a reference to a copy of the
     * maximum possible key for a given index.
     */
    struct KeyMax
    {
        Key* operator()(arena::ArenaPtr arena, DataStore::DataStoreKey const& dsk) const
        {
            Key* maxKey = arena::newScalar<Key>(*arena, dsk);
            maxKey->_attId = 0;
            maxKey->_nDims = 0;
            return maxKey;
        }
    };

public:  // key-manipulation function objects.
    // clang-format off
    KeyLess     const keyLess = KeyLess();
    KeyEqual    const keyEqual = KeyEqual();
    KeyVersion  const keyVersion = KeyVersion();
    KeySize     const keySize = KeySize();
    KeyToString const keyToString = KeyToString();
    KeyMax      const keyMax = KeyMax();
    // clang-format on

public:  // public methods DiskIndex does not need to know.
    /**
     * Given a dsk, attributeID and a chunkCoords, fill in a key.
     * @pre  key must have enough space
     * @param[out] key         a key to fill.
     * @param[in]  dsk         a DataStoreKey.
     * @param[in]  attrId      an attribute ID.
     * @param[in]  chunkCoords the chunk's start coordinates.
     */
    void fillKey(Key* key,
                 DataStore::DataStoreKey const& dsk,
                 AttributeID const& attId,
                 CoordinateCRange const& chunkCoords) const
    {
        key = new (key) Key(dsk);
        key->_attId = attId;
        key->_nDims = chunkCoords.size();

        for (size_t i = 0; i < chunkCoords.size(); ++i) {
            key->_coords[i] = chunkCoords[i];
        }
    }

    /**
     * Given a dsk and an Address object, fill in a key.
     * @pre  key must have enough space
     * @param[out] key         a key to fill.
     * @param[in]  dsk         a DataStoreKey
     * @param[in]  address     a scidb::Address object.
     */
    void keyFromAddress(Key* key, DataStore::DataStoreKey const& dsk, Address const& address) const
    {
        fillKey(key, dsk, address.attId, address.coords);
    }

    /**
     * Given a Key object, fill in an Address.
     * @param[in]  key         a key.
     * @param[out] address     a scidb::Address object to fill.
     */
    void keyToAddress(Key const* key, Address& address) const
    {
        address.coords.resize(key->_nDims);
        address.attId = key->_attId;
        for (size_t i = 0; i < key->_nDims; ++i) {
            address.coords[i] = key->_coords[i];
        }
    }

    /**
     * A wrapper over a Key object, which owns the key's memory space.
     */
    class KeyWithSpace
    {
    public:
        KeyWithSpace()
            : _buffer(NULL)
        {}

        ~KeyWithSpace()
        {
            if (_buffer) {
                delete[] _buffer;
            }
        }

        void initializeKey(MemAddressMeta const& addr, size_t nDims)
        {
            DataStore::DataStoreKey dsk;
            Key key(dsk);
            key._nDims = nDims;
            _buffer = new char[addr.keySize(&key)];
        }

        Key* getKey() const { return reinterpret_cast<Key*>(_buffer); }

    private:
        char* _buffer;
    };
};

/**
 * DbAddressMeta is a version of scidb::Address which is suitable for
 * instantiating a DiskIndex for persistent arrays.
 */
class DbAddressMeta : public DiskIndexKeyMetaBase
{
public:
    /**
     * The structure of a key.  DbAddressMeta::Key objects  have an
     * interesting ordering scheme. They are ordered by:
     * AttributeID, Coordinates, ArrayID (reverse).
     *
     * For a given array, you will see this kind of ordering:
     *
     * - AttributeID = 0
     *   - Coordinates = {0,0}
     *     - ArrayID = 1 --> CHUNK (this chunk exists in all versions >= 1)
     *     - ArrayID = 0 --> CHUNK (this chunk exists only in version 0)
     *   - Coordinates = {0,10}
     *     - ArrayID = 2 --> NULL (tombstone)
     *     - ArrayID = 0 --> CHUNK (this chunk exists only in versions 0 and 1; there's a tombstone at 2)
     * - AttributeID = 1
     */
    struct Key : public DiskIndexKeyMetaBase::KeyBase
    {
        Key(DataStore::DataStoreKey const& dsk)
            : DiskIndexKeyMetaBase::KeyBase(dsk)
        {}

        // clang-format off
        size_t                  _nDims;
        AttributeID             _attId;
        ArrayID                 _arrVerId;
        Coordinate              _coords[1];
        // clang-format on
    };

    /**
     * A less-than function object over two keys (given pointers to them).
     */
    struct KeyLess
    {
        bool operator()(Key const* key1, Key const* key2) const
        {
            DiskIndexKeyMetaBase kmb;

            if (!kmb.keyEqualBase(key1, key2)) {
                return kmb.keyLessBase(key1, key2);
            }
            if (key1->_nDims != key2->_nDims) {
                // *Not* a bug: _nDims == 0 denotes keyMax sentinel.
                return key1->_nDims > key2->_nDims;
            }
            if (key1->_attId != key2->_attId) {
                return key1->_attId < key2->_attId;
            }
            for (size_t i = 0; i < key1->_nDims; ++i) {
                if (key1->_coords[i] != key2->_coords[i]) {
                    return key1->_coords[i] < key2->_coords[i];
                }
            }
            if (key1->_arrVerId != key2->_arrVerId) {
                // Most recent version comes first.
                return key1->_arrVerId > key2->_arrVerId;
            }
            return false;
        }
    };

    /**
     * A equal-to function object over two keys (given pointers to them).
     */
    struct KeyEqual
    {
        bool operator()(Key const* key1, Key const* key2) const
        {
            DiskIndexKeyMetaBase kmb;

            if (!kmb.keyEqualBase(key1, key2)) {
                return false;
            }
            if (key1->_nDims != key2->_nDims) {
                return false;
            }
            if (key1->_attId != key2->_attId) {
                return false;
            }
            for (size_t i = 0; i < key1->_nDims; ++i) {
                if (key1->_coords[i] != key2->_coords[i]) {
                    return false;
                }
            }
            if (key1->_arrVerId != key2->_arrVerId) {
                return false;
            }
            return true;
        }
    };

    /**
     * A function object that returns the array version id of a key
     * (given a pointer to it).
     */
    struct KeyVersion
    {
    public:
        size_t operator()(Key const* key) const { return key->_arrVerId; }
    };

    /**
     * A function object returning the size of a key
     * (given a pointer to it).
     */
    struct KeySize
    {
        size_t operator()(Key const* key) const
        {
            /* size of basic Key, plus size of extra _coords beyond 1
             */
            return sizeof(Key) + sizeof(Coordinate) * (key->_nDims - 1);
        }
    };

    /**
     * A function object returning a string serialization of the key
     */
    struct KeyToString
    {
        std::string operator()(Key const* key) const
        {
            DiskIndexKeyMetaBase kmb;
            std::stringstream ss;
            ss << "{ \"keybase\": " << kmb.keyToStringBase(key)
               << " , \"ndims\": " << key->_nDims
               << " , \"attid\": " << key->_attId
               << " , \"coords\": [";
            for (size_t i = 0; i < key->_nDims;) {
                ss << key->_coords[i];
                if (++i < key->_nDims)
                    ss << ", ";
            }
            ss << "] , \"arrverid\": " << key->_arrVerId
               << " }";
            return ss.str();
        }
    };

    /**
     * A function object which returns a reference to a copy of
     * the maximum possible key for a given index.  (See KeyLess
     * re. why zeroes means maximum.)
     */
    struct KeyMax
    {
        Key* operator()(arena::ArenaPtr arena, DataStore::DataStoreKey const& dsk) const
        {
            Key* maxKey = arena::newScalar<Key>(*arena, dsk);
            maxKey->_attId = 0;
            maxKey->_arrVerId = 0;
            maxKey->_nDims = 0;
            maxKey->_coords[0] = 0;
            return maxKey;
        }
    };

public:  // key-manipulation function objects.
    // clang-format off
    KeyLess     const keyLess = KeyLess();
    KeyEqual    const keyEqual = KeyEqual();
    KeyVersion  const keyVersion = KeyVersion();
    KeySize     const keySize = KeySize();
    KeyToString const keyToString = KeyToString();
    KeyMax      const keyMax = KeyMax();
    // clang-format on

public:  // public methods DiskIndex does not need to know.
    /**
     * Given a dsk, attributeID, chunkCoords, and a version
     * array id, fill in a key.
     * @pre  key must have enough space
     * @param[out] key         a key to fill.
     * @param[in]  dsk         a DataStoreKey.
     * @param[in]  attrId      an attribute ID.
     * @param[in]  chunkCoords the chunk's start coordinates.
     * @param[in]  arrVerId    the array version number
     */
    void fillKey(Key* key,
                 DataStore::DataStoreKey const& dsk,
                 AttributeID const& attId,
                 CoordinateCRange const& chunkCoords,
                 ArrayID const& arrVerId = 0) const
    {
        key = new (key) Key(dsk);
        key->_attId = attId;
        key->_arrVerId = arrVerId;
        key->_nDims = chunkCoords.size();

        for (size_t i = 0; i < chunkCoords.size(); ++i) {
            key->_coords[i] = chunkCoords[i];
        }
    }

    /**
     * Given a dsk a PersistentAddress object, fill in a key.
     * @pre  key must have enough space
     * @param[out] key         a key to fill.
     * @param[in]  dsk         a DataStoreKey
     * @param[in]  address     a scidb::PersistentAddress object.
     */
    void keyFromAddress(Key* key,
                        DataStore::DataStoreKey const& dsk,
                        PersistentAddress const& address) const
    {
        fillKey(key, dsk, address.attId, address.coords, address.arrVerId);
    }

    /**
     * Given a Key object, fill in a PersistentAddress.
     * @param[in]  key         a key.
     * @param[out] address     a scidb::PersistentAddress object to fill.
     */
    void keyToAddress(Key const* key, PersistentAddress& address) const
    {
        address.coords.resize(key->_nDims);
        address.attId = key->_attId;
        for (size_t i = 0; i < key->_nDims; ++i) {
            address.coords[i] = key->_coords[i];
        }
        address.arrVerId = key->_arrVerId;
    }

    /**
     * A wrapper over a Key object, which owns the key's memory space.
     */
    class KeyWithSpace
    {
    public:
        KeyWithSpace()
            : _buffer(NULL)
        {}

        ~KeyWithSpace()
        {
            if (_buffer) {
                delete[] _buffer;
            }
        }

        void initializeKey(DbAddressMeta const& addr, size_t nDims)
        {
            DataStore::DataStoreKey dsk;
            Key key(dsk);
            key._nDims = nDims;
            _buffer = new char[addr.keySize(&key)];
        }

        Key* getKey() const { return reinterpret_cast<Key*>(_buffer); }

    private:
        char* _buffer;
    };
};

}  // namespace scidb

#endif  // ADDRESS_META_H_
