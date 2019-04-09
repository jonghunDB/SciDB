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

#ifndef ADDRESS_META_UNIT_TESTS_H_
#define ADDRESS_META_UNIT_TESTS_H_

#include <array/AddressMeta.h>

namespace scidb {

struct AddressMetaUnitTest : CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(AddressMetaUnitTest);
    CPPUNIT_TEST(MemAddressMeta_KeyLess_1);
    CPPUNIT_TEST(MemAddressMeta_KeyLess_2);
    CPPUNIT_TEST(MemAddressMeta_KeyEqual_1);
    CPPUNIT_TEST(MemAddressMeta_KeyEqual_2);
    CPPUNIT_TEST(MemAddressMeta_KeyEqual_3);
    CPPUNIT_TEST(MemAddressMeta_KeyEqual_4);
    CPPUNIT_TEST(MemAddressMeta_KeyToString);
    CPPUNIT_TEST(DbAddressMeta_KeyEqual_1);
    CPPUNIT_TEST(DbAddressMeta_KeyEqual_2);
    CPPUNIT_TEST(DbAddressMeta_KeyToString);
    CPPUNIT_TEST_SUITE_END();

    void MemAddressMeta_KeyLess_1();
    void MemAddressMeta_KeyLess_2();
    void MemAddressMeta_KeyEqual_1();
    void MemAddressMeta_KeyEqual_2();
    void MemAddressMeta_KeyEqual_3();
    void MemAddressMeta_KeyEqual_4();
    void MemAddressMeta_KeyToString();
    void DbAddressMeta_KeyEqual_1();
    void DbAddressMeta_KeyEqual_2();
    void DbAddressMeta_KeyToString();
};

void AddressMetaUnitTest::MemAddressMeta_KeyLess_1()
{
    DataStore::DataStoreKey keybase1(2, 2);
    MemAddressMeta::Key key1(keybase1);
    key1._nDims = 1;

    DataStore::DataStoreKey keybase2(1, 4);
    MemAddressMeta::Key key2(keybase2);
    key2._nDims = 1;

    CPPUNIT_ASSERT(MemAddressMeta::KeyLess()(&key1, &key2) == false);
}

void AddressMetaUnitTest::MemAddressMeta_KeyLess_2()
{
    DataStore::DataStoreKey keybase1(1, 2);
    MemAddressMeta::Key key1(keybase1);
    key1._nDims = 1;

    DataStore::DataStoreKey keybase2(1, 2);
    MemAddressMeta::Key key2(keybase2);
    key2._nDims = 2;

    CPPUNIT_ASSERT(MemAddressMeta::KeyLess()(&key1, &key2) == false);
}

void AddressMetaUnitTest::MemAddressMeta_KeyEqual_1()
{
    MemAddressMeta::Key key1({1, 2});
    MemAddressMeta::Key key2({2, 1});
    CPPUNIT_ASSERT(MemAddressMeta::KeyEqual()(&key1, &key2) == false);
}

void AddressMetaUnitTest::MemAddressMeta_KeyEqual_2()
{
    MemAddressMeta::Key key1({1, 2});
    key1._nDims = 1;

    MemAddressMeta::Key key2({1, 2});
    key2._nDims = 2;

    CPPUNIT_ASSERT(MemAddressMeta::KeyEqual()(&key1, &key2) == false);
}

void AddressMetaUnitTest::MemAddressMeta_KeyEqual_3()
{
    MemAddressMeta::Key key1({1, 2});
    key1._nDims = 1;
    key1._attId = 6;

    MemAddressMeta::Key key2({1, 2});
    key2._nDims = 1;
    key2._attId = 7;

    CPPUNIT_ASSERT(MemAddressMeta::KeyEqual()(&key1, &key2) == false);
}

void AddressMetaUnitTest::MemAddressMeta_KeyEqual_4()
{
    MemAddressMeta::Key key1({1, 2});
    key1._nDims = 1;
    key1._attId = 6;
    key1._coords[0] = 1;

    MemAddressMeta::Key key2({1, 2});
    key2._nDims = 1;
    key2._attId = 6;
    key2._coords[0] = 2;

    CPPUNIT_ASSERT(MemAddressMeta::KeyEqual()(&key1, &key2) == false);
}

void AddressMetaUnitTest::MemAddressMeta_KeyToString()
{
    MemAddressMeta::Key key1({1, 2});
    key1._nDims = 1;
    key1._coords[0] = 4;
    key1._attId = 5;
    auto asString = MemAddressMeta::KeyToString()(&key1);
    CPPUNIT_ASSERT(asString == "{ \"keybase\": { \"nsid\": 1 , \"dsid\": 2 } , \"ndims\": 1 , \"attid\": 5 , \"coords\": [4] }");
}

void AddressMetaUnitTest::DbAddressMeta_KeyEqual_1()
{
    DbAddressMeta::Key key1({1, 2});
    key1._nDims = 1;
    key1._attId = 4;

    DbAddressMeta::Key key2({1, 2});
    key2._nDims = 1;
    key2._attId = 5;

    CPPUNIT_ASSERT(DbAddressMeta::KeyEqual()(&key1, &key2) == false);
}

void AddressMetaUnitTest::DbAddressMeta_KeyEqual_2()
{
    DbAddressMeta::Key key1({1, 2});
    key1._nDims = 1;
    key1._attId = 4;
    key1._coords[0] = 5;
    key1._arrVerId = 6;

    DbAddressMeta::Key key2({1, 2});
    key2._nDims = 1;
    key2._attId = 4;
    key2._coords[0] = 5;
    key2._arrVerId = 7;

    CPPUNIT_ASSERT(DbAddressMeta::KeyEqual()(&key1, &key2) == false);
}

void AddressMetaUnitTest::DbAddressMeta_KeyToString()
{
    MemAddressMeta::Key key1({1, 2});
    key1._nDims = 1;
    key1._coords[0] = 4;
    key1._attId = 5;
    auto asString = MemAddressMeta::KeyToString()(&key1);
    CPPUNIT_ASSERT(asString == "{ \"keybase\": { \"nsid\": 1 , \"dsid\": 2 } , \"ndims\": 1 , \"attid\": 5 , \"coords\": [4] }");
}

}  // namespace scidb

CPPUNIT_TEST_SUITE_REGISTRATION(scidb::AddressMetaUnitTest);

#endif  // ADDRESS_META_UNIT_TESTS_H_
