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

#ifndef ADDRESS_UNIT_TESTS_H_
#define ADDRESS_UNIT_TESTS_H_

#include <array/Address.h>

namespace scidb {

struct AddressUnitTest : CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(AddressUnitTest);
    CPPUNIT_TEST(copyConstruction);
    CPPUNIT_TEST(toString);
    CPPUNIT_TEST_SUITE_END();

    void copyConstruction();
    void toString();
};

void AddressUnitTest::copyConstruction()
{
    Address addr({1}, {2});
    CPPUNIT_ASSERT(addr.attId == 1);
    CPPUNIT_ASSERT(addr.coords.size() == 1);
    CPPUNIT_ASSERT(addr.coords[0] == 2);

    auto addrCopy = addr;
    CPPUNIT_ASSERT(addrCopy.attId == addr.attId);
    CPPUNIT_ASSERT(addrCopy.coords == addr.coords);
}

void AddressUnitTest::toString()
{
    Address addr({1}, {2});
    auto asString = addr.toString();
    CPPUNIT_ASSERT(asString == "{ attid = 1, coords = {2}}");
}

struct PersistentAddressUnitTest : CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PersistentAddressUnitTest);
    CPPUNIT_TEST(toString);
    CPPUNIT_TEST_SUITE_END();

    void toString();
};

void PersistentAddressUnitTest::toString()
{
    PersistentAddress addr({1}, {2}, {3});
    auto asString = addr.toString();
    CPPUNIT_ASSERT(asString == "{ attid = 2, coords = {3}, vaid = 1}");
}

}  // namespace scidb

CPPUNIT_TEST_SUITE_REGISTRATION(scidb::AddressUnitTest);
CPPUNIT_TEST_SUITE_REGISTRATION(scidb::PersistentAddressUnitTest);

#endif  // ADDRESS_UNIT_TESTS_H_
