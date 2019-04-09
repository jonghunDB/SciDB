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
 * @file ConstArrayIterator.cpp
 *
 * @brief class ConstArrayIterator
 */
#include <log4cxx/logger.h>

#include <array/ConstArrayIterator.h>
#include <system/Exceptions.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.ConstArrayIterator"));

    bool ConstArrayIterator::setPosition(Coordinates const& pos)
    {
        ASSERT_EXCEPTION(false,"ConstArrayIterator::setPosition");
        return false;
    }

    void ConstArrayIterator::restart()
    {
        ASSERT_EXCEPTION(false,"ConstArrayIterator::restart");
    }

}
