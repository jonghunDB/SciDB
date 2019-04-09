#ifndef ___SYNCHABLEARRAY_H___
#define ___SYNCHABLEARRAY_H___
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

#ifndef SCIDB_CLIENT

#include <array/StreamArray.h>

namespace scidb {

/// An interface for an Array which requires a (implementation-dependent) synchronization point
class SynchableArray : virtual public Array
{
public:
    virtual void sync() = 0;
    virtual ~SynchableArray() {}
protected:
    SynchableArray() {}
private:
    SynchableArray(const SynchableArray&);
    SynchableArray& operator=(const SynchableArray&);
};
}  // scidb
#endif //SCIDB_CLIENT

#endif
