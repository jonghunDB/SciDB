#ifndef COMPRESSORTYPE_H_GUARD
#define COMPRESSORTYPE_H_GUARD
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
#include <cstdint>
#include <limits>

namespace scidb {
enum CompressorType : uint16_t {
    // The numerical values of the enums are important, and must remain.
    // Unfortunately, the following code locations make use of this numbering:
    //  - AttributeDesc::toString
    //  - ArrayWriter.cpp -- saveOpaque(). The OpaqueChunkHeader.compressionMethod is
    //    written as an int8_t.
    //  - Other places used this as a uint16_t, int16_t, uint32_t and int32_t such as in
    //    the protobufs Messaging.
    //
    // Only NONE, BZLIB_COMPRESSOR, and ZLIB_COMPRESSOR are supported according to the
    // SciDB Reference Guide, and the others are obsolete (see SDB-3223).
    //
    NONE = 0,
    // obsolete NULL_FILTER = 1,
    // obsolete RUN_LENGTH_ENCODING = 2,
    // obsolete BITMAP_ENCODING = 3,
    // obsolete NULL_SUPPRESSION = 4,
    // obsolete DICTIONARY_ENCODING = 5,
    ZLIB = 6,
    BZLIB = 7,
    // obsolete USER_DEFINED_COMPRESSOR = 8,
    //
    // The "extensible" Compressor plugin idea would never have worked in the original
    // implementation. The valid compressors are defined inside a vector, which has a size
    // of MAX_DEFINED to accommodate the vector size in earlier implementation of the
    // "CompressorFactory".
    MAX_DEFINED = 9,

    UNKNOWN = std::numeric_limits<uint16_t>::max()  //!< Special marker value used to indicate unset Value.
};

}  // scidb
#endif
