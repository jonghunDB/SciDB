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
 * @file RedimSettings.h
 * @brief Common keyword settings shared by repart() and redimension().
 */

#ifndef REDIM_SETTINGS_H
#define REDIM_SETTINGS_H

#include <query/Operator.h>

namespace scidb {

class RedimSettings
{
public:

    // Constant keyword names prevent typos.
    static const char* const KW_CELLS_PER_CHUNK;
    static const char* const KW_PHYS_CHUNK_SIZE;

    // Throws on bad or inconsistent keyword parameters.
    RedimSettings(std::string const& opName,
                  KeywordParameters const& kwParams,
                  bool logicalOp);

    // Return values guaranteed to be non-negative.
    int64_t getCellsPerChunk() const { return _cellsPerChunk; }
    int64_t getPhysChunkSize() const { return _physChunkSize; }

private:
    int64_t _cellsPerChunk;
    int64_t _physChunkSize;
};

} // namespace scidb

#endif // ! REDIM_SETTINGS_H
