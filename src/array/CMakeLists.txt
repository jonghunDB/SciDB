########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2018 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
########################################

set (array_src
    Metadata.cpp
     Array.cpp
     ConstIterator.cpp
     ConstArrayIterator.cpp
     ArrayIterator.cpp
     ConstItemIterator.cpp
     ConstChunk.cpp
     Chunk.cpp
     ConstChunkIterator.cpp
     AllocationBuffer.cpp
     CompressedBuffer.cpp
     SharedBuffer.cpp
    ArrayDistribution.cpp
    Tile.cpp
    MemArray.cpp
    MemChunk.cpp
    CachedTmpChunk.cpp 
    StreamArray.cpp
    AccumulatorArray.cpp
    MultiStreamArray.cpp
    SinglePassArray.cpp
    DelegateArray.cpp
    TupleArray.cpp
    DBArray.cpp
    ParallelAccumulatorArray.cpp
    RLE.cpp
    DeepChunkMerger.cpp
    MergeSortArray.cpp
    SortArray.cpp
    TransientCache.cpp
    SpatialRangesChunkPosIterator.cpp
    Coordinate.cpp
    ListArrayBuilder.cpp
    Dense1MChunkEstimator.cpp
    CachedDBChunk.cpp
    CachedDBChunk_debug.cpp
    NewDBArray.cpp
    NewReplicationMgr.cpp
)

file(GLOB array_include "*.h")

include_directories(${CityHash_INCLUDE_DIR})
add_library(array_lib OBJECT ${array_src} ${array_include})

add_dependencies(array_lib scidb_msg_lib)

