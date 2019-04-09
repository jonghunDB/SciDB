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
 * @file Array.h
 *
 * @brief the Array class
 *
 * Note: Array.h was split into
 *   8 main classes:
 *     ConstIterator.h ConstArrayIterator.h ArrayIterator.h ConstItemIterator.h
 *     ConstChunk.h Chunk.h
 *     ConstChunkIterator.h ChunkIterator.h
 *   and 5 ancillary classes:
 *     CompressedBuffer.h MemoryBuffer.h SharedBuffer.h
 *     PinBuffer.h UnPinner.h
 * Use "git blame -C" to show the code motion of the splitup.
 */

#ifndef ARRAY_H_
#define ARRAY_H_

#include <set>                          // CoordinateSet

#include <array/Metadata.h>             // ArrayID typedef, e.g.

#include <array/ConstChunkIterator.h>   // (grr) only due to use of a un unfactored default for getItemIterator()

namespace scidb
{
class ArrayIterator;
class ConstArrayIterator;
class ConstItemIterator;
class Query;


typedef std::set<Coordinates, CoordinatesLess> CoordinateSet;

/**
 * The array interface provides metadata about the array, including its handle, type and
 * array descriptors.
 * To access the data in the array, a constant (read-only) iterator can be requested, or a
 * volatile iterator can be used.
 */
class Array
{
public:

    /**
     * An enum that defines three levels of Array read access policy, ranging from most to least restrictive.
     */
    enum Access
    {
        /**
         * Most restrictive access policy wherein the array can only be iterated over one time.
         * If you need to read multiple attributes, you need to read all of the attributes horizontally, at the same time.
         * Imagine that reading the array is like scanning from a pipe - after a single scan, the data is no longer available.
         * This is the only supported access mode for InputArray and MergeSortArray.
         * Any SINGLE_PASS array must inherit from scidb::SinglePassArray if that array is ever returned from PhysicalOperator::execute().
         * The reason is that the sg() operator can handle only the SINGLE_PASS arrays conforming to the SinglePassArray interface.
         */
        SINGLE_PASS = 0,

        /**
         * A policy wherein the array can be iterated over several times, and various attributes can be scanned independently,
         * but the ArrayIterator::setPosition() function is not supported.
         * This is less restrictive than SINGLE_PASS.
         * This is the least restrictive access mode supported by ConcatArray.
         */
        MULTI_PASS  = 1,

        /**
         * A policy wherein the client of the array can use the full functionality of the API.
         * This is the least restrictive access policy and it's supported by the vast majority of Array subtypes.
         */
        RANDOM      = 2
    };

    virtual ~Array() {}

    /**
     * Get array name
     */
    virtual std::string const& getName() const;

    /**
     * Get array identifier
     */
    virtual ArrayID getHandle() const;

    /**
     * Determine if this array has an easily accessible list of chunk positions. In fact, a set of chunk positions can
     * be generated from ANY array simply by iterating over all of the chunks once. However, this function will return true
     * if retrieving the chunk positions is a separate routine that is more efficient than iterating over all chunks.
     * All materialized arrays can and should implement this function.
     * @return true if this array supports calling getChunkPositions(). false otherwise.
     */
    virtual bool hasChunkPositions() const
    {
        return false;
    }

    /**
     * Build and return a list of the chunk positions. Only callable if hasChunkPositions() returns true, throws otherwise.
     * @return the sorted set of coordinates, containing the first coordinate of every chunk present in the array
     */
    virtual std::shared_ptr<CoordinateSet> getChunkPositions() const;

    /**
     * If hasChunkPositions() is true, return getChunkPositions(); otherwise build a list of chunk positions manually
     * by iterating over the chunks of one of the array attributes. The attribute to iterate over is chosen according to a heuristic,
     * using empty_tag if available, otherwise picking the smallest fixed-size attribute. The array getSupportedAccess() must be
     * at least MULTI_PASS.
     * @return the sorted set of coordinates, containing the first coordinate of every chunk present in the array
     */
    virtual std::shared_ptr<CoordinateSet> findChunkPositions() const;

    /**
     * Determine if the array is materialized; which means all chunks are populated either memory or on disk, and available on request.
     * This returns false by default as that is the case with all arrays. It returns true for MemArray, etc.
     * @return true if this is materialized; false otherwise
     */
    virtual bool isMaterialized() const
    {
        return false;
    }

    /**
     * Get the least restrictive access mode that the array supports. The default for the abstract superclass is RANDOM
     * as a matter of convenience, since the vast majority of our arrays support it. Subclasses that have access
     * restrictions are responsible for overriding this appropriately.
     * @return the least restrictive access mode
     */
    virtual Access getSupportedAccess() const
    {
        return RANDOM;
    }

    /**
     * Extract subarray between specified coordinates into the buffer.
     * @param attrID extracted attribute of the array (should be fixed size)
     * @param buf buffer preallocated by caller which should be preallocated by called and be large enough
     *        to fit all data.
     * @param first minimal coordinates of extract box
     * @param last maximal coordinates of extract box
     * @param init EXTRACT_INIT_ZERO or EXTRACT_INIT_NAN
     *             if buf is floating-point, EXTRACT_INIT_NAN writes a NaN in
     *             buf for each cell that was empty; otherwise a zero.
     *             the second option is only meaningful for extracting to arrays of float or double
     * @param null EXTRACT_NULL_AS_EXCEPTION or EXTRACT_NULL_AS_NAN
     *             if buf is floating-point, EXTRACT_NULL_AS_NAN writes a NaN in
     *             buf for each null; otherwise a null is an exception.
     *             if a floating-point array, whether it should be extracted as a NaN
     * @return number of extracted chunks
     */
    enum extractInit_t { EXTRACT_INIT_ZERO=0, EXTRACT_INIT_NAN };
    enum extractNull_t { EXTRACT_NULL_AS_EXCEPTION=0, EXTRACT_NULL_AS_NAN };
    virtual size_t extractData(AttributeID attrID, void* buf, Coordinates const& first, Coordinates const& last,
                               extractInit_t init=EXTRACT_INIT_ZERO,
                               extractNull_t null=EXTRACT_NULL_AS_EXCEPTION) const;

    /**
     * Append data from the array, all attribute-chunks
     * before advancing the chunk position ("horizontal")
     * @param[in] input source array
     * @param[out] newChunkCoordinates if set - the method shall insert the coordinates of all appended chunks into the set pointed to.
     */
    virtual void appendHorizontal(const std::shared_ptr<Array>& input, CoordinateSet* newChunkCoordinates = NULL);

    /**
     * Append data from the array, all chunks positions of one attribute
     * before advancing to the next attribute ("vertical") [Deprecated, use appendHorizontal]
     * @param[in] input source array
     * @param[out] newChunkCoordinates if set - the method shall insert the coordinates of all appended chunks into the set pointed to.
     */
    virtual void appendVertical(const std::shared_ptr<Array>& input,  CoordinateSet* newChunkCoordinates = NULL);

    /**
     * Get array descriptor
     */
    virtual ArrayDesc const& getArrayDesc() const = 0;

    /**
     * Get read-write iterator
     * @param attr attribute ID
     * @return iterator through chunks of spcified attribute
     */
    virtual std::shared_ptr<ArrayIterator> getIterator(AttributeID attr);

    /**
     * Get read-only iterator
     * @param attr attribute ID
     * @return read-only iterator through chunks of spcified attribute
     */
    virtual std::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const = 0;

    ConstArrayIterator* getConstIteratorPtr(AttributeID attr) const
    {
        return getConstIterator(attr).operator->();
    }

    /**
     * Get read-only iterator thtough all array elements
     * @param attr attribute ID
     * @param iterationMode chunk iteration mode
     */
    virtual std::shared_ptr<ConstItemIterator> getItemIterator(AttributeID attr, int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS|ConstChunkIterator::IGNORE_EMPTY_CELLS) const;

    /**
     * Scan entire array and print contents to logger
     * DEBUG build only.  Otherwise a nullop
     */
    void printArrayToLogger() const;

    void setQuery(std::shared_ptr<Query> const& query) {_query = query;}

    /**
     * If count() can return its result in O(1) [or O(nExistingChunks) time if from a
     * fast in-memory index such as a chunkMap], then
     * this method should return true to indicate it is "reasonably" fast
     * Otherwse, this should return false, for example, when chunks themselves would return false
     * from their isCountKnown() method.  If there are gray areas in between these cases
     * O(1) or O(nExistingChunks) vs O(nNonNullCells), then the API of isCountKnown()
     * will either need definition refinement and/or API revision
     * @return true if count() is sufficiently fast by the above criteria
     */
    virtual bool isCountKnown() const;

    /**
     * While we would like all arrays to do this in O(1) time or O(nChunks) time,
     * some cases still require traversal of all the chunks of one attribute of the array.
     * If that expense is too much, then isCountKnown() should return false, and you
     * should avoid calling count().
     * @return the count of all non-empty cells in the array
     */
    virtual size_t count() const; // logically const, even if an result is cached.


    /**
     * Given the coordinate set liveChunks - remove the chunk version for
     * every chunk that is in the array and NOT in liveChunks. This is used
     * by overriding-storing ops to ensure that new versions of arrays do not
     * contain chunks from older versions unless explicitly added.
     * @param query  shared pointer to query context
     * @param liveChunks the set of chunks that should NOT be tombstoned
     */
    virtual void removeDeadChunks(
        std::shared_ptr<Query>& query,
        std::set<Coordinates, CoordinatesLess> const& liveChunks)
        {}

    /**
     * Insert a tombstone locally for a single position for all attributes
     * @param query  shared pointer to query context
     * @param coords  position at which to remove
     */
    virtual void removeLocalChunk(
        std::shared_ptr<Query> const& query,
        Coordinates const& coords)
        {}

    /**
     * Flush all changes to the physical device(s) for the array, if required.
     */
    virtual void flush()
        {}

    /**
     * Remove all versions prior to lastLiveArrId from the array. If
     * lastLiveArrId is 0, removes all versions.
     * @param query  shared pointer to query context
     * @param lastLiveArrId the Versioned Array ID of last version to preserve
     */
    virtual void removeVersions(
        std::shared_ptr<Query>& query,
        ArrayID lastLiveArrId)
        {}

protected:
    /// The query context for this array
    std::weak_ptr<Query> _query;                // TODO: make private by providing a setter method
                                                //       (currently directly set up to three levels
                                                //       of deriviation away, e.g. in PullSGArray)

private:
    /**
     * private to force the use of append{Vertical,Horizontal} so that we can find any remaining
     * uses of appendVertical in the code, which are problematic for pullSG
     * @param[in] input source array
     * @param[in] vertical
     *            If false - append the first chunk for all attributes, then the second chunk...
     * @param[out] newChunkCoordinates if set - the method shall insert the coordinates of all appended chunks into the set pointed to.
     */
    virtual void append(const std::shared_ptr<Array>& input, bool vertical, CoordinateSet* newChunkCoordinates = NULL);
};

} // namespace
#endif /* ARRAY_H_ */
