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

/*
 * DBArray.h
 *
 *  Created on: 17.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Database array implementation
 */

#ifndef DBARRAY_H_
#define DBARRAY_H_

#include <vector>

#include <array/MemArray.h>
#include <array/NewDBArray.h>

namespace scidb
{
/**
 * Implementation of database array.
 */
class DBArray : public Array, public std::enable_shared_from_this<DBArray>
{
public:
    virtual std::string const& getName() const;
    virtual ArrayID getHandle() const;

    virtual ArrayDesc const& getArrayDesc() const;

    virtual std::shared_ptr<ArrayIterator> getIterator(AttributeID attId);
    virtual std::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const;

    /**
     * Returns a flag indicating that this array has an available list of chunk positions
     * @return true unless we don't have a query context
     */
    virtual bool hasChunkPositions() const
    {
        return true;
    }

    /**
     * Build and return a list of the chunk positions.
     * @return the new sorted set of coordinates, containing the first
     *           coordinate of every chunk present in the array
     */
    virtual std::shared_ptr<CoordinateSet> getChunkPositions() const;

    /**
     * @see Array::isMaterialized()
     */
    virtual bool isMaterialized() const
    {
        return true;
    }

    static std::shared_ptr<Array> createDBArray(
        ArrayDesc const& desc,
        const std::shared_ptr<Query>& query)
    {
        static const int createNewDBArray =
            Config::getInstance()->getOption<int>(CONFIG_NEW_DBARRAY);
        
        if (createNewDBArray)
        {
            return NewDBArray::createDBArray(desc, query);
        }
        else
        {
            return std::shared_ptr<DBArray>(new DBArray(desc, query));
        }
    }

    /**
     * Given the coordinate set liveChunks - remove the chunk version for
     * every chunk that is in the array and NOT in liveChunks. This is used
     * by overriding-storing ops to ensure that new versions of arrays do not
     * contain chunks from older versions unless explicitly added.
     * @param query  shared pointer to query context
     * @param liveChunks the set of chunks that should NOT be tombstoned
     */
    void removeDeadChunks(std::shared_ptr<Query>& query,
                          std::set<Coordinates, CoordinatesLess> const& liveChunks);

    /**
     * Flush all changes to the physical device(s) for the array, if required.
     */
    void flush();

    /**
     * Remove all versions prior to lastLiveArrId from the array. If
     * lastLiveArrId is 0, removes all versions.
     * @param query  shared pointer to query context
     * @param lastLiveArrId the Versioned Array ID of last version to preserve
     */
    void removeVersions(std::shared_ptr<Query>& query,
                        ArrayID lastLiveArrId);

private:
    DBArray(ArrayDesc const& desc, const std::shared_ptr<Query>& query);
    DBArray();
    DBArray(const DBArray& other);
    DBArray& operator=(const DBArray& other);

private:
    ArrayDesc _desc;
};

} // namespace
#endif
