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

/****************************************************************************/
#include "ListArrayBuilders.h"

#include <array/ChunkAuxMeta.h>
#include <rbac/NamespaceDesc.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Session.h>

#include <boost/assign/list_of.hpp>                      // For list_of()
#include <iostream>


using namespace std;
using NsComm = scidb::namespaces::Communicator;

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace boost::assign;                           // For list_of()

/****************************************************************************/

Attributes ListChunkMapArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(STORAGE_VERSION,  "svrsn",TID_UINT32,0,CompressorType::NONE))
    (AttributeDesc(INSTANCE_ID,      "instn",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(DATASTORE_DSID,   "dsid", TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(DISK_OFFSET,      "doffs",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(U_ARRAY_ID,       "uaid", TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(V_ARRAY_ID,       "arrid",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(ATTRIBUTE_ID,     "attid",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(COORDINATES,      "coord",TID_STRING,0,CompressorType::NONE))
    (AttributeDesc(COMPRESSION,      "comp", TID_INT8,  0,CompressorType::NONE))
    (AttributeDesc(FLAGS,            "flags",TID_UINT8, 0,CompressorType::NONE))
    (AttributeDesc(NUM_ELEMENTS,     "nelem",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(COMPRESSED_SIZE,  "csize",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(UNCOMPRESSED_SIZE,"usize",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(ALLOCATED_SIZE,   "asize",TID_UINT64,0,CompressorType::NONE))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

/* For back-compatibility with the old flags enum
 */
const uint32_t CHUNK_HEADER_TOMBSTONE_FLAG = 8;

void ListChunkMapArrayBuilder::list(typename DbAddressMeta::Key const* key,
                                    BufferMgr::BufferHandle& buf,
                                    PointerRange<const char> auxMeta)
{
    ChunkAuxMeta const* chunkAuxMeta =
        reinterpret_cast<ChunkAuxMeta const*>(auxMeta.begin());
    Coordinates coords;
    std::ostringstream s;

    for (uint i = 0; i < key->_nDims; ++i)
    {
        coords.push_back(key->_coords[i]);
    }

    beginElement();
    write(STORAGE_VERSION,  chunkAuxMeta->getStorageVersion());
    write(INSTANCE_ID,      chunkAuxMeta->getInstanceId());
    write(DATASTORE_DSID,   key->dsk().getDsid());
    write(DISK_OFFSET,      buf.offset());
    write(U_ARRAY_ID,       key->dsk().getDsid());
    write(V_ARRAY_ID,       key->_arrVerId);
    write(ATTRIBUTE_ID,     key->_attId);
    s << coords;
    write(COORDINATES,      s.str());
    write(COMPRESSION,      static_cast<uint8_t>(buf.getCompressorType()));
    write(FLAGS,
          buf.isNull() ?
          uint8_t(CHUNK_HEADER_TOMBSTONE_FLAG) :
          uint8_t(0));
    write(NUM_ELEMENTS,     chunkAuxMeta->getNElements());
    write(COMPRESSED_SIZE,  buf.compressedSize());
    write(UNCOMPRESSED_SIZE, buf.size());
    write(ALLOCATED_SIZE,   buf.allocSize());
    endElement();
}

Attributes ListBufferStatsArrayBuilder::getAttributes() const
{
    return list_of
        (AttributeDesc(CACHED_BYTES, "cache", TID_UINT64, 0, CompressorType::NONE))
        (AttributeDesc(DIRTY_BYTES, "dirty", TID_UINT64, 0, CompressorType::NONE))
        (AttributeDesc(PINNED_BYTES, "pinned", TID_UINT64, 0, CompressorType::NONE))
        (AttributeDesc(PENDING_BYTES, "pending", TID_UINT64, 0, CompressorType::NONE))
        (AttributeDesc(RESERVE_REQ_BYTES, "reserve", TID_UINT64, 0, CompressorType::NONE))
        (AttributeDesc(USED_MEM_LIMIT, "limit", TID_UINT64, 0, CompressorType::NONE))
        (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListBufferStatsArrayBuilder::list()
{
    BufferMgr::BufferStats bufStats;
    BufferMgr::getInstance()->getByteCounts(bufStats);

    beginElement();
    write(CACHED_BYTES, bufStats.cached);
    write(DIRTY_BYTES,  bufStats.dirty);
    write(PINNED_BYTES, bufStats.pinned);
    write(PENDING_BYTES, bufStats.pending);
    write(RESERVE_REQ_BYTES, bufStats.reserveReq);
    write(USED_MEM_LIMIT, bufStats.usedLimit);
    endElement();
}

/****************************************************************************/

Attributes ListDiskIndexArrayBuilder::getAttributes() const
{
   return list_of
       (AttributeDesc(DATASTORE_DSID,  "dsid",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(DATASTORE_NSID,  "nsid",TID_UINT32,0,CompressorType::NONE))
       (AttributeDesc(ARRAY_ID,        "arrid",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(VERSION_ID,      "verid",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(ATTRIBUTE_ID,    "attid",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(COORDINATES,     "coord",TID_STRING,0,CompressorType::NONE))
       (AttributeDesc(BUFFER_OFF,      "boffs",TID_INT64,0,CompressorType::NONE))
       (AttributeDesc(BUFFER_SIZE,     "bsize",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(BUF_COMPRESS_SIZE, "csize",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(ALLOCATED_SIZE,  "asize",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(NUM_ELEMENTS,    "nelem",TID_UINT64,0,CompressorType::NONE))
       (AttributeDesc(STORAGE_VERSION, "svrsn",TID_UINT32,0,CompressorType::NONE))
       (AttributeDesc(INSTANCE_ID,     "iid",TID_UINT64,0,CompressorType::NONE))
       (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListDiskIndexArrayBuilder::list(typename DbAddressMeta::Key const* key,
                                     BufferMgr::BufferHandle& bufHandle,
                                     PointerRange<const char> auxMeta)
{
    ChunkAuxMeta const* chunkAuxMeta =
        reinterpret_cast<ChunkAuxMeta const*>(auxMeta.begin());
    Coordinates coords;
    std::ostringstream s;

    for (uint i = 0; i < key->_nDims; ++i)
    {
        coords.push_back(key->_coords[i]);
    }

    beginElement();

    write(DATASTORE_DSID,   key->dsk().getDsid());
    write(DATASTORE_NSID,   key->dsk().getNsid());
    write(ARRAY_ID,         key->dsk().getDsid());
    write(VERSION_ID,       key->_arrVerId);
    write(ATTRIBUTE_ID,     key->_attId);
    s << coords;
    write(COORDINATES,      s.str());
    write(BUFFER_OFF,       bufHandle.offset());
    write(BUFFER_SIZE,      bufHandle.size());
    write(BUF_COMPRESS_SIZE, bufHandle.compressedSize());
    write(ALLOCATED_SIZE,   bufHandle.allocSize());
    write(NUM_ELEMENTS,     chunkAuxMeta->getNElements());
    write(STORAGE_VERSION,  chunkAuxMeta->getStorageVersion());
    write(INSTANCE_ID,      chunkAuxMeta->getInstanceId());
    endElement();
}

/****************************************************************************/

Attributes ListLibrariesArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(PLUGIN_NAME,"name",      TID_STRING,0,CompressorType::NONE))
    (AttributeDesc(MAJOR,      "major",     TID_UINT32,0,CompressorType::NONE))
    (AttributeDesc(MINOR,      "minor",     TID_UINT32,0,CompressorType::NONE))
    (AttributeDesc(PATCH,      "patch",     TID_UINT32,0,CompressorType::NONE))
    (AttributeDesc(BUILD,      "build",     TID_UINT32,0,CompressorType::NONE))
    (AttributeDesc(BUILD_TYPE, "build_type",TID_STRING,AttributeDesc::IS_NULLABLE, CompressorType::NONE))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListLibrariesArrayBuilder::list(const PluginManager::Plugin& e)
{
    beginElement();
    write(PLUGIN_NAME,e._name);
    write(MAJOR,      e._major);
    write(MINOR,      e._minor);
    write(BUILD,      e._build);
    write(PATCH,      e._patch);
    if (e._buildType.empty())
        write(BUILD_TYPE,Value());
    else
        write(BUILD_TYPE,e._buildType);
    endElement();
}

/****************************************************************************/

ListAggregatesArrayBuilder::ListAggregatesArrayBuilder(size_t cellCount, bool showSys)
 : ListArrayBuilder("aggregates")
 , _cellCount(cellCount)
 , _showSys(showSys)
{}

Dimensions ListAggregatesArrayBuilder::getDimensions(std::shared_ptr<Query> const& query) const
{
    size_t cellCount = std::max(_cellCount,1UL);

    return Dimensions(1,DimensionDesc("No",0,0,cellCount-1,cellCount-1,cellCount,0));
}

Attributes ListAggregatesArrayBuilder::getAttributes() const
{
    Attributes attrs = list_of
        (AttributeDesc(0, "name",   TID_STRING,0,CompressorType::NONE))
        (AttributeDesc(1, "typeid", TID_STRING,0,CompressorType::NONE))
        (AttributeDesc(2, "library",TID_STRING,0,CompressorType::NONE));
    if (_showSys) {
        attrs.push_back(AttributeDesc(3, "internal", TID_BOOL,0,CompressorType::NONE));
        attrs.push_back(emptyBitmapAttribute(4));
    } else {
        attrs.push_back(emptyBitmapAttribute(3));
    }
    return attrs;
}

void ListAggregatesArrayBuilder::list(const std::string& name,const TypeId& type,const std::string& lib)
{
    bool isSys = (name.find('_') == 0);
    if (!isSys || _showSys) {
        beginElement();
        write(0, name);
        write(1, type);
        write(2, lib);
        if (_showSys) {
            write(3, isSys);
        }
        endElement();
    }
}

/****************************************************************************/

ListDataStoresArrayBuilder::ListDataStoresArrayBuilder()
    : PERSISTENT_NSID(DataStores::getInstance()->openNamespace("persistent"))
{ }


Attributes ListDataStoresArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(DSID,           "uaid",           TID_UINT64,0,CompressorType::NONE))

    // the next two values are passed as signed values.  file_free_bytes can return -1
    (AttributeDesc(FILE_BYTES,     "file_bytes",     TID_INT64,0,CompressorType::NONE))
    (AttributeDesc(FILE_FREE,      "file_free_bytes",TID_INT64,0,CompressorType::NONE))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListDataStoresArrayBuilder::list(DataStore& item)
{
    off_t    filebytes = 0;
    off_t    filefree = 0;

    // list only the persistent datastores
    if (item.getDsk().getNsid() != PERSISTENT_NSID)
    {
        return;
    }

    item.getSizes(filebytes, filefree);

    beginElement();
    write(DSID,           item.getDsk().getDsid());
    write(FILE_BYTES,     filebytes);
    write(FILE_FREE ,     filefree); // can be -1 e.g. when onfig-punch-holes disabled
    endElement();
}

/****************************************************************************/

ListQueriesArrayBuilder::ListQueriesArrayBuilder(std::shared_ptr<Query> const& query)
    : ListArrayBuilder()
{
    _myUserId = query->getSession()->getUser().getId();

    // Find out if we have operator privilege.  If not, user can only
    // list their own queries.

    rbac::RightsMap rights;
    rights.upsert(rbac::ET_DB, "", rbac::P_DB_OPS);
    try {
        NsComm::checkAccess(query->getSession().get(), &rights);
        _hasOperatorPrivs = true;
    }
    catch (Exception&) {
        _hasOperatorPrivs = false;
    }
}

Attributes ListQueriesArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(QUERY_ID,     "query_id",     TID_STRING,  0,CompressorType::NONE))
    (AttributeDesc(COORDINATOR,  "coordinator",  TID_UINT64,  0,CompressorType::NONE))
    (AttributeDesc(QUERY_STR,    "query_string", TID_STRING,  0,CompressorType::NONE))
    (AttributeDesc(CREATION_TIME,"creation_time",TID_DATETIME,0,CompressorType::NONE))
    (AttributeDesc(ERROR_CODE,   "error_code",   TID_INT32,   0,CompressorType::NONE))
    (AttributeDesc(ERROR,        "error",        TID_STRING,  0,CompressorType::NONE))
    (AttributeDesc(IDLE,         "idle",         TID_BOOL,    0,CompressorType::NONE))
    (AttributeDesc(USER_ID,      "user_id",      TID_UINT64,  0,CompressorType::NONE))
    (AttributeDesc(USER_NAME,    "user",         TID_STRING,  0,CompressorType::NONE))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListQueriesArrayBuilder::list(std::shared_ptr<Query> const& query)
{
    std::shared_ptr<Session> session(query->getSession());

    // Skip this query if it's not ours and we don't have operator privilege.
    if (!_hasOperatorPrivs && session->getUser().getId() != _myUserId) {
        return;
    }

    const bool resolveLocalInstanceID = true;
    std::stringstream qs;
    beginElement();
    qs << query->getQueryID();

    write(QUERY_STR,    query->queryString);
    write(QUERY_ID,     qs.str());
    write(COORDINATOR,  query->getPhysicalCoordinatorID(resolveLocalInstanceID));
    write(CREATION_TIME,query->getCreationTime());

    std::shared_ptr<Exception> error(query->getError());

    write(ERROR_CODE,   error ? error->getLongErrorCode() : 0);
    write(ERROR,        error ? error->getErrorMessage() : "");
    write(IDLE,         query->idle());
    write(USER_ID,      session ? session->getUser().getId() : rbac::NOBODY);
    write(USER_NAME,    session ? session->getUser().getName() : rbac::NOBODY_USER);
    endElement();
}

/****************************************************************************/

Attributes ListCounterArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(NAME,       "name",       TID_STRING,0,CompressorType::NONE))
    (AttributeDesc(TOTAL,      "total",      TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(TOTAL_MSECS,"total_msecs",TID_UINT64,0,CompressorType::NONE))
    (AttributeDesc(AVG_MSECS,  "avg_msecs",  TID_FLOAT, 0,CompressorType::NONE))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListCounterArrayBuilder::list(const CounterState::Entry& item)
{
    float avg_msecs =
      item._num ?
      static_cast<float>(item._msecs) / static_cast<float>(item._num) :
      0;

    beginElement();
    write(NAME,       CounterState::getInstance()->getName(item._id));
    write(TOTAL,      item._num);
    write(TOTAL_MSECS,item._msecs);
    write(AVG_MSECS,  avg_msecs);
    endElement();
}

/****************************************************************************/

Dimensions ListArraysArrayBuilder::getDimensions(const std::shared_ptr<Query>& query) const
{
    return Dimensions(
        1,DimensionDesc("No",0,0,
        CoordinateBounds::getMax(),
        CoordinateBounds::getMax(),
        LIST_CHUNK_SIZE,0));
}

Attributes ListArraysArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(ARRAY_NAME,        "name",        TID_STRING,0,CompressorType::NONE))
    (AttributeDesc(ARRAY_UAID,        "uaid",        TID_INT64, 0,CompressorType::NONE))
    (AttributeDesc(ARRAY_ID,          "aid",         TID_INT64, 0,CompressorType::NONE))
    (AttributeDesc(ARRAY_SCHEMA,      "schema",      TID_STRING,0,CompressorType::NONE))
    (AttributeDesc(ARRAY_IS_AVAILABLE,"availability",TID_BOOL,  0,CompressorType::NONE))
    (AttributeDesc(ARRAY_IS_TRANSIENT,"temporary",   TID_BOOL,  0,CompressorType::NONE))
    (AttributeDesc(ARRAY_NAMESPACE,   "namespace",   TID_STRING,0,CompressorType::NONE))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListArraysArrayBuilder::list(const ArrayDesc& desc)
{
    stringstream s;
    printSchema(s,desc);

    beginElement();
    write(ARRAY_NAME,           desc.getName());
    write(ARRAY_UAID,           desc.getUAId());
    write(ARRAY_ID,             desc.getId());
    write(ARRAY_SCHEMA,         s.str());
    write(ARRAY_IS_AVAILABLE,   !desc.isDead() && desc.isComplete());
    write(ARRAY_IS_TRANSIENT,   desc.isTransient());
    write(ARRAY_NAMESPACE,      desc.getNamespaceName());
    endElement();
}

/****************************************************************************/
}
/****************************************************************************/
