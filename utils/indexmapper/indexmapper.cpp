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
 * @file indexmapper.cpp
 *
 * @author Steve Fridella <sfridella@paradigm4.com>
 *
 * @brief Utility to scan chunk map
 *
 * @note Uses DbAddressMeta but MemAddressMeta could be made to work
 *       as well (that's how Steve originally had it).
 */

#include <getopt.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <string.h>

#include <rocksdb/db.h>

#define NO_NOTIFICATION_LINKAGE 1

#include <array/AddressMeta.h>
#include <storage/BufferMgr.h>
#include <storage/PersistentIndexMap.h>

using namespace std;
using namespace scidb;

bool opt_bufs = false;         // No buffer headers, just keys
bool opt_verbose = false;

namespace scidb {

// These are not necessarily the same as operator<<() in BufferMgr.cpp
// Since this is a special purpose dumping propgram, it is totally appropriate
// to have its own customizable definitions of such methods, and as such
// probably not appropriate to have them as overloaded operators

std::ostream& toStream(std::ostream& os, BufferMgr::BufferKey const& bk)
{
    os << "{\"dsk\":" << bk.getDsk().toString()
       << ", \"off\":" << bk.getOffset()
       << ", \"csz\":" << bk.getCompressedSize()
       << ", \"sz\":" << bk.getSize()
       << ", \"asz\":" << bk.getAllocSize()
       << '}';
    return os;
}

std::ostream& toStream(std::ostream& os, BufferMgr::BufferHandle const& bh)
{
    if (bh.isNull()) {
        os << "{null";
    } else {
        os << "{\"off\":" << bh.offset()
           << ", \"csz\":" << bh.compressedSize()
           << ", \"sz\":" << bh.size()
           << ", \"asz\":" << bh.allocSize();
    }
    os << ", \"slot\":" << bh.getSlot()
       << ", \"gen\":" << bh.getGenCount()
       << ", \"key\":" ;
    toStream(os, bh.getKey());
    os << ", \"cType\":" << static_cast<uint16_t>(bh.getCompressorType())
       << '}';
    return os;
}

} // end scidb

int process(string const& dbPath)
{
    string const indent(2, ' ');

    scidb::RocksIndexMap<DbAddressMeta>::KeyComp kc;
    rocksdb::Options dbOptions;
    rocksdb::DB* dbConn;
    dbOptions.comparator = &kc;
    rocksdb::Status dbStatus = rocksdb::DB::Open(dbOptions, dbPath, &dbConn);
    if (!dbStatus.ok()) {
        cerr << "Cannot open " << dbPath << ": " << dbStatus.ToString() << endl;
        return 1;
    }

    /* Iterate the entire db and dump the contents one key at a time.
     */
    DbAddressMeta am;
    int count = 0;
    rocksdb::Iterator* it = dbConn->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        DbAddressMeta::Key const* key =
            reinterpret_cast<DbAddressMeta::Key const*>(it->key().data());
        cout << "key " << count << ": " << am.keyToString(key) << endl;
        if (opt_bufs) {
            if (key->_nDims == 0) {
                cout << indent << "{Sentinel}" << endl;
            } else {
                // We brashly assume that a BufferHandle appears first
                // in a PersistentIndexMap::Entry.
                BufferMgr::BufferHandle const* bh =
                    reinterpret_cast<BufferMgr::BufferHandle const*>(it->value().data());
                toStream(cout << indent << "hdl=", *bh) ;
                cout << endl;
            }
        }
        count++;
    }
    // Check for any errors found during the scan
    if (!it->status().ok()) {
        cerr << "Bad post-scan status: " << it->status().ToString() << endl;
    }
    delete it;

    return 0;
}


void usage()
{
    cerr <<
        "usage: indexmapper [ -b ] [ -h ] DIR\n"
        "Options:\n"
        "  -b/--buffers  Print buffer handles\n"
        "  -h/--help     Print this text\n\n"
        "Arguments:\n"
        "  DIR          RocksDB metadata directory"
         << endl;
}


// Don't forget to change both short_ and long_options!
const char* short_options = "bhv";
struct option long_options[] = {
    { "buffers",      no_argument,       0, 'b' },
    { "help",         no_argument,       0, 'h' },
    { "verbose",      no_argument,       0, 'v' },
    {0, 0, 0, 0}
};

int main(int argc, char* argv[])
{
    char c;
    int optidx = 0;
    while (1) {
        c = static_cast<char>(
            ::getopt_long(argc, argv, short_options, long_options, &optidx));
        if (c == -1)
            break;
        switch (c) {
        case 'b':
            opt_bufs = true;
            break;
        case 'h':
            usage();
            return 0;
        case 'v':
            opt_verbose = true;
            break;
        default:
            usage();
            return 2;
        }
    }

    if (optind != (argc - 1)) {
        usage();
        return 2;
    }

    return process(argv[optind]);
}
