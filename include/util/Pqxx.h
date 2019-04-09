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
 * @file Pqxx.h
 * @brief Wrap libpqxx header files, and provide version-specific tailoring.
 *
 * @description
 * The C++ Postgres client library libpqxx version 4.0 has eliminated
 * the necessity to declare types when preparing an SQL query.  See
 * this message on the libpqxx-general mailing list and its replies:
 *
 *  http://lists.pgfoundry.org/pipermail/libpqxx-general/2012-July/000680.html
 *
 * In the near term we must build with different versions of the
 * library, so this header file encapsulates the API differences.  All
 * code that needs to access Postgres should use it.  See
 * src/system/catalog/SystemCatalog.cpp for an example of its use.
 */

#ifndef UTIL_PQXX_H
#define UTIL_PQXX_H

#include <pqxx/connection>
#include <pqxx/transaction>
#include <pqxx/prepared_statement>
#include <pqxx/except>
#include <pqxx/binarystring>
#include <pqxx/version>
#include <libpq-fe.h>

//
// Note from Donghui Zhang (2016-6-7):
// With version 4 and above of PQXX, the macro PQXX_DECL is a no-op.
// Longer term, we should remove all occurrences of them (200+) in our code base.
// Keeping them temporarily, however, may help users who really need to build with pqxx v3.
//

static_assert(PQXX_VERSION_MAJOR>=4, "Building SciDB requires PQXX to have a minimum of version 4.");

#if PQXX_VERSION_MAJOR < 4
#  define NEED_PQXX_DECLS 1
#  define PQXX_DECL(_type, _treatment)        (_type, _treatment)
#  define PQXX_DECL_USE(_decl_use)        { _decl_use ; }
#else
   /* pqxx::declaration and other types went away in 4.0 */
#  define PQXX_DECL(_type, _treatment)
#  define PQXX_DECL_USE(_decl_use)        { ; }
#  if PQXX_VERSION_MAJOR >= 5
      /* No more PGSTD (supported older compilers). */
#     define PGSTD std
#  endif
#endif

/**
 * @return true iff the libpqxx library requires explicit parameter declarations for prepared statements
 * @note Version 4 of libpqxx stopped requiring explicit decls.  Any usage of explicit decls should
 *       be wrapped in PQXX_DECL_USE().
 */
inline bool needPreparedParamDecls()
{
#if defined(NEED_PQXX_DECLS)
    return true;
#else
    return false;
#endif
}

#endif  // ! UTIL_PQXX_H
