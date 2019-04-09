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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Defines source-wide constants out of line. The use of '-D  <symbol>,
 *        preprocessor macros, declarations of compile time constants within
 *        header files and the like, all tend to cause the make system to re-
 *        compile the entire system whenever any of these definitions change,
 *        so we pefer instead to define these constants out of line and refer
 *        to them through the functional interface defined in "Constant.h".
 */

#include <sstream>
#include <system/Constants.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

const char* SCIDB_VERSION()         {return "18.1.14.be15f8e";}
const char* SCIDB_VERSION_PUBLIC()  {return "18.1.14";}
const char* SCIDB_BUILD_TYPE()      {return "Debug";}
const char* SCIDB_COMMIT()          {return "be15f8e";}
const char* SCIDB_COPYRIGHT()       {return "Copyright (C) 2008-2017 SciDB, Inc.";}
const char* SCIDB_INSTALL_PREFIX()  {return "/opt/scidb/18.1";}

uint32_t    SCIDB_VERSION_MAJOR()   {return 18;}
uint32_t    SCIDB_VERSION_MINOR()   {return 1;}
uint32_t    SCIDB_VERSION_PATCH()   {return 14;}
uint32_t    SCIDB_VERSION_BUILD()   {return 0xbe15f8e;}

std::string DEFAULT_MPI_DIR()       {return "/usr/lib/mpich2scidb";}
std::string DEFAULT_MPI_TYPE()      {return "mpich2scidb";}

std::string SCIDB_BUILD_INFO_STRING(const char* separator)
{
    std::ostringstream o;

    o << "SciDB Version: " << SCIDB_VERSION_PUBLIC() << separator
      << "Build Type: "    << SCIDB_BUILD_TYPE()     << separator
      << "Commit: "        << SCIDB_COMMIT()         << separator
      << SCIDB_COPYRIGHT();

    return o.str();
}

/****************************************************************************/
}
/****************************************************************************/
