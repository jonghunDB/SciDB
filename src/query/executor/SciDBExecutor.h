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
 * @file SciDBExecutor.h
 * @author roman.simakov@gmail.com
 *
 * @brief Provide access to SciDB Executor instance
 */

#ifndef SCIDBEXECUTOR_H_
#define SCIDBEXECUTOR_H_

namespace scidb
{
class SciDB;
/**
 *  @return a reference to scidb executor instance.
 */
scidb::SciDB& getSciDBExecutor();

}

#endif /* SCIDBEXECUTOR_H_ */
