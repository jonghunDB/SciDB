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
 * @file suite.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing pure interfaces (i.e. abstract base classes)
 */

# ifndef SUITE_H
# define SUITE_H

# include <string>
# include <vector>
# include <log4cxx/logger.h>

# include "manager.h"
# include "reporter.h"
# include "global.h"

namespace scidbtestharness
{

/**
 * collects all the suites under the given suite id,
 * collects all the tests cases under those subsuites,
 * gives those subsuites one by one to the MANAGER to run them.
 */
class Suite
{
	private :
		std::string _suiteId;
		std::vector <std::string> _subSuites;
		std::vector <std::string> _tcList;
		std::vector <std::string> _disabledtcList;
		log4cxx :: LoggerPtr _logger;

	public :
		Suite (const std::string sid) : _suiteId(sid)
		{
			_logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);
		}

		/// @brief Run a test suite.
		/// @param root_dir  the root dir of the harness test in the source code, e.g. ${SCIDBTRUNK}/tests/harness/testcases
		/// @param disabledtesetfname  the disable.tests filename, e.g. ${SCIDBSTAGE}/build/tests/harness/testcases/disable.tests
		/// @param disabled_tclist  the list of disabled test/suite names.
		/// @param regex_expr
		/// @param regex_flag
		/// @param M
		/// @param no_parallel_testcases
		/// @param[out] testcases_total  how many tests are there in total.
		/// @param[out] nDisabled  how many tests are disabled.
		/// @param[out] rptr
		/// @param[out] suitesDisabled  how many suites are disabled.
		/// @param cutoff_set  the set of long-running tests that should be cut off.
		/// @param[out] p_num_cutoff  the number of cut-off tests encountered when running this suite.
		int run (const std::string root_dir, std::string disabledtestfname, const std::vector<std::string> &disabled_tclist, const std::string regex_expr,
                 RegexType regex_flag, MANAGER &M, int no_parallel_testcases, int *testcases_total, int *nDisabled, REPORTER *rptr, int *suitesDisabled,
                 const std::set<std::string>& cutoff_set, int* p_num_cutoff);
		int collectSubSuites (std::string parentdir, std::string sid);
};
} //END namespace scidbtestharness

# endif
