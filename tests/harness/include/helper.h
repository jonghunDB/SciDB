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
 * @file helper.h
 * @author girish_hilage@persistent.co.in
 * @brief file containing helper functions for general purpose
 */

# ifndef HELPER_H
# define HELPER_H

# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <set>
# include <string>
# include <vector>
# include <sys/socket.h>
# include <net/if.h>

# define TESTCASE_IDS   1
# define TESTCASE_NAMES 2
# define DIFF_FILES_DIFFER   1
# define DIFF_FILES_MATCH    2

# include "global.h"

namespace scidbtestharness
{
int if_addr_fetch (struct ifconf *ifc, int fd);
int ReadOutputOf (const std::string &command, FILE **pipe, char *buf, int len, int *exit_code);
int runShellCommand (const std::string &command);
int manual_diff (const std::string &file1, const std::string &file2, const std::string &diff_file);
int diff (const std::string &scratch_dir,
          const std::string &file1, const std::string &file2, const std::string &diff_file);
void print_execution_stats (const struct ExecutionStats &es);
bool check_regex_match (const std::string fmt, const std::string str);
int remove_duplicates (std::vector <std::string> &strcollection);
std::string converttoid (std::string rootdir, std::string filename);
std::string converttopath (std::string testid);
void print_vector (const std::vector<std::string> v);
int filterDisabledTestSuites (std::vector <std::string> &suitelist, std::vector <std::string> &disabled_tclist);

/// @param cutoff_set  the set of cutoff tests.
/// @return <num_disabled, num_disabled_due_to_cutoff>
std::pair<int, int> filterDisabledTestCases (std::vector <std::string> &tclist, const std::vector <std::string> &disabled_tclist,
                                             const std::set<std::string>& cutoff_set);

/// collectDisabledTestCases() collects a list of disabled tests and disabled suites.
/// It is called in two cases:
/// - SciDBTestHarness::execute() calls it, to gather the list from the top-level disable.tests file.
/// - Suite::run() calls it, to collect from disable.tests in child directories, if any.
/// Only in the first case shall we gather the cutoff set. The set will be passed to filterDisabledTestCases().
/// So in the second use case, nullptr will be used as p_cutoff_set.
/// @param[out] disabled_tclist  the list of tests that are disabled.
/// @param[out] p_cutoff_set     nullptr, or placeholder to the subset of disabled tests due to cutoff.
int collectDisabledTestCases (std::string root_dir, std::string under_directory, std::string disabledtestfname, std::vector <std::string> &disabled_tclist,
                              std::set<std::string>* p_cutoff_set);
int collectTestCases (std::string root_dir, std::string under_directory, const std::string regex_expr, RegexType regex_flag,
                      std::vector <std::string> &tclist);
int collectTestCases (std::string root_dir, const std::vector <std::string> &testcaseids, const std::string regex_expr, RegexType regex_flag,
                      std::vector <std::string> &tclist, std::string under_directory=DEFAULT_TEST_CASE_DIR, int flag = TESTCASE_IDS);
int tokenize_commandline (const std::string str, std::vector<std::string> &token_list);
int tokenize (const std::string str, std::vector<std::string> &token_list, const std::string separators);
std::string getAbsolutePath (std::string path, bool logger_enabled);
std::string getAbsolutePath (std::string path);
bool Is_regular (const std::string fname);
int Creat(const char *pathname, mode_t mode);
long int sToi (const std::string str);
std::string iTos (long long int i);
void prepare_filepaths (InfoForExecutor &ie, int internally_called=false);
int Socket (int domain, int type, int protocol);
int Close (int fd);
int Open (const char *pathname, int flags);
void normalizePath (std::vector <std::string> &pathvec);
} //END namespace scidbtestharness

# endif
