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
 * @file iquery.cpp
 *
 * @author Roman Simakov <roman.simakov@gmail.com>
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief SciDB's querying utility
 */
#include <limits>
#include <string>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <fstream>
#include <algorithm>
#include <signal.h>

// include log4cxx header files.
#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <readline/readline.h>
#include <readline/history.h>

#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>
#include <boost/format.hpp>
#include <boost/filesystem.hpp>

#include <pwd.h>

#include <SciDBAPI.h>
#include <array/MemArray.h>
#include <system/Exceptions.h>
#include <system/ErrorCodes.h>
#include <smgr/io/ArrayWriter.h>
#include <rbac/SessionProperties.h>
#include <util/PluginManager.h>
#include <util/Utility.h>
#include <network/BaseConnection.h>

#include "commands.h"
#include "iquery_parser.h"
#include "IqueryConfig.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <network/proto/scidb_msg.pb.h>


using namespace std;
using namespace boost;
using namespace yy;
using namespace iquery;

static log4cxx::LoggerPtr logger = log4cxx::Logger::getRootLogger();


namespace bfs = boost::filesystem;

#define IQUERY_HISTORY_FILE "iquery.history"
#define IQUERY_CFG_FILE "iquery.conf"
#define IQUERY_AUTH_FILE "iquery.auth"

std::string szExecName;

bool getConfigPath(const string& fileName, string &path, bool &exists);
void configHook(int32_t configOption);


struct IQueryState
{
    size_t col;
    size_t line;
    size_t queryStart;

    bool insideComment;
    bool insideString;
    bool aql;
    bool interactive;

    void* connection;
    scidb::QueryID currentQueryID;

    bool firstSaving; //For clearing result file for the first time and appending next times

    bool nofetch;
    bool timer;
    bool verbose;

    bool ignoreErrors;

    std::string format;
    std::string authenticationFile;
};

IQueryState iqueryState;

ostream& operator<<(ostream &out, IQueryState const& state)
{
    out << "IQueryState("
        << " col=" << state.col
        << " line=" << state.line
        << " queryStart=" << state.queryStart
        << " insideComment=" << state.insideComment
        << " insideString=" << state.insideString
        << " aql=" << state.aql
        << " interactive=" << state.interactive
        << " connection=" << state.connection
        << " currentQueryID=" << state.currentQueryID
        << " firstSaving=" << state.firstSaving
        << " nofetch=" << state.nofetch
        << " timer=" << state.timer
        << " verbose=" << state.verbose
        << " ignoreErrors=" << state.ignoreErrors
        << " format='" << state.format
        << "' authFile='" << state.authenticationFile
        << "')";

    return out;
}

const char* helpString()
{
    return
      "set            - List current options\n"
      "set lang afl   - Set AFL as querying language\n"
      "set lang aql   - Set AQL as querying language\n"
      "set fetch      - Start retrieving query results\n"
      "set no fetch   - Stop retrieving query results\n"
      "set timer      - Start reporting query setup time\n"
      "set no timer   - Stop reporting query setup time\n"
      "set verbose    - Start reporting details from engine\n"
      "set no verbose - Stop reporting details from engine\n"
      "set format <base_format> [ :<options> ] - Switch output format\n"
      "quit or exit   - End iquery session-\n"
      "\n"
      // See supportedFormats in ArrayWriter.cpp
      "Available base_format values are:\n"
      "csv     - Comma separated values\n"
      "csv+    - Comma separated values including coordinates\n"
      "dcsv    - \"Display CSV\", a more human-readable CSV variant\n"
      "dense   - A variant of 'text' suitable for dense data\n"
      "sparse  - A variant of 'text' suitable for sparse data\n"
      "opaque  - SciDB raw storage format\n"
      "store   - A variant of 'text' that includes overlap regions\n"
      "text    - SciDB native text format\n"
      "tsv     - Tab separated values (LinearTSV dialect)\n"
      "tsv+    - Tab separated values including coordinates (LinearTSV)\n"
      "\n"
      // See Value::Formatter in TypeSystem.cpp and XsvParms in ArrayWriter.cpp
      "Option flags for modifying output format.  Not all flags affect all\n"
      "formats.  These may be combined:\n"
      "d - Use double quotes in CSV output\n"
      "E - Use an empty string for null in TSV or CSV output\n"
      "l - Print label line for TSV or CSV output\n"
      "N - Use \\N for null in CSV output\n"
      "n - Use the unquoted token 'null' for null in TSV or CSV output\n"
      "s - Use single quotes in CSV output (normally the default)\n"
      "? - Use ?0 for null in TSV or CSV output\n"
      ;
}

void saveHistory()
{
    string path;
    bool exists;
    if (getConfigPath(IQUERY_HISTORY_FILE, path, exists))
    {
        write_history(path.c_str());
    }
}

void loadHistory()
{
    string path;
    bool exists;
    if (getConfigPath(IQUERY_HISTORY_FILE, path, exists))
    {
        read_history(path.c_str());
    }
}

void executePreparedSciDBQuery(const string &queryString, scidb::QueryResult& queryResult, string const& format)
{
    scidb::SciDB& sciDB = scidb::getSciDB();
    scidb::Config *cfg = scidb::Config::getInstance();
    const vector<string> plugins = queryResult.plugins;

    if (iqueryState.verbose)
    {
        cout << "Query ID: " << queryResult.queryID << endl;
    }

    sciDB.executeQuery(queryString, !iqueryState.aql, queryResult, iqueryState.connection);

    if (queryResult.selective && !iqueryState.nofetch)
    {
        /**
         * Printing result schema
         */
        if (iqueryState.verbose)
        {
            const scidb::ArrayDesc d = queryResult.array->getArrayDesc();
            cout << "Result schema: " << (d.getName().empty() ? "<unnamed>" : d.getName()) << " <";
            for (size_t i = 0; i < d.getAttributes().size() - 1; i++)
            {
                cout << d.getAttributes()[i] << ", ";
            }
            cout << d.getAttributes()[d.getAttributes().size() - 1] << ">[";

            for (size_t i = 0; i < d.getDimensions().size() - 1; i++)
            {
                cout << d.getDimensions()[i] << ", ";
            }
            cout << d.getDimensions()[d.getDimensions().size() - 1] << "]" << endl;

            if (plugins.size() > 0)
            {
                vector<string>::const_iterator it = plugins.begin();
                cout << "Used plugins: " << *it;
                ++it;
                for (; it != plugins.end(); ++it)
                    cout << ", " << *it;
                cout << ";" << endl;
            }
        }

        uint64_t numCells=0;
        uint64_t numChunks=0;
        /**
         * Fetching result array
         */
        if ("/dev/null" == cfg->getOption<string>(CONFIG_RESULT_FILE))
        {
            scidb::AttributeID nAttrs = (scidb::AttributeID)queryResult.array->getArrayDesc().getAttributes().size();
            std::vector< std::shared_ptr<scidb::ConstArrayIterator> >iterators(nAttrs);
            for (scidb::AttributeID i = 0; i < nAttrs; i++)
            {
                iterators[i] = queryResult.array->getConstIterator(i);
            }
            uint64_t totalSize = 0;
            while (!iterators[0]->end())
            {
                numChunks++;
                for (scidb::AttributeID i = 0; i < nAttrs; i++)
                {
                    scidb::ConstChunk const& chunk = iterators[i]->getChunk();
                    totalSize += chunk.getSize();
                    if (i ==0 )
                    {
                        numCells += chunk.count();
                    }
                    ++(*iterators[i]);
                }

            }
            if (iqueryState.verbose)
            {
                cout << "Result size (bytes): " << totalSize;
                if (numCells)
                {
                    cout<< " chunks: " << numChunks
                        << " cells: " << numCells
                        << " cells/chunk: "
                        << static_cast<double>(numCells) / static_cast<double>(numChunks) << endl;
                }
                else
                {
                    cout<< " chunks: 0"<< endl;
                }
            }
        }
        else
        {
            scidb::ArrayWriter::setPrecision(cfg->getOption<int>(CONFIG_PRECISION));

            std::shared_ptr<scidb::Query> emptyQuery; //query is not validated on the client side
            scidb::ArrayWriter::save(*queryResult.array,
                                     cfg->getOption<string>(CONFIG_RESULT_FILE),
                                     emptyQuery,
                                     format,
                                     (iqueryState.firstSaving ? 0 : scidb::ArrayWriter::F_APPEND));
            iqueryState.firstSaving = false;
        }

        if (iqueryState.timer) {
            cout << "Query execution time: " << queryResult.executionTime << "ms" << endl;
        }

        if (iqueryState.verbose)
        {
            cout << "Query execution time: " << queryResult.executionTime << "ms" << endl;
            cout << "Logical plan: " << endl << queryResult.explainLogical << endl;
            cout << "Physical plans: " << endl << queryResult.explainPhysical << endl;
        }
    }

    if (queryResult.hasWarnings())
    {
        cerr << "Warnings during execution:" << endl;
        while (queryResult.hasWarnings())
        {
            cerr << queryResult.nextWarning().msg() << endl;
        }
    }
}

void executeSciDBQuery(const string &queryString)
{
    scidb::QueryResult queryResult;
    scidb::SciDB& sciDB = scidb::getSciDB();
    string const& format = iqueryState.format;

    sciDB.prepareQuery(queryString, !iqueryState.aql, "", queryResult, iqueryState.connection);

    iqueryState.currentQueryID = queryResult.queryID;

    if (queryResult.hasWarnings())
    {
        cerr << "Warnings during preparing:" << endl;
        while (queryResult.hasWarnings())
        {
            cerr << queryResult.nextWarning().msg() << endl;
        }
    }

    executePreparedSciDBQuery(queryString, queryResult, format);

    iqueryState.currentQueryID = scidb::QueryID();

    if (queryResult.queryID.isValid() &&
        !queryResult.autoCommit &&
        iqueryState.connection)
    {
        sciDB.completeQuery(queryResult.queryID, iqueryState.connection);
    }

    if (!queryResult.selective || iqueryState.nofetch)
    {
        // The query is not selective. Maybe DDL.
        cout << "Query was executed successfully" << endl;
    }
}

void executeCommandOrQuery(const string &query)
{
    string trimmedQuery = query;
    boost::trim(trimmedQuery);
    if (trimmedQuery == ""
        || (trimmedQuery.size() >= 2
            && trimmedQuery[0] == '-'
            && trimmedQuery[1] == '-'
            &&  trimmedQuery.find('\n') == string::npos))
    {
        return;
    }

    try
    {
        IqueryParser p;

        if (p.parse(query))
        {
            // Parsing failed.

            if (!p.isIqueryCommand())
            {
                // If this is not an iquery command, try to execute on the server.
                executeSciDBQuery(query);
            }
            else
            {
                // If this is an iquery command, it got an error in it. Show the error and return.
                if (scidb::Config::getInstance()->getOption<string>(CONFIG_QUERY_FILE) != "")
                    cerr << "Error in file '" << scidb::Config::getInstance()->getOption<string>(CONFIG_QUERY_FILE)
                         << "' near line " << iqueryState.queryStart << endl;
                cerr << "Unknown command '" << query << "' .\n"
                    "Type 'help;' for iquery internal commands reference."
                     << endl;
                return;
            }
        }
        else
        {
            // Parsed correctly now let's execute command;
            assert(p.getResult());
            switch (p.getResult()->getCmdType())
            {
            case IqueryCmd::HELP:
                cout << helpString() << flush;
                break;

            case IqueryCmd::SET:
                cout <<
                    "Lang:    " << (iqueryState.aql ? "AQL" : "AFL") <<
                    "\nFetch:   " << (iqueryState.nofetch ? "NO" : "YES") <<
                    "\nTimer:   " << (iqueryState.timer ? "YES" : "NO") <<
                    "\nVerbose: " << (iqueryState.verbose ? "YES" : "NO") <<
                    "\nFormat:  " << iqueryState.format << endl;
                break;

            case IqueryCmd::FETCH:
                iqueryState.nofetch = !((const IntIqueryCmd*)p.getResult())->getValue();
                break;

            case IqueryCmd::VERBOSE:
                iqueryState.verbose = ((const IntIqueryCmd*)p.getResult())->getValue();
                break;

            case IqueryCmd::TIMER:
                iqueryState.timer = ((const IntIqueryCmd*)p.getResult())->getValue();
                break;

            case IqueryCmd::QUIT:
                saveHistory();
                exit(0);
                break;

            case IqueryCmd::LANG:
                iqueryState.aql = (((const IntIqueryCmd*)p.getResult())->getValue() == 0) ? true : false;
                break;

            case IqueryCmd::FORMAT:
                iqueryState.format = ((const StrIqueryCmd*)p.getResult())->getValue();
                break;

            case IqueryCmd::BINARY_FORMAT:
            {
                string fmt = ((const StrIqueryCmd*)p.getResult())->getValue();
                boost::trim(fmt);
                if (fmt[0] != '(' || fmt[fmt.size() - 1] != ')')
                {
                    cerr << "Binary format template should be surrounded by parentheses" << endl;
                }
                else
                {
                    iqueryState.format = fmt;
                }
                break;
            }

            default:
                assert(0);
            }
        }
    }
    catch (const scidb::Exception& e)
    {
        //Eat scidb exceptions, cleanup query, print error message and continue, if in interactive mode
        scidb::SciDB& sciDB = scidb::getSciDB();

        //Don't try to cancel query if we have connection problems!
        if (iqueryState.currentQueryID.isValid() && iqueryState.connection
            && !(e.getShortErrorCode() == scidb::SCIDB_SE_NETWORK))
        {
            try
            {
                sciDB.cancelQuery(iqueryState.currentQueryID, iqueryState.connection);
            }
            catch (const scidb::Exception& e)
            {
                if (e.getLongErrorCode() != scidb::SCIDB_LE_QUERY_NOT_FOUND
                    && e.getLongErrorCode() != scidb::SCIDB_LE_QUERY_NOT_FOUND2) {
                    cerr << "Error during query canceling: " << endl << e.what() << endl << endl;
                }
            }
        }

        iqueryState.currentQueryID = scidb::QueryID();

        if (scidb::Config::getInstance()->getOption<string>(CONFIG_QUERY_FILE) != "")
        {
            cerr << "Error in file '" << scidb::Config::getInstance()->getOption<string>(CONFIG_QUERY_FILE)
                 << "' near line " << iqueryState.queryStart << endl;
        }
        cerr << e.what() << endl;

        if ((!iqueryState.interactive && !iqueryState.ignoreErrors)
            || e.getShortErrorCode() == scidb::SCIDB_SE_NETWORK)
        {
            exit(1);
        }
    }
    catch (const std::exception& e)
    {
        // Eat all other exceptions exception in interactive mode,
        // but exit with error in non-interactive
        cerr << szExecName << ": Exception caught " << e.what() << endl;
        if (!iqueryState.interactive) {
            exit(1);
        }
    }
}

void getPasswordFromTty(scidb::Credential* cred, void*)
{
    assert(cred);
    string user(cred->getUsername());
    if (user.empty()) {
        char *cp = ::getenv("USER");
        if (cp) {
            user = cp;
        }
    }

    string prompt("Enter");
    if (!user.empty()) {
        prompt += " " + user;
    }
    prompt += " password: ";

    // We are not multi-threaded or doing fancy signal handling, so
    // IMHO using the allegedly obsolete getpass(3) is permissible.
    char *pass = ::getpass(prompt.c_str());
    if (!pass) {
        stringstream ss;
        ss << "Cannot get password from tty: " << ::strerror(errno);
        using scidb::SCIDB_SE_AUTHENTICATION;
        using scidb::SCIDB_LE_UNKNOWN_ERROR;
        throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_UNKNOWN_ERROR)
            << ss.str();
    }
    cred->setPassword(pass);
}


void termination_handler(int signum)
{
    if (iqueryState.interactive) {
        saveHistory();
    }

    // To avoid hangs and unexpected errors caused by mixing
    // the query traffic with the cancelQuery traffic on the same connection
    // we will just hard stop here.
    // Note that _exit() is async-signal-safe.
    _exit(1);
}

int main(int argc, char* argv[])
{
    struct sigaction action;
    action.sa_handler = termination_handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    sigaction (SIGINT, &action, NULL);
    sigaction (SIGTERM, &action, NULL);

    szExecName = std::string ( argv[0]);

    int ret = 0;

    string queries = ""; //all set of queries (can be divided with semicolon)

    iqueryState.insideComment = false;
    iqueryState.insideString = false;
    iqueryState.aql = true;

    iqueryState.col = 1;
    iqueryState.line = 1;
    iqueryState.connection = NULL;
    iqueryState.interactive = false;
    iqueryState.currentQueryID = scidb::QueryID();
    iqueryState.firstSaving = true;

    try
    {
        log4cxx::BasicConfigurator::configure();

        string cfgPath;
        {
            bool exists;
            if (!getConfigPath(IQUERY_CFG_FILE, cfgPath, exists) || !exists)
                cfgPath = "";
        }

        scidb::Config *cfg = scidb::Config::getInstance();

        cfg->addOption
            (CONFIG_PRECISION, 'w', "precision", "PRECISION", "", scidb::Config::INTEGER,
                "Precision for printing floating point numbers. Default is 6", 6, false)
            (CONFIG_HOST, 'c', "host", "host", "IQUERY_HOST", scidb::Config::STRING,
                "Host of one of the cluster instances. Default is 'localhost'", string("localhost"), false)
            (CONFIG_PORT, 'p', "port", "port", "IQUERY_PORT", scidb::Config::INTEGER,
                "Port for connection. Default is 1239", 1239, false)
            (CONFIG_QUERY_STRING, 'q', "query", "", "", scidb::Config::STRING,
                "Query to be executed", string(""), false)
            (CONFIG_QUERY_FILE, 'f', "query-file", "", "", scidb::Config::STRING,
                "File with query to be executed", string(""), false)
            (CONFIG_AFL, 'a', "afl", "afl", "", scidb::Config::BOOLEAN,
                "Switch to AFL query language mode. AQL by default", false, false)
            (CONFIG_TIMER, 't', "timer", "timer", "", scidb::Config::BOOLEAN,
                "Print query execution time (in milliseconds)", false, false)
            (CONFIG_VERBOSE, 'v', "verbose", "verbose", "", scidb::Config::BOOLEAN,
                "Print debug info. Disabled by default", false, false)
            (CONFIG_RESULT_FILE, 'r', "result", "", "", scidb::Config::STRING,
                "Filename with result array data", string("console"), false)
            (CONFIG_NO_FETCH, 'n', "no-fetch", "", "", scidb::Config::BOOLEAN,
                "Skip data fetching. Disabled by default", false, false)
            (CONFIG_RESULT_FORMAT, 'o', "format", "format", "", scidb::Config::STRING,
                "Output format. Type 'iquery -q help' for details. Default is 'dcsv'.",
             string("dcsv"), false)
            (CONFIG_PLUGINSDIR, 'u', "pluginsdir", "plugins", "", scidb::Config::STRING,
                "Path to the plugins directory",
                string(scidb::SCIDB_INSTALL_PREFIX()) + string("/lib/scidb/plugins"),
                false)
            (CONFIG_HELP, 'h', "help", "", "", scidb::Config::BOOLEAN,
                "Show this help text", false, false)
            (CONFIG_VERSION, 'V', "version", "", "", scidb::Config::BOOLEAN,
                "Show version info", false, false)
            (CONFIG_IGNORE_ERRORS, 0, "ignore-errors", "", "", scidb::Config::BOOLEAN,
                "Ignore execution errors in batch mode", false, false)
            (CONFIG_AUTHENTICATION_FILE, 'A', "auth-file", "auth-file", "",
                scidb::Config::STRING, "User authentication file", string(""), false)
            (CONFIG_ADMIN, 0, "admin", "admin", "", scidb::Config::BOOLEAN,
                "Create an administrative connection to use reserved system resources", false, false)
            ;

        cfg->addHook(configHook);
        cfg->parse(argc, argv, cfgPath.c_str());

        const std::string& connectionString = cfg->getOption<string>(CONFIG_HOST);
        uint16_t port = scidb::safe_static_cast<uint16_t>(cfg->getOption<int>(CONFIG_PORT));
        const std::string& queryFile = cfg->getOption<string>(CONFIG_QUERY_FILE);
        std::string queryString = cfg->getOption<string>(CONFIG_QUERY_STRING);


        scidb::PluginManager::getInstance()->setPluginsDirectory(
            cfg->getOption<string>(CONFIG_PLUGINSDIR));

        iqueryState.aql          = !cfg->getOption<bool>(CONFIG_AFL);
        iqueryState.verbose      = cfg->getOption<bool>(CONFIG_VERBOSE);
        iqueryState.nofetch      = cfg->getOption<bool>(CONFIG_NO_FETCH);
        iqueryState.timer        = cfg->getOption<bool>(CONFIG_TIMER);
        iqueryState.ignoreErrors = cfg->getOption<bool>(CONFIG_IGNORE_ERRORS);
        iqueryState.format       = cfg->getOption<string>(CONFIG_RESULT_FORMAT);
        iqueryState.authenticationFile = cfg->getOption<string>(CONFIG_AUTHENTICATION_FILE);

        // If we don't have an auth file, try our best to get one.
        if (iqueryState.authenticationFile.empty()) {
            // First try SCIDB_CONFIG_USER environment variable.
            const char* path = ::getenv("SCIDB_CONFIG_USER");
            if (path) {
                LOG4CXX_DEBUG(logger, "SCIDB_CONFIG_USER=" << path);
                iqueryState.authenticationFile = path;
            }
            // Next try per-user auth file.
            else {
                bool exists = false;
                string path;
                bool ok = getConfigPath(IQUERY_AUTH_FILE, path, exists);
                if (ok && exists) {
                    SCIDB_ASSERT(!path.empty());
                    iqueryState.authenticationFile = path;
                    LOG4CXX_DEBUG(logger, "Authenticating with " << path);
                }
            }
        }

        if (!queryString.empty())
        {
            queries = queryString;
        }
        else if (queryString.empty() && !queryFile.empty())
        {
            //
            // PGB: Not good.
            //
            //      This method for handling the file creates the awful
            //        possibility for divergence between the format from
            //      files, interactive commands (from a pipe say), and
            //         commands given "-q" option.
            ifstream ifs;
            ifs.exceptions(ifstream::eofbit | ifstream::failbit | ifstream::badbit);
            ifs.open(queryFile.c_str());
            string str((istreambuf_iterator<char>(ifs)), istreambuf_iterator<char>());
            ifs.close();
            queries = str;
        }
        else if (queryString.empty() && queryFile.empty())
        {
            // Read stdin if (a) it's not a tty or (b) explicit "-" argument.
            bool readStdin = !::isatty(fileno(stdin));
            if (!readStdin)
            {
                for (int i = 0; i < argc; ++i)
                {
                    if (!strcmp(argv[i], "-"))
                        readStdin = true;
                }
            }

            int ch;
            if (readStdin)
            {
                while ((ch = fgetc (stdin)) != EOF)
                {
                    queries += static_cast<char>(ch);
                }
            }
            else
            {
                iqueryState.interactive = true;
            }
        }


        if (!iqueryState.verbose)
        {
            log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getError());
        }

        if (iqueryState.interactive)
            loadHistory();


        scidb::Credential cred;
        if(!iqueryState.authenticationFile.empty())
        {
            cred.fromAuthFile(iqueryState.authenticationFile);
        }
        if (cred.getUsername().empty())
        {
            cred.setUsername(::getenv("USER"));
        }
        scidb::SessionProperties sessionProperties(cred);
        if (cred.getPassword().empty()) {
            sessionProperties.setCredCallback(&getPasswordFromTty);
        }

        bool isAdmin = cfg->getOption<bool>(CONFIG_ADMIN);
        if (isAdmin) {
            sessionProperties.setPriority(scidb::SessionProperties::ADMIN);
        }

        scidb::SciDB& sciDB = scidb::getSciDB();
        iqueryState.connection = sciDB.connect(sessionProperties, connectionString, port);


        string query = ""; // separated query from overall set of queries

        // Whenever '{' or '[' is encountered, the count is increased by 1.
        // Whenever '}' or ']' is encountered, the count is reduced by 1.
        // The usage: do NOT terminate a query at ';' if the count is greater than 0.
        // TO-DO: negative count should be reported as an exception but omit this error checking for now.
        int nLevelsInsideBrackets = 0;

        do
        {
            // We analyzing begin of queries or next query, so reset position
            if (query == "")
            {
                iqueryState.col = 1;
                iqueryState.queryStart = iqueryState.line;
            }

            // If we in interactive mode, requesting next line of query(ies)
            if (iqueryState.interactive)
            {
                iqueryState.insideComment = false;
                char *line = readline(query == "" ? (iqueryState.aql ? "AQL% " : "AFL% ") : "CON> ");
                if (line) {
                    queries = line;
                } else {
                    break;
                }

                // Ignore whitelines in begin of queries
                string trimmedQueries = queries;
                boost::trim(trimmedQueries);
                if (trimmedQueries == "" && query == "") {
                    continue;
                }
            }

            // Parsing next line of query(ies)
            char currC = 0;     // Character in current position
            char prevC = 0; // Character in previous position
            bool eoq = false;

            for (size_t pos = 0; pos < queries.size(); ++pos)
            {
                prevC = currC;
                currC = queries[pos];

                // Checking string literal begin and end, but ignore if current part of query is comment
                if (currC == '\'' && prevC != '\\' && !iqueryState.insideComment)
                {
                    iqueryState.insideString = !iqueryState.insideString;
                }

                // Checking comment, but ignore if current part of query is string
                if (currC == '-' && prevC == '-' && !iqueryState.insideString)
                {
                    iqueryState.insideComment = true;
                }

                // Checking newline. Resetting comment if present
                if (currC == '\n')
                {
                    iqueryState.insideComment = false;
                    ++iqueryState.line;
                    iqueryState.col = 1;

                    if (query == "") {
                        iqueryState.queryStart = iqueryState.line;
                    } else {
                        query += currC;
                    }
                }
                // Checking query separator, if not in string and comment, execute this query
                else if (currC == ';' && !iqueryState.insideComment && !iqueryState.insideString
                        && nLevelsInsideBrackets==0)
                {
                    executeCommandOrQuery(query);
                    query = "";
                    eoq = true;
                    ++iqueryState.col;
                }
                else if (!iqueryState.insideComment && !iqueryState.insideString) {
                    // Maintain nLevelsInsideBrackets.  Allows mispairings "{]",
                    // but if you do that you have bigger problems.
                    switch (currC) {
                    case '{':
                    case '[':
                        ++nLevelsInsideBrackets;
                        query += currC;
                        break;
                    case '}':
                    case ']':
                        --nLevelsInsideBrackets;
                        query += currC;
                        break;
                    default:
                        query += currC;
                        ++iqueryState.col;
                        break;
                    }
                }
                // All other just added to query
                else
                {
                    query += currC;
                    ++iqueryState.col;
                }
            }

            if (eoq) {
                boost::trim_left(query);
            }

            // Adding last part of query to history
            if (iqueryState.interactive) {
                add_history(queries.c_str());
            }

            // If interactive add newline after any part of query to maintain original query view
            if (iqueryState.interactive && query != "")
            {
                query += '\n';
            }
            // Execute last part of query even without leading semicolon in non-interactive mode
            else if (!iqueryState.interactive && query != "")
            {
                executeCommandOrQuery(query);
            }
        }
        while (iqueryState.interactive);
    }
    catch (const std::exception& e)
    {
        std::cerr << szExecName << " " << e.what() << std::endl;
        ret = 1;
    }

    if (iqueryState.interactive)
    {
        saveHistory();
    }

    return ret;
}

bool getConfigPath(const string& fileName, string &path, bool &exists)
{
    exists = false;
    // Forming path to config based of f.d.o. specification
    string cfgDir = "";
    // If XDG_CONFIG_HOME defined, using it
    if (getenv("XDG_CONFIG_HOME"))
    {
        cfgDir = str(format("%s/scidb") % getenv("XDG_CONFIG_HOME"));
    }
    // Else fallback to ~/.config dir
    else
    {
        struct passwd *pw = getpwuid(getuid());

        if (!pw || !(pw->pw_dir))
        {
            return false;
        }

        cfgDir = str(format("%s/.config/scidb") % pw->pw_dir);
    }


    // Check directory existing and create if needed
    try
    {
        if (bfs::exists(cfgDir))
        {
            if (!bfs::is_directory(cfgDir))
            {
                return false;
            }
        }
        else
        {
            if (!bfs::create_directories(cfgDir))
            {
                return false;
            }
        }

        path = cfgDir + "/" + fileName;

        exists = bfs::exists(path) && !bfs::is_directory(path);
    }
    catch (const boost::exception&)
    {
        return false;
    }
    catch (const std::exception&)
    {
        return false;
    }

    return true;
}

void configHook(int32_t configOption)
{
    switch (configOption)
    {
        case CONFIG_HELP:
            cout << "Available options:" << endl
                << scidb::Config::getInstance()->getDescription() << endl;
            exit(0);
            break;

        case CONFIG_VERSION:
            cout << scidb::SCIDB_BUILD_INFO_STRING() << endl;
            exit(0);
            break;
    }
}
