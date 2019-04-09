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
 * entry.cpp
 *
 *  Created on: Dec 28, 2009
 *      Author: roman.simakov@gmail.com
 */

#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <boost/io/ios_state.hpp>

#include <dlfcn.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <malloc.h>
#include <fstream>

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <query/FunctionLibrary.h>
#include <query/OperatorLibrary.h>

#include <array/NewReplicationMgr.h>
#include <dense_linear_algebra/blas/initMathLibs.h>
#include <network/NetworkManager.h>
#include <query/FunctionLibrary.h>
#include <query/OperatorLibrary.h>
#include <query/Parser.h>
#include <query/QueryProcessor.h>
#include <rbac/NamespaceDesc.h>
#include <rbac/RoleDesc.h>
#include <rbac/UserDesc.h>
#include <system/Config.h>
#include <system/Constants.h>
#include <system/SciDBConfigOptions.h>
#include <system/SystemCatalog.h>
#include <system/Utils.h>
#include <util/InjectedError.h>
#include <util/JobQueue.h>
#include <util/PathUtils.h>
#include <util/PluginManager.h>
#include <util/RTLog.h>
#include <util/ThreadPool.h>
#include <util/Utility.h>

#ifdef COVERAGE
extern "C" void __gcov_flush(void);
#endif

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.entry"));

namespace scidb {namespace arena {bool setMemoryLimit(size_t);}}

using namespace scidb;
using namespace std;

std::shared_ptr<ThreadPool> messagesThreadPool;

void printPrefix(const char * msg=""); // forward decl

void scidb_termination_handler(int signum)
{
    NetworkManager::shutdown();
}

namespace scidb { namespace arena {
/// @return whether pooled allocation is used.
/// @see the definition in RootArena.cpp.
bool pooledAllocationWrapper();
}}

void runSciDB()
{
   struct sigaction action;
   action.sa_handler = scidb_termination_handler;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   sigaction (SIGINT, &action, NULL);
   sigaction (SIGTERM, &action, NULL);

   struct sigaction ignore;
   ignore.sa_handler = SIG_IGN;
   sigemptyset(&ignore.sa_mask);
   ignore.sa_flags = 0;
   sigaction (SIGPIPE, &ignore, NULL);

   Config *cfg = Config::getInstance();
   assert(cfg);

   // Force load
   MonitorConfig *mcfg = MonitorConfig::getInstance();
   SCIDB_ASSERT(mcfg);

   // Configure loggers and make first log entry.
   const string& log4cxxProperties = cfg->getOption<string>(CONFIG_LOGCONF);
   if (log4cxxProperties.empty()) {
      log4cxx::BasicConfigurator::configure();
      const string& log_level = cfg->getOption<string>(CONFIG_LOG_LEVEL);
      log4cxx::LoggerPtr rootLogger(log4cxx::Logger::getRootLogger());
      rootLogger->setLevel(log4cxx::Level::toLevel(log_level));
   }
   else {
      log4cxx::PropertyConfigurator::configure(log4cxxProperties.c_str());
   }
   LOG4CXX_INFO(logger, "Start SciDB instance (pid="<<getpid()<<"). " << SCIDB_BUILD_INFO_STRING(". "));

   // Force deciding whether to use pooled allocation, before threads are being used.
   bool usePooled = scidb::arena::pooledAllocationWrapper();
   LOG4CXX_INFO(logger, "Pooled memory allocation: " << (usePooled ? "ON" : "OFF"));

   // log the config
   LOG4CXX_INFO(logger, "Configuration:\n" << cfg->toString());

   // all subsytems with constraints on their config vars
   // should check them here prior to other initializations
   try
   {
       checkConfigNewStorage();
   }
   catch (const std::exception &e)
   {
      printPrefix();
      cerr << "Failed to initialize server configuration: " << e.what() << endl;
      scidb::exit(1);
   }

   // here we are guaranteed outside of mutex, we can read the
   // configuration variable (which uses a mutex) about whether
   // a mutex (and other waits) should be timed
   ScopedWaitTimer::adjustWaitTimingEnabled();

   //Initialize random number generator
   //We will try to read seed from /dev/urandom and if we can't for some reason we will take time and pid as seed
   ifstream file ("/dev/urandom", ios::in|ios::binary);
   unsigned int seed;
   if (file.is_open())
   {
       const size_t size = sizeof(unsigned int);
       char buf[size];
       file.read(buf, size);
       file.close();
       seed = *reinterpret_cast<unsigned int*>(buf);
   }
   else
   {
       seed = static_cast<unsigned int>(time(0) ^ (getpid() << 8));
       LOG4CXX_WARN(logger, "Cannot open /dev/urandom.  Using fallback seed based on time and pid.");
   }
   srandom(seed);

   if (cfg->getOption<int>(CONFIG_MAX_MEMORY_LIMIT) > 0)
   {
       size_t maxMem = ((int64_t) cfg->getOption<int>(CONFIG_MAX_MEMORY_LIMIT)) * MiB;
       LOG4CXX_DEBUG(logger, "Capping maximum memory:");

       if (scidb::arena::setMemoryLimit(maxMem))
       {
           LOG4CXX_WARN(logger, ">arena cap set to " << maxMem  << " bytes.");
       }
       else
       {
           LOG4CXX_WARN(logger, ">arena cap of " << maxMem <<
                        " is too small; the cap has already been exceeded.");
       }

       struct rlimit rlim;
       if (getrlimit(RLIMIT_AS, &rlim) != 0)
       {
           LOG4CXX_DEBUG(logger, ">getrlimit call failed: " << ::strerror(errno)
                         << " (" << errno << "); memory cap not set.");
       }
       else
       {
           if (rlim.rlim_cur == RLIM_INFINITY || rlim.rlim_cur > maxMem)
           {
               rlim.rlim_cur = maxMem;
               if (setrlimit(RLIMIT_AS, &rlim) != 0)
               {
                   LOG4CXX_DEBUG(logger, ">setrlimit call failed: " << ::strerror(errno)
                                 << " (" << errno << "); memory cap not set.");
               }
               else
               {
                   LOG4CXX_DEBUG(logger, ">memory cap set to " << rlim.rlim_cur  << " bytes.");
               }
           }
           else
           {
               LOG4CXX_DEBUG(logger, ">memory cap " << rlim.rlim_cur
                             << " is already under " << maxMem << "; not changed.");
           }
       }
   }

   string tmpDir = FileManager::getInstance()->getTempDir();
   if (tmpDir.length() == 0 || tmpDir[tmpDir.length()-1] != '/') {
       tmpDir += '/';
   }
   int rc = path::makedirs(tmpDir, 0755, nothrow_t());
   if (rc) {
       LOG4CXX_ERROR(logger, "Cannot create FileManager temp directory "
                     << tmpDir << ": " << ::strerror(rc));
       throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_OPEN_FILE)
           << tmpDir << ::strerror(rc) << rc;
   }

   string memArrayBasePath = tmpDir + "/memarray";

   int largeMemLimit = cfg->getOption<int>(CONFIG_LARGE_MEMALLOC_LIMIT);
   if (largeMemLimit>0 && (0==mallopt(M_MMAP_MAX, largeMemLimit))) {

       LOG4CXX_WARN(logger, "Failed to set large-memalloc-limit");
   }

   size_t smallMemSize = cfg->getOption<size_t>(CONFIG_SMALL_MEMALLOC_SIZE);
   if (smallMemSize>0 && (0==mallopt(M_MMAP_THRESHOLD, safe_static_cast<int>(smallMemSize)))) {
       LOG4CXX_WARN(logger, "Failed to set small-memalloc-size");
   }

   std::shared_ptr<JobQueue> messagesJobQueue = std::make_shared<JobQueue>("messagesJobQueue");

   // Here we can play with thread number
   const uint32_t nJobs = std::max(cfg->getOption<int>(CONFIG_EXECUTION_THREADS),3);
   messagesThreadPool = make_shared<ThreadPool>(nJobs, messagesJobQueue, "messagesThreadPool");

   SystemCatalog* catalog = SystemCatalog::getInstance();
   const bool initializeCluster = Config::getInstance()->getOption<bool>(CONFIG_INITIALIZE);
   try
   {
       //Disable metadata upgrade in initialize mode
       catalog->connect(!initializeCluster);
   }
   catch (const std::exception &e)
   {
       LOG4CXX_ERROR(logger, "System catalog connection failed: " << e.what());
       scidb::exit(1);
   }
   int errorCode = 0;
   try
   {
       if (!catalog->isInitialized() || initializeCluster)
       {
           catalog->initializeCluster();
       }

       // PathUtils pathname expansion initialization.
       path::initialize();

       catalog->invalidateTempArrays();

       TypeLibrary::registerBuiltInTypes();

       FunctionLibrary::getInstance()->registerBuiltInFunctions();

       // Force preloading builtin operators
       OperatorLibrary::getInstance();

       PluginManager::getInstance()->preLoadLibraries();

       loadPrelude();  // load built in macros

       // Pull in the injected error library symbols
       InjectedErrorLibrary::getLibrary()->getError(InjectErrCode::LEGACY_INITIALIZE);
       PhysicalOperator::getInjectedErrorListener();
       ThreadPool::startInjectedErrorListener();

       NewReplicationManager::getInstance()->start(messagesJobQueue);

       messagesThreadPool->start();
       NetworkManager::getInstance()->run(messagesJobQueue);
   }
   catch (const std::exception &e)
   {
       LOG4CXX_ERROR(logger, "Error during SciDB execution: " << e.what());
       errorCode = 1;
   }
   try
   {
      Query::freeQueries();
      if (messagesThreadPool) {
         messagesThreadPool->stop();
      }
      NewReplicationManager::getInstance()->stop();
   }
   catch (const std::exception &e)
   {
      LOG4CXX_ERROR(logger, "Error during SciDB exit: " << e.what());
      errorCode = 1;
   }
   LOG4CXX_INFO(logger, "SciDB instance. " << SCIDB_BUILD_INFO_STRING(". ") << " is exiting.");
   log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getOff());
   scidb::exit(errorCode);
}

void printPrefix(const char * msg)
{
   time_t t = time(NULL);
   assert(t!=(time_t)-1);
   struct tm tee_em;
   struct tm *date = localtime_r(&t, &tee_em);
   assert(date);
   assert(date == &tee_em);
   if (date) {
       boost::io::ios_all_saver guard(cerr);
       cerr << setfill('0')
            << date->tm_year+1900 << "-"
            << setw(2) << date->tm_mon + 1 << "-"
            << setw(2) << date->tm_mday << " "
            << setw(2) << date->tm_hour << ":"
            << setw(2) << date->tm_min << ":"
            << setw(2) << date->tm_sec << " ";
   }
   cerr << "(ppid=" << getpid() << "): " << msg;
}

void handleFatalError(const int err, const char * msg)
{
   printPrefix(msg);
   cerr << ": "
        << err << ": "
        << strerror(err) << endl;
   scidb::exit(1);
}

int controlPipe[2];

void setupControlPipe()
{
   close(controlPipe[0]);
   close(controlPipe[1]);
   if (pipe(controlPipe)) {
      handleFatalError(errno,"pipe() failed");
   }
}

void checkPort()
{
    uint16_t n = 10;
    while (true) {
        try {
            boost::asio::io_service ioService;
            boost::asio::ip::tcp::acceptor
            testAcceptor(ioService,
                         boost::asio::ip::tcp::endpoint(
                             boost::asio::ip::tcp::v4(),
                             safe_static_cast<uint16_t>(
                                 Config::getInstance()->getOption<int>(CONFIG_PORT))));
            testAcceptor.close();
            ioService.stop();
            return;
        } catch (const boost::system::system_error& e) {
            if ((n--) <= 0) {
                printPrefix();
                cerr << e.what() << ". Exiting." << endl;
                scidb::exit(1);
            }
        }
        sleep(1);
    }
}

void terminationHandler(int signum)
{
   unsigned char byte = 1;
   ssize_t ret = write(controlPipe[1], &byte, sizeof(byte));
   if (ret!=1){}
   printPrefix("Terminated.\n");
   // A signal handler should only call async signal-safe routines
   // _exit() is one, but exit() is not
#ifdef COVERAGE
   __gcov_flush();
#endif
   _exit(0);
}

void initControlPipe()
{
   controlPipe[0] = controlPipe[1] = -1;
}

void setupTerminationHandler()
{
   struct sigaction action;
   action.sa_handler = terminationHandler;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   sigaction (SIGINT, &action, NULL);
   sigaction (SIGTERM, &action, NULL);
}

void handleExitStatus(int status, pid_t childPid)
{
   if (WIFSIGNALED(status)) {
      printPrefix();
      cerr << "SciDB child (pid="<<childPid<<") terminated by signal = "
           << WTERMSIG(status) << (WCOREDUMP(status)? ", core dumped" : "")
           << endl;
   }
   if (WIFEXITED(status)) {
      printPrefix();
      cerr << "SciDB child (pid="<<childPid<<") exited with status = "
           << WEXITSTATUS(status)
           << endl;
   }
}

void runWithWatchdog()
{
   setupTerminationHandler();

   uint32_t forkTimeout = 3; //sec
   uint32_t backOffFactor = 1;
   uint32_t maxBackOffFactor = 32;

   printPrefix("Started.\n");

   while (true)
   {
      checkPort();
      setupControlPipe();

      time_t forkTime = time(NULL);
      assert(forkTime > 0);

      pid_t pid = ::fork();

      if (pid < 0) { // error
         handleFatalError(errno,"fork() failed");
      } else if (pid > 0) { //parent

         // close the read end of the pipe
         close(controlPipe[0]);
         controlPipe[0] = -1;

         int status;
         pid_t p = wait(&status);
         if (p == -1) {
            handleFatalError(errno,"wait() failed");
         }

         handleExitStatus(status, pid);

         time_t exitTime = time(NULL);
         assert(exitTime > 0);

         if ((exitTime - forkTime) < forkTimeout)
         {
            sleep(backOffFactor*(forkTimeout - static_cast<uint32_t>((exitTime - forkTime))));
            backOffFactor *= 2;
            backOffFactor = (backOffFactor < maxBackOffFactor) ? backOffFactor : maxBackOffFactor;
         } else {
            backOffFactor = 1;
         }

      }  else { //child

         //close the write end of the pipe
         close(controlPipe[1]);
         controlPipe[1] = -1;

         // connect stdin with the read end
         if (dup2(controlPipe[0], STDIN_FILENO) != STDIN_FILENO) {
            handleFatalError(errno,"dup2() failed");
         }
         if (controlPipe[0] != STDIN_FILENO) {
            close(controlPipe[0]);
            controlPipe[0] = -1;
         }

         runSciDB();
         assert(0);
      }
   }
   assert(0);
}

int main(int argc,char* argv[])
{
    RTLog::log("Proof that RTLog::log() is ready for debugging in all scidb builds at any time");

    // Force the ___Dscriptor constructor to be linked.
    // Because scidb does not invoke the constructor the linker optimizes
    // it out.  However, the plugin libraries need the constructor.
    static NamespaceDesc      forceNSD;
    static UserDesc           forceUD;
    static RoleDesc           forceRD;

    struct LoggerControl : boost::noncopyable, scidb::stackonly
    {
       ~LoggerControl()
        {
            log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getOff());
        }
    }   loggerControl;


    try
    {
        earlyInitMathLibEnv();  // environ changes must precede multi-threading.
    }
    catch(const std::exception &e)
    {
        printPrefix();
        cerr << "Failed to initialize math lib environ: " << e.what() << endl;
        scidb::exit(1);
    }

    // need to adjust sigaction SIGCHLD ?
   try
   {
       initConfig(argc, argv); // parse config file
   }
   catch (const std::exception &e)
   {
      printPrefix();
      cerr << "Failed to initialize server configuration: " << e.what() << endl;
      scidb::exit(1);
   }
   Config *cfg = Config::getInstance();

   if (cfg->getOption<bool>(CONFIG_DAEMON_MODE))
   {
      if (daemon(1, 0) == -1) {
         handleFatalError(errno,"daemon() failed");
      }
      // STDIN is /dev/null in a daemon process,
      // we need to fake it out in case we run without the watchdog
      initControlPipe();
      close(STDIN_FILENO);
      setupControlPipe();
      if (controlPipe[0]==STDIN_FILENO) {
          close(controlPipe[1]);
          controlPipe[1]=-1;
      } else {
          assert(controlPipe[1]==STDIN_FILENO);
          close(controlPipe[0]);
          controlPipe[0]=-1;
      }
   } else {
       initControlPipe();
   }

   if(cfg->getOption<bool>(CONFIG_REGISTER) ||
      cfg->getOption<bool>(CONFIG_NO_WATCHDOG)) {
      runSciDB();
      assert(0);
      scidb::exit(1);
   }
   runWithWatchdog();
   assert(0);
   scidb::exit(1);
}
