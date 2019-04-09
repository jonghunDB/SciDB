#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2016-2018 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#
import getpass
from scidblib import AppError
from ConfigParser import RawConfigParser

_DBG = False

def printDebug(*args):
   if _DBG:
      _log_to_stderr("DEBUG:", *args)

# The options below either require special handling or apply only to scidb.py
# (this script) only.
non_cmdline_options = {
    'malloc_check_':    False, # For scidb.py (causes env. variable changes).
    'tcmalloc':         False, # For scidb.py (causes env. variable changes).
    'preload':          False, # For scidb.py (changes LD_PRELOAD env. variable).
    'purge-days':       False, # For scidb.py.
    'logconf':          True,  # Used during init sub-command.
    'malloc_arena_max': False, # For scidb.py (env. variable changes).
    'db_user':          True,  # For command line, but requires special handling.
    'db_passwd':        False, # Backward compatibility.  Ignored.
    'db_port':          False, # Port for Postgres communications.
    'db_host':          False, # Host for Postgres communications.
    'pgpassfile':       False, # Location of pgpass file.
    'base-path':        True,  # For command line, but requires special handling.
    'base-port':        True,  # For command line, but requires special handling.
    'interface':        False, # Looks like a legacy option: probably not used.
    'gcov_prefix':      False, # For scidb.py (causes env. variable changes).
    'ssh-port':         False, # SSH port for communicating with the cluster hosts.
    'pg-port':          False, # Port for Postgres communications. DEPRECATED.
    'key-file-list':    False  # Key file for the SSH communications.
}

# This horrible violation of the SPOT principle brought to you by ticket #3533.
# Table with scidb command line options.  Each line below lists the name of
# the command line option and the flag indicating whether or not it is a
# required option.  The table immediately below lists options that appear on
# scidb command line in "--option=value" format.  if a required option is
# missing from config.ini, the script will error out.
scidb_cmdline_options = {
    'install_root':                  True,
    'pluginsdir':                    True,
    'chunk-reserve':                 False,
    'redundancy':                    False,
    'merge-sort-buffer':             False,
    'merge-sort-nstreams':           False,
    'merge-sort-pipeline-limit':     False,
    'redimension-chunksize':         False,
    'max-open-fds':                  False,
    'smgr-cache-size':               False,
    'mem-array-threshold':           False,
    'redim-chunk-overhead-limit-mb': False,
    'target-cells-per-chunk':        False,
    'target-mb-per-chunk'  :         False,
    'chunk-size-limit-mb':           False,
    'network-buffer':                False,
    'liveness-timeout':              False,
    'deadlock-timeout':              False,
    'execution-threads':             False,
    'client-queries':                False,
    'admin-queries':                 False,
    'operator-threads':              False,
    'result-prefetch-threads':       False,
    'result-prefetch-queue-size':    False,
    'sync-io-interval':              False,
    'io-log-threshold':              False,
    'max-memory-limit':              False,
    'small-memalloc-size':           False,
    'large-memalloc-limit':          False,
    'replication-send-queue-size':   False,
    'replication-receive-queue-size':False,
    'sg-send-queue-size':            False,
    'sg-receive-queue-size':         False,
    'requests':                      False,
    'load-scan-buffer':              False,
    'mpi-dir':                       False,
    'preallocate-shared-mem':        False,
    'materialized-window-threshhold':False,
    'data-dir-prefix':               False,
    'input-double-buffering':        False,
    'security':                      False,
    'autochunk-max-synthetic-interval': False,
    'stats-query-history-size':      False,
    'pooled-allocation':             False,
    'perf-wait-timing':              False,
    'filter-use-rtree':              False,
    'storage-log-enable':            False,
    'io-paths-list':                 False,
    'client-auth-timeout':           False,
    'pam-options':                   False,
    'max-arena-page-size':           False,
    'swap-temp-metadata':            False,
    'new-db-array':                  False,
    'datastore-punch-holes':         False,
}

# Same table as above, except these options are boolean flags.  That is, they
# appear on scidb command line simply as --option.  Note that if a boolean flag
# appears on the command line as --option=True or --option=False, then it has
# to be treated as a regular option and should be placed in the table of
# regular options above (scidb_cmdline_options).
scidb_cmdline_bool_options = {
    'enable-delta-encoding':         False,
    'daemon-mode':                   False,
    'no-watchdog':                   False,
    'enable-catalog-upgrade':        False,
    'enable-chunkmap-recovery':      False,
    'skip-chunkmap-integrity-check': False,
    'window-old-or-new':             False,
    'resource-monitoring':           False
    }

def cmd_line_option_to_switch(option,value):
    """ Turn an option into actual command line switch for scidb.
        @param option string representing config.ini option
        @param ctx
        @return string form of the actual command line switch for scidb
    """
    switch = ''
    if not option:
        return switch
    # If boolean option, then return "--option".
    if option in scidb_cmdline_bool_options.keys():
        # If not "true" or "on", then "" will be returned.
        if (str(value).lower() in ['true','on','yes']):
            switch = '--' + option
    else: # It is a "value" option: return "--option=value".
        val = str(value)
        if (val.lower() in ['false','off','no']):
            val = 'False'
        switch = '--' + option + '=' + val
    return switch

def filter_options_for_command_line(all_options, op):
    """ Pre-compute the subset of specified options that can appear on scidb
        command line.
        @param all_options dictionary of all possible options known to this
               script
        @param op option to filter
        @return None if op is the data prefix option, server option,
                or any other option unknown to this script; else op
    """
    if (op in non_cmdline_options.keys()):
       return None # Ignore script, special handling, and other options.
    if ('data-dir-prefix' in op):
       return None # Ignore data prefix options.
    if ('server-' in op):
       return None # Ignore server options.
    if (op not in all_options.keys()):
        return None # Ignore unknown options.
    return op

def setup_scidb_options(ctx):
    """ Pre-compute all options table and actual command line option strings
        for scidb.  These tables will be accesible through the global settings
        object ctx.
        @param ctx global object with script settings; config.ini settings
               should be parsed and present in the ctx object when this
               function is called.
        @return a dictionary comprised of all possible options (script and
                config.ini) known to this script and a list of command line
                switches for starting scidb corresponding to options specified
                by the user.
    """
    all_options = dict( # Combine all options into one table.
        non_cmdline_options.items() + \
        scidb_cmdline_bool_options.items() + \
        scidb_cmdline_options.items()
        )
    # Filter out everything except options that actually go onto the command
    # line.
    # Record the actual command line switches to used on scidb command line.
    option2CommandLineArgFunc = lambda opt_val: \
             cmd_line_option_to_switch(filter_options_for_command_line(all_options,opt_val[0]), opt_val[1])
    scidb_start_switches = [ x for x in map(option2CommandLineArgFunc, ctx._configOpts.iteritems()) if x ]

    return all_options,scidb_start_switches

def parseServerInstanceIds(instanceList):
   '''
   Parse the instance list as specified in a config.ini file.
   I.e. server-<sid>=<host>,<instance_list>
   where instance_list is of the form: 'n,m-p,q-s, ...'
   @param instanceList pre-split instance_list, i.e.  [ 'n','m-p','q-s', ...]
   @return a list of integer ranges [ [0,1,...n], [m,m+1,..p], [q,q+1,...s], ...]
   Checks that 0<n<m<p<q<s ...
   '''
   instances = []
   ranges = [r.split("-") for r in instanceList]
   isFirst = True
   last = 0
   for r in ranges:
      if isFirst and len(r) == 1:
         left = 0
         right = int(r[0])
      elif len(r) == 2:
         left = int(r[0])
         right = int(r[1])
      else:
         raise AppError("Invalid server entry")

      isFirst = False

      if left > right:
         raise AppError("Invalid server instance range in server entry")
      elif left < last:
         raise AppError("Duplicate server instances in server entry")
      else:
         last = right + 1
      instances.extend(range(left, last))
   return instances

class ServerEntry:
   '''
   Representation of a parsed server entry in config.ini
   '''
   def __init__(self, sid, host, instance_list):
      self._sid = int(sid)
      self._host = host
      try:
         self._instances = parseServerInstanceIds(instance_list)
      except AppError as e:
         raise AppError("%s for host: %s with server-id: %d" % (str(e), host, sid))

   def __str__(self):
     return ("%d, %s, %s") % (self._sid, self._host, str(self._instances))

   def toConfigOpt(self):
      '''
      Return a string representation suitable for specifying config.ini
      I.e. server-<sid>=<host>,<instance_list>, where instance_list is 'n,m-p,q-s, ...'
      '''
      ranges = []
      r = None
      printDebug("toConfigOpt: "+str(self))
      for i in self._instances:
         if r is None:
            r=[i,i]
         elif i-r[1] > 1:
            ranges.append(r)
            r=[i,i]
         else:
            r[1]=i

      assert r is not None , "Unexpected None instance range"
      ranges.append(r)

      key = "server-%d" % (self._sid)
      val = self._host
      for rg in ranges:
         val = val + (",%d-%d" % (rg[0],rg[1]))

      printDebug("toConfigOpt: %s = %s"%(key,val))
      return (key,val)

   def addInstances(self, srv):
      '''
      Add instances from srv to this entry
      Checks for duplicates.
      '''
      if self._sid != srv.getServerId():
         raise AppError("Cannot add instances. Server IDs are different %d!=%d" % \
                        (self._sid,srv.getServerId()))

      tempInstances = set(self._instances)

      for i in srv.getServerInstances():
         if i in tempInstances:
            raise AppError("Duplicate instance %d" % (i))
         else:
            tempInstances.add(i)
      self._instances = sorted([i for i in tempInstances])

   def removeInstances(self, srv):
      if self._sid != srv.getServerId():
         raise AppError("Cannot remove instances. Server IDs are different %d!=%d" % \
                        (self._sid,srv.getServerId()))

      tempInstances = set(self._instances)
      for i in srv.getServerInstances():
         if i in tempInstances:
            tempInstances.remove(i)
         else:
            raise AppError("Cannot remove non-existent instance %d" % (i))
      self._instances = sorted([i for i in tempInstances])

   def __lt__(self, other):
      return self._sid < other._sid

   def __gt__(self, other):
      return self._sid > other._sid

   def getServerId(self):
      return self._sid

   def getServerHost(self):
      return self._host

   def getServerInstances(self):
      return self._instances

class Context:
   def __init__(self,
                config_file='',
                scidb_name='',
                srvList=[],
                configOpts=None,
                coordSrvId = 0,
                installPath='',
                baseDataPath='',
                dataDirPrefix='',
                basePort = 1239,
                sshPort = 22,
                pgPort = 5432,
                pguser = None,
                pgdb = None,
                pgpassfile = None,
                keyFilenameList = None,
                args = None):

      self._config_file = config_file
      self._scidb_name = scidb_name
      self._srvList = srvList
      if  configOpts:
         self._configOpts = configOpts
      else:
         self._configOpts = {}
      self._coordSrvId = coordSrvId
      self._installPath = installPath
      self._baseDataPath = baseDataPath
      self._dataDirPrefix = dataDirPrefix
      self._basePort = basePort
      self._sshPort = sshPort
      self._pgPort = pgPort
      self._pguser = pguser
      self._pgdb = pgdb
      self._pgpassfile = pgpassfile
      if keyFilenameList:
         self._keyFilenameList = keyFilenameList
      else:
         self._keyFilenameList = []
      if args:
         self._args = args
      else:
         self._args = []
      self._scidb_start_switches = {}
      self._all_options = {}
      printDebug("#### Context created by user", getpass.getuser())

   def __str__(self):
      return str(self.__dict__)

def parseConfig(ctx):
   '''
   Parse config.ini file
   '''
   config = RawConfigParser()
   try:
      printDebug("Parsing config file = %s" %(ctx._config_file))
      config.readfp(open(ctx._config_file, 'r'))
      # If _scidb_name has not been assigned, use the first section name as the dbname.
      if ctx._scidb_name=='':
         ctx._scidb_name=config.sections()[0]
   except Exception, e:
      raise AppError("Cannot read config file: %s" % e)
   section_name = ctx._scidb_name
   # Check for upper case letters in database name.
   if not section_name.islower():
      raise AppError("Invalid specification for database name = %s; uppercase letters are not allowed!" %
                     section_name)

   # First process the "global" section.
   try:
      ctx._srvList=[]
      srvIdSet = set()

      for (key, value) in config.items(section_name):
         ctx._configOpts[key] = value

         # make a srv & instance list
         # format: server-N=ip(,n)|(,m-p) [,q-s]  number of local workers
         if key.startswith('server-'):
            srvId = int(key.split('-')[1])
            valueSplit = value.split(',')
            if len(valueSplit) < 2:
               raise RuntimeError("Invalid server specification for server %s = %s" % (key, value))
            host = valueSplit[0]
            if srvId in srvIdSet:
               raise RuntimeError("Duplicate server specification for server %s = %s" % (key, value))
            else:
               printDebug("Adding server-id=%d" % (srvId))
               srvIdSet.add(srvId)
            ctx._srvList.append(ServerEntry(srvId,host,valueSplit[1:]))

         # Check for upper case letters in db_user and db_passwd entries.
         if (key in ['db_user']):
            if (not value.islower()):
               raise RuntimeError("Invalid specification for %s = %s; uppercase letters are not allowed!" % (key, value))
   except Exception, e:
      raise AppError("config file parser error in file %s: %s" % (ctx._config_file, e))

   ctx._srvList.sort(key=lambda entry: entry.getServerId())
   ctx._configOpts['db_name'] = ctx._scidb_name
   # Pre-compute all options and command line switches to start scidb.
   ctx._all_options,ctx._scidb_start_switches = setup_scidb_options(ctx)
