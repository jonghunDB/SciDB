#!/usr/bin/python

# Initialize, start and stop scidb in a cluster.
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2015-2018 SciDB, Inc.
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
import argparse
import array
import datetime
import errno
import fcntl
import functools
import getpass
import itertools
import os
import paramiko
import pwd as pw
import random
import re
import select
import signal
import socket
import string
import struct
import subprocess
import sys
import textwrap
import time
import traceback

from ConfigParser import RawConfigParser
from glob import glob
from scidblib.pgpass_updater import (PgpassUpdater, PgpassError, I_PASS)
from scidblib.psql_client import Psql
from scidblib.util import getVerifiedPassword
from scidblib.iquery_client import IQuery
from scidblib import AppError

from scidb_config import (cmd_line_option_to_switch,
                          filter_options_for_command_line,
                          setup_scidb_options,
                          parseServerInstanceIds,
                          ServerEntry,
                          Context,
                          parseConfig)

_DBG = False                    # Debug flag
_PGM = None                     # Name of this script

class RemoteAppError(AppError):
   """Raised for problems when manipulating a remote instance.

   For other problems that halt script execution, raise plain AppError.
   """
   pass

def _log_to_stderr(*args):
   # Stderr is line-buffered by default, no need to flush line-oriented output.
   print >>sys.stderr, _PGM, ' '.join(map(str, args))

def printError(*args):
   _log_to_stderr("ERROR:", *args)

def printWarn(*args):
   _log_to_stderr("WARNING:", *args)

def printDebug(*args):
   if _DBG:
      _log_to_stderr("DEBUG:", *args)

def printInfo(*args):
   _log_to_stderr("INFO:", *args)

def removeInstDir(srv, liid):
   '''
   Remove the data directory for a given instance
   '''
   who = validateInstance(srv, liid)

   ldir = getInstanceDataPath(srv, liid)
   assert ldir, "No instance data path!"

   printInfo("Removing data directory %s on %s" % (ldir, who))
   if ldir and len(ldir) > ldir.count('/'):
      cmdList = [ 'rm', '-rf', ldir]
      try:
         executeIt(cmdList, srv, liid,
                   nocwd=True, useConnstr=False, useSSH4Local=True,
                   ignoreError=False, useShell=True)
      except OSError as e:
         if e.errno != errno.ENOENT:
            raise AppError("Cannot remove instance directory for %s: %s" % (who, e))
   else:
      printWarn(' '.join((
            "Not removing data directory %s on server %d (%s), local instance %d",
            "because it appears to be the root directory")) % (ldir, srv.getServerId(),srv.getServerHost(),liid))
      raise AppError("Unexpected data directory %s for %s" % (ldir, who))

def random_string(size=5):
   """ Return a pseudo-randomly selected string of specified size
       composed of letters and digits.
       @param size length (in characters) of the random string to
              produce
       @return string composed of letters and digist of specified
               size
   """
   return ''.join(
      random.sample(
         string.digits + string.letters,
         size
         )
      )

def get_required_opts(opts_dict):
    """ Filter all options and return a set of required ones.
        @param all_options set of all possible options
        @param opts_dict dictionary with all possible options and
                         their attributes
        @return set of required options (strings)
    """
    required_options = []
    for op in gCtx._all_options.keys():
        if (gCtx._all_options[op]):
            required_options.append(op)
    return set(required_options)

def validate_config_init_settings(opts_dict):
    """ Perform a simple check on the config.ini options specified
        by the user.  Config.ini settings will be checked against
        a subset (an ever-growing subset) of available options.

        @param opts_dict dictionary with the parsed settings
    """
    ignore_specified_opts = set([
        'db_name' # This option is not in config.ini: it is derived from the cluster name.
        ])

    specified_options = set(opts_dict.keys())
    all_options = set(gCtx._all_options.keys())

    specified_options = specified_options - ignore_specified_opts

    required_options = get_required_opts(opts_dict)

    # Check if any of the required options are missing:
    missing_required_opts = required_options - (required_options & specified_options)
    if missing_required_opts:
        missing_list = ['Following required config.ini options are not specified:']
        missing_list.extend(missing_required_opts)
        raise AppError('\n'.join(missing_list))

    # Remove data dir prefix and server-N option(s):
    data_dir_options = []
    server_options = False
    unknown_options = []
    for op in specified_options:
        if ('data-dir-prefix' in op):
            data_dir_options.append(op)
            continue
        if ('server-' in op):
            server_options = True
            continue
        if op.startswith('vg-'):
           # Silence complaints re. valgrind options, there are too many to validate.
           continue
        if (op not in all_options):
            unknown_options.append(op)

    if unknown_options:
        unknown_list = list(unknown_options)
        unknown_list.sort()
        unknown_list = ['Unsupported options found in config.ini:'] + unknown_list
        printWarn('\n'.join(unknown_list))

    # Validate server name options.
    # First, check that servers are specified:
    if not server_options:
        raise AppError('No servers specified in config.ini!')

    # Validate (if specified) data dir prefix options:
    if (len(data_dir_options) > 1 and 'data-dir-prefix' in data_dir_options):
        msg_list = ['Bad data-dir-prefix entries found!']
        msg_list.append(
            'Please specify either one data-dir-prefix entry or data-dir-prefix-X-Y for each instance!'
            )
        raise AppError('\n'.join(msg_list))
    else:
        data_dir_options = set(data_dir_options) - set(['data-dir-prefix'])

        data_prefixes = [ 'data-dir-prefix-%d-%d' % (srv.getServerId(),liid)
                          for srv in gCtx._srvList \
                             for liid in srv.getServerInstances() ]

        # Make sure that each instance has its own specified data dir prefix:
        if data_dir_options and (len(data_prefixes) != len(data_dir_options)):
            printWarn('Mismatch between data dir prefixes and total number of scidb instances in config.ini!')

        bad_dir_options = data_dir_options - set(data_prefixes)

        # If there are still options, raise an exception.
        if bad_dir_options:
            unknown_list = list(bad_dir_options)
            unknown_list.sort()
            unknown_list = ['Unsupported data dir prefix options found in config.ini:'] + unknown_list
            printWarn('\n'.join(unknown_list))

def get_ld_library_path(addVarName=True):
   """ Return value for environment variable LD_LIBRARY_PATH and
       optionally append LD_LIBRARY_PATH= to the beginning of
       the string.

       @param addVarName optional keyword parameter that indicates
                     if LD_LIBRARY_PATH= definition header should
                     be added to the return value
       @return path-like string with the value for the LD_LIBRARY_PATH
               environment variable
   """
   # Lib and lib/scidb/plugins were removed from LD_LIBRARY_PATH
   # because they are no longer required.  LD_LIBRARY_PATH variable
   # is defined on all commands sent to local and remote hosts.
   ld_lib_path = os.environ.get("LD_LIBRARY_PATH", "")

   if addVarName: # Add the definition header
      ld_lib_path = 'LD_LIBRARY_PATH=' + ld_lib_path

   return ld_lib_path


def getSrvDataPath(srv):
   '''
   Get the path for a srv (parent) directory
   '''
   path = os.path.join(gCtx._baseDataPath, str(srv.getServerId()))
   printDebug("Server data path for %s : %s" % (str(srv),path))
   return path


def getInstanceFS(srv, liid):
   '''
   Get the path for a specific instance
   '''
   validateInstance(srv, liid)

   perInstanceKey = "data-dir-prefix-%d-%d" % (srv.getServerId(),liid)
   if perInstanceKey in gCtx._configOpts.keys():
      return gCtx._configOpts[perInstanceKey]

   if gCtx._dataDirPrefix is None:
      return None

   perInstanceDataDir = "%s.%d.%d" % (gCtx._dataDirPrefix, srv.getServerId(), liid)
   return perInstanceDataDir


def getInstanceDataPath( srv, liid):
   '''
   Get the path for a specific instance
   '''
   validateInstance(srv, liid)
   path = os.path.join(getSrvDataPath(srv), str(liid))
   return path


def getInstanceCount(servers):
   nInstances = sum(map(lambda srv: len(srv.getServerInstances()), servers))
   return nInstances

# Get IP address of an interface
def get_ip_address(ifname):
   s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))[20:24])

def isLocalHost(hostname):
   '''
   Return true iff the hostname is the name/IP of the local machine
   '''
   s = None
   try:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      addr = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
      s.bind(addr[0][4])
      return True
   except:
      return False
   finally:
      if s: s.close()

# Get hold of the Postgres password, by hook or by crook.
# See http://www.postgresql.org/docs/9.3/interactive/libpq-pgpass.html .
def get_db_password(host, port, dbname, dbuser, pgpassfile):
   printDebug("### get_db_password:", '/'.join(map(str, [host, port, dbname, dbuser, pgpassfile])))
   pup = None
   try:
      pup = PgpassUpdater(pgpassfile)
   except PgpassError as e:
      printWarn("PgpassUpdater:", e)
   else:
       entry = pup.find(dbuser, dbname, host, port)
       if entry:
          return entry[I_PASS]
       printWarn("cannot find match for dbuser ", dbuser, " dbname ", dbname, " host", host, " port ", port);
   # OK, I guess we'll have to ask.
   dbpass = getVerifiedPassword(
      "Postgres user: {0}\nPostgres password [{0}]: ".format(dbuser))
   if not dbpass:
      dbpass = dbuser
   # Offer to store it...
   if pup and confirm("Update %s?" % pup.filename(), resp=True):
      pup.update(dbuser, dbpass, dbname, host, port)
      pup.write_file()
   return dbpass

# Connection string
def createConnstr(remote=False, want_password=False):
   host = gCtx._pgHost
   port = gCtx._pgPort
   dbname = gCtx._configOpts.get('db_name')
   dbuser = gCtx._configOpts.get('db_user')
   if want_password:
      password_clause = " password=%s" % gCtx._configOpts.get('db_passwd')
   else:
      password_clause = ""
   connstr = "host={0} port={1} dbname={2} user={3}{4}".format(
      host, port, dbname, dbuser, password_clause)
   if remote:
      connstr = ''.join(("'", connstr, "'"))
   return connstr

def runRemoteCommands(clients,cmds):
   channels = map(lambda client: client.get_transport().open_session(),clients)
   _ = map(lambda client: client.get_transport().set_keepalive(0),clients)
   # Put all channels in non-blocking mode: receive data operations will not block.
   _ = map(lambda channel: channel.settimeout(0),channels)
   _ = map(lambda chCmdZip: chCmdZip[0].exec_command(chCmdZip[1]),zip(channels,cmds))
   return channels

def trackRemoteCommandExecution(
    clients, # SSH connections.
    remoteChannels, # Channels obtained from the SSH connections.
    cmds, # Running commands.
    auto_close=False, # Flag to close remote channels in this function.
    wait=True, # Flag that indicates the function should wait for commands to exit.
    timeout=600, # Timeout value (in seconds) for the select call.
    read_max=128*1024 # Max number of bytes to read from channel buffer.
    ):
    #...............................................................
    read_max = int(read_max / 2)

    stdins = map(lambda x: x.makefile('wb', -1),remoteChannels)
    validStdins = [x for x in stdins if x]

    # Send shutdown to valid stdin descriptors and close them:
    map(lambda x: x.channel.shutdown_write(),validStdins)
    map(lambda x: x.close(), validStdins)

    # Set up flag lists for status-checking.
    nChans = len(remoteChannels)
    stdoutsDone = [False] * nChans
    stderrsDone = [False] * nChans
    statusDone = [False] * nChans

    # Set up lists for storing chunks of output text.
    stdoutsText = [[]] * nChans
    stderrsText = [[]] * nChans

    # Set up the list for storing command exit codes.
    exits = [-1] * nChans

    # No wait + auto_close: close clients (ssh connections).
    if not wait:
       if auto_close:
          map(lambda x: x.close(),clients)
       return exits, stdoutsText, stderrsText

    start_time = time.time()

    # Keep a dynamic list of active SSH channels.
    activeChannels = list(remoteChannels)

    while activeChannels:
       # Check which which channels have commands that exited.
       exitsStatus = [
          ((not statusDone[i]) and remoteChannels[i].exit_status_ready()) for i in range(len(remoteChannels))
          ]
       # Grab the exits for channels whose commands have exited.
       exits = [
          remoteChannels[i].recv_exit_status() if exitsStatus[i] else exits[i] \
             for i in range(len(exitsStatus))
          ]
       printDebug("exit codes (%s)" % exits)

       # Check "done" status for all channels.
       statusDone = [
          True if exitsStatus[i] else statusDone[i] \
             for i in range(len(exitsStatus))
          ]
       printDebug("done (%s)" % statusDone)

       # Collect stderr data for all channels.
       stderrsDone = map(
          lambda x: getStreamStatus(x[0],x[1],x[2],x[3],x[2].recv_stderr_ready,x[2].recv_stderr,read_max),
          zip(stderrsDone,statusDone,remoteChannels,stderrsText)
          )
       printDebug("stderr drained (%s)" % stderrsDone)

       # Collect stdout data for all channels.
       stdoutsDone = map(
          lambda x: getStreamStatus(x[0],x[1],x[2],x[3],x[2].recv_ready,x[2].recv,read_max),
          zip(stdoutsDone,statusDone,remoteChannels,stdoutsText)
          )
       printDebug("stdout drained (%s)" % stdoutsDone)

       # Reconstruct the list of active channels and remove the finished ones.
       activeChannels = [
          remoteChannels[i] for i in range(len(remoteChannels))
              if not statusDone[i] or not stdoutsDone[i] or not stderrsDone[i]
          ]
       printDebug("# active (%d)" % (len(activeChannels)))

       # Timeouts handling
       time2wait = int(start_time) + timeout - int(time.time())
       if (time2wait <= 0):
          timeoutIndices = [x for i in range(len(cmds)) if not statusDone[i]]
          for i in timeoutIndices:
             printError("remote_exec(%s) timed out" % cmds[i])
             stderrsText[i] += "\nexit_status("+str(exits[i])+") after timeout ("+str(timeout)+")"
          break

       # Wait for channels to become ready (for IO).
       if activeChannels:
          select.select(activeChannels,activeChannels,[],time2wait)

    map(lambda x: x.close(),stdins)

    if auto_close:
       map(lambda client: client.close(),clients)

    # Done; collapse the output chunks for every channel and return everything.
    return exits,[''.join(t) for t in stdoutsText],[''.join(t) for t in stderrsText]

def parallelRemoteExec(clients, cmds,
                       waitFlag=True,
                       ignoreError=False):

    printDebug("Executing in parallel %s" %(str(cmds)))

    cmds = [prepareRemoteShellCmd(cmd) for cmd in cmds]
    channels = runRemoteCommands(clients,cmds)
    try:
        exits,outputs,errors = trackRemoteCommandExecution(clients,channels,cmds,wait=waitFlag)
        if (waitFlag):
            abExitIndices = [i for i in range(len(exits)) if exits[i] != 0 ]
            if (len(abExitIndices) > 0):
                index = abExitIndices[0]
                raise RuntimeError("Abnormal return code: %s stderr: %s" % (exits[index],errors[index]))
    except Exception, e1:
        if _DBG:
            traceback.print_exc()
        printDebug('Remote command exceptions:\n%s\n%s' % (cmds, e1))
        if (not ignoreError):
            sshCloseNoError(clients)
            raise RemoteAppError('Remote command exceptions:\n%s' % e1)
    return exits,outputs,errors

# Run remote command over SSH
def remote_exec(client, lcmd, auto_close=False, wait=True, tmo=600, read_max=10*1024*1024):
        output = ''
        err = ''
        read_max = int(read_max/2)
        exit_status = -1

        chan = client.get_transport().open_session()
        client.get_transport().set_keepalive(0)
        chan.settimeout(0) # Make channel IO non-blocking.
        chan.exec_command(lcmd)

        stdin = chan.makefile('wb', -1)
        if stdin:
           stdin.channel.shutdown_write()
           stdin.close()
        # end if

        if (not wait):
                if (auto_close):
                   client.close
                # end if
                return (exit_status, output, err)
        # end if

        exits,outputs,errors = trackRemoteCommandExecution(
           [client], # SSH connections.
           [chan], # Channels obtained from the SSH connections.
           [lcmd], # Running commands.
           auto_close, # Flag to close remote channels in this function.
           wait, # Flag that indicates the function should wait for commands to exit.
           tmo, # Timeout value (in seconds) for the select call.
           read_max # Max number of bytes to read from channel buffer.
           )

        return (exits[0], outputs[0], errors[0])
# end def remote_exec
#.............................................................................
# getStreamStatus: returns whether or not a given stream has any more bytes
# in it along with the current (appended) output text.
def getStreamStatus(
        streamDone, # Flag indicating if this stream is finished or not.
        statusDone, # Flag indicating if the command run by the channel has finished.
        channel,     # I/O stream for reading.
        streamTextList, # Current text from the stream in string form.
        readyFunction, # Callable function to check if channel's stream has bytes in it for reading.
        receiveFunction,# Callable function to retrieve bytes from the channel stream buffer.
        read_max  # Maximum number of bytes to read from the stream.
        ):
        sDone = streamDone
        # ReadyFunction returns True only if there are actual bytes of data in
        # the channel's I/O pipes; on EOF readyFunction still returns False
        # which is why we rely on statusDone variable to check for EOF
        # condition.
        if (not streamDone) and (statusDone or readyFunction()):
           try:
              while True:
                 ret = receiveFunction(read_max)
                 if ret:
                    streamTextList.append(ret)
                 else:
                    sDone=True
                    break
           except socket.timeout: # Non-blocking recv call throws exception if there is no text in buffer.
              pass
        return sDone
#.............................................................................
# Use globals sshPort, keyFilenameList
def sshconnect( srv, username=None, password=None):
        sshc = paramiko.SSHClient()
        sshc.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
           sshc.connect(srv.getServerHost(), port=gCtx._sshPort,
                        username=username, password=password,
                        key_filename=gCtx._keyFilenameList, timeout=10)
        except Exception, s:
                if _DBG:
                   traceback.print_exc()
                raise RemoteAppError("ssh failure: server=%s port=%d %s" %
                                     (srv.getServerHost(), gCtx._sshPort, s))
        return sshc

def sshCloseNoError(conns):
   if not conns:
      return
   for con in conns:
      try: con.close()
      except: pass

def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.
    """
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    # end if
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        # end if
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        # end if
        if ans == 'y' or ans == 'Y':
            return True
        # end if
        if ans == 'n' or ans == 'N':
            return False
        # end if
    # end while

# end def confirm
## end of http://code.activestate.com/recipes/541096/ }}}

# Local/Remote Execution Module
# by the identity of the srv, it decides whether to run locally or remotely
# also if supplied with an existing connection, it uses it
def executeLocal( cmdList,
                  dataDir,
                  waitFlag=True,
                  nocwd=False,
                  useConnstr=True,
                  sout=None,
                  serr=None,
                  stdoutFile=None,
                  stderrFile=None,
                  useShell=False,
                  ignoreError=False
                  ):
        ret = 0
        out = ''
        err = ''

        if nocwd:
                currentDir = None
        else:
                currentDir = dataDir

        if sout is None:
                if stdoutFile is not None:
                        sout=open(dataDir+"/"+stdoutFile,"a+")
                elif not waitFlag:
                        sout=open("/dev/null","a")

        if serr is None:
                if stderrFile is not None:
                        serr=open(dataDir+"/"+stderrFile,"a+")
                elif not waitFlag:
                        serr=open("/dev/null","a")

        if useConnstr:
           connstr = createConnstr(remote=False, want_password=False)
           if useShell:
              connstr = ''.join(("'", connstr, "'"))
           cmdList.extend(['-c', connstr])

        # print 'Using modified cmdList: ', cmdList
        my_env = os.environ
        if gCtx._configOpts.get('malloc_check_'):
                my_env["MALLOC_CHECK_"] = gCtx._configOpts.get('malloc_check_')
                # end if
        if gCtx._configOpts.get('malloc_arena_max'):
                my_env["MALLOC_ARENA_MAX"] = gCtx._configOpts.get('malloc_arena_max')
                # end if
        if gCtx._configOpts.get('gcov_prefix'):
                my_env["GCOV_PREFIX"] = dataDir
                printInfo("gcov_prefix = %s" % (dataDir))
        # end if

        if (gCtx._configOpts.get('tcmalloc') in  ['true', 'True', 'on', 'On']):
                if "LD_LIBRARY_PATH" in my_env:
                        my_env["LD_LIBRARY_PATH"] = gCtx._installPath+"/lib:" + my_env["LD_LIBRARY_PATH"]
                else:
                        my_env["LD_LIBRARY_PATH"] = gCtx._installPath+"/lib:"
                        my_env["LD_PRELOAD"] = "libtcmalloc.so"
                        my_env["HEAPPROFILE"] = "/tmp/heapprof"
                # end if
        # end if

        executable=None
        if useShell:
           cmdList = ['source ~/.bashrc;', 'export PGPASSFILE=' + os.environ.get("PGPASSFILE","") + ';'] + cmdList
           cmdList=[" ".join(cmdList)]
           executable="/bin/bash"
        # end if
        try:
                #print currentDir, cmdList, sout, useShell
                p = subprocess.Popen(cmdList, env=my_env, cwd=currentDir,
                                     stderr=serr, stdout=sout, shell=useShell,
                                     executable=executable)
                if waitFlag:
                        p.wait()
                        ret = p.returncode
                        if ret != 0 :
                                raise RuntimeError("Abnormal return code: %s" % str(ret))
                        if (sout is not None):
                                sout.flush()
                                sout.seek(0)
                                out = sout.read()
                        if (serr is not None):
                                serr.flush()
                                serr.seek(0)
                                err = serr.read()
                 # end if
        except Exception, e1:
           printDebug(e1)
           if not ignoreError:
              printError("command %s: %s" % (" ".join(cmdList), e1))
              logs = ", ".join([x for x in (stderrFile, stdoutFile) if x])
              if logs:
                 printError("Check logs in", logs)
              raise AppError("executeLocal: %s" % e1)
        # end try
        return (ret, out, err)

# Make sure the command is executed by bash
def prepareRemoteShellCmd(cmdString):
   cmdString = cmdString.replace("'","\\'")
   cmdString = "exec /bin/bash -c $'" + \
       'source ~/.bashrc; export PGPASSFILE=' + os.environ.get("PGPASSFILE","") + '; ' + \
       cmdString + \
       "'"

   printDebug("Remote command="+cmdString)
   return cmdString

# Remote Execution
# by the identity of the srv, it decides whether to run locally or remotely
# also if supplied with an existing connection, it uses it
def executeRemote( cmdList, srv, liid,
                   waitFlag=True,
                   nocwd=False,
                   useConnstr=True,
                   sshc=None,
                   stdoutFile=None,
                   stderrFile=None,
                   ignoreError=False
                   ):
        ret = 0
        out = ''
        err = ''

        dataDir = getInstanceDataPath(srv,liid)
        if nocwd:
                currentDir = None
        else:
                currentDir = dataDir
        # end if

        needsClose = False
        if sshc is None:
                sshc = sshconnect( srv)
                needsClose = True
        # end if
        if useConnstr:
                cmdList.append('-c')
                cmdList.append(createConnstr(remote=True, want_password=False))
        # end if
        if stdoutFile is not None:
                cmdList.append('1>')
                cmdList.append(stdoutFile)
        # end if
        if stderrFile is not None:
                cmdList.append('2>')
                cmdList.append(stderrFile)
        # end if

        if currentDir:
                cmdString = "cd "+currentDir+";"+(" ".join(cmdList))
        else:
                cmdString = " ".join(cmdList)
        # end if

        cmdString = prepareRemoteShellCmd(cmdString)

        try:
                (ret,out,err) = remote_exec(sshc, cmdString, wait=waitFlag)
                if needsClose:
                        sshc.close()
                # end if
                if waitFlag:
                   if ret != 0 or err :
                      raise RuntimeError("Abnormal return code: %s stderr: %s" % (str(ret),err))

        except Exception, e1:
           error = "Remote command exception:\n%s\n%s" % (cmdString, e1)
           printDebug(error)
           if not ignoreError:
              if needsClose:
                 sshCloseNoError([sshc])
              raise RemoteAppError(error)
           # end if
        # end try
        return (ret, out, err)

# Local/Remote Execution Module
# by the identity of the srv, it decides whether to run locally or remotely
# also if supplied with an existing connection, it uses it
def executeIt( cmdList, srv, liid,
               waitFlag=True, nocwd=False,
               useConnstr=True, executable=None,
               sshc=None, stdoutFile=None, stderrFile=None,
               useSSH4Local=True,  #XXX TODO: to be removed
               useShell=False,     #XXX TODO: to be removed
               ignoreError=False):

      return executeRemote(cmdList, srv, liid,
                           waitFlag=waitFlag,
                           nocwd=nocwd,
                           useConnstr=useConnstr,
                           sshc=sshc,
                           stdoutFile=stdoutFile,
                           stderrFile=stderrFile,
                           ignoreError=ignoreError)


def binFile( srv, liid):
   '''
   Return instance specific SciDB binary name.
   '''
   validateInstance(srv, liid)
   return "SciDB-%d-%d-%s"%(srv.getServerId(),liid,gCtx._scidb_name)


def validateInstance(srv, liid):
   '''
   Check liid is in srv's set of instances.
   Return human-readable description of an instance.
   '''
   who = "(server %d (%s) local instance %d)" % (srv.getServerId(),srv.getServerHost(),liid)
   if not liid in srv.getServerInstances():
      raise AppError("Invalid instance %s" % (who))
   return who


def initAll(force=False):
   '''
   Initialize dirs and links and initialize/register all instances.
   Assumes that syscat init script was already run.
   '''
   if not gCtx._srvList:
      raise AppError("No servers specified")

   if (check_scidb_running() > 0):
      raise AppError("SciDB is still running.")

   try:
      checkMaxPostgresConns(getInstanceCount(gCtx._srvList))
   except Exception as pgException:
      if not force:
         raise AppError("Postgres exception: %s" % pgException)
      printWarn(pgException)

   checkRedundancy(len(gCtx._srvList))

   initSomeInParallel(gCtx._srvList[0:1], force=force, initialize=True)

   if len(gCtx._srvList)>1:
      initSomeInParallel(gCtx._srvList[1:], force=True, initialize=False)


def removeInstDirCmd(srv, liid):
   '''
   Return shell command to remove the data directory for an instance.
   '''
   who = validateInstance(srv, liid)

   ldir = getInstanceDataPath(srv, liid)
   assert ldir, "No instance data path!"

   cmdList = []
   printInfo("Removing data directory %s on %s" % (ldir, who))
   if ldir and len(ldir) > ldir.count('/'):
      cmdList = [ 'rm', '-rf', ldir]
   else:
      printWarn(' '.join((
            "Not removing data directory %s on server %d (%s), local instance %d",
            "because it appears to be the root directory")) % (ldir, srv.getServerId(),srv.getServerHost(),liid))
      raise AppError("Unexpected data directory %s for %s" % (ldir, who))

   return " ".join(cmdList)


def createDirsAndLinksCmd(srv, liid):
   '''
   Create directories and link the scidb binary if they dont already exist.
   '''
   validateInstance(srv,liid)

   ndir = getSrvDataPath(srv)
   allCmds = []
   # create the directories for this instance
   cmdList = ['mkdir', '-p', ndir]
   allCmds.append(" ".join(cmdList))

   ddir = getInstanceFS(srv, liid)
   ldir = getInstanceDataPath( srv, liid)

   # create/link directories and add symlink for executable
   if ddir:
      # ddir must be empty and must exist
      cmdList = ['ls', ddir+'/*', '1>/dev/null', '2>/dev/null', '&&', 'exit', '2', ';',
                 'ls', ddir+'/', '1>/dev/null']
      allCmds.append(" ".join(cmdList))
      cmdList = ['ln', '-T', '-s', ddir, ldir]
      allCmds.append(" ".join(cmdList))
   else:
      cmdList = ['mkdir', ldir]
      allCmds.append(" ".join(cmdList))

   cmd = ' && '.join(allCmds)
   cmd = ' && '.join([cmd, makeRelinkBinaryCmd(srv,liid)])
   return cmd


def init(srv, liid, initialize=False, online=None, cmdOnly=False):
   '''
   Initialize and register an instance.
   '''
   who = validateInstance (srv,liid)
   printInfo("init %s"%(who))
   printInfo("Initializing local scidb instance/storage.\n")

   linkCmd = createDirsAndLinksCmd(srv, liid)

   cdCmd = 'cd ' + getInstanceDataPath(srv,liid)

   cmdList = [gCtx._installPath + "/bin/scidb", "--register"]

   if initialize:
      initFlag = "--initialize"
      cmdList.extend(["-p", str(gCtx._basePort+liid), initFlag])
   else:
      cmdList.extend(["-p", str(gCtx._basePort+liid)])

   logconf = gCtx._configOpts.get('logconf')
   cmdList.extend(["-i", srv.getServerHost(),
                   "-s", getInstanceDataPath(srv,liid) + '/storage.cfg17',
                   "--logconf", logconf])
   if online:
      cmdList.extend(["-o", str(online)])

   if gCtx._configOpts.get('enable-delta-encoding') in ['true', 'True', 'on', 'On']:
      deltaClause = "--enable-delta-encoding"
      cmdList.extend([deltaClause])

   if gCtx._configOpts.get('daemon-mode') in ['true', 'True', 'on', 'On']:
      daemonClause = "--daemon-mode"
      cmdList.extend([daemonClause])

   if gCtx._configOpts.get('chunk-reserve'):
      reserveClause = "--chunk-reserve=%s" % gCtx._configOpts.get('chunk-reserve')
      cmdList.extend([reserveClause])

   if gCtx._configOpts.get('install_root'):
      installPathClause = "--install_root=%s" % gCtx._configOpts.get('install_root')
      cmdList.extend([installPathClause])

   cmd = ''
   if cmdOnly:
      cmdList.extend(['-c',createConnstr(remote=True, want_password=False)])
      cmdList.extend(['1>init-stdout.log','2>init-stderr.log'])
      cmd = ' && '.join([linkCmd, cdCmd, " ".join(cmdList)])
   else:
      tmpList = cmdList
      cmdList = [linkCmd, '&&', cdCmd, '&&']
      cmdList.extend(tmpList)
      executeIt(cmdList, srv, liid,
                stdoutFile="init-stdout.log", stderrFile="init-stderr.log",
                nocwd=True, useShell=True, useSSH4Local=True)
   return cmd

#XXX TODO: to be removed
def initSomeSerially(servers, instances=None, force=False, remove=True, initialize=False, online=None):
   '''
   Initialize dirs and links and initialize/register all instances. It assumes
   that syscat init script was already run.
   '''
   if not force and remove:
      if not confirm('WARNING: This will delete all data and reinitialize storage', False):
         raise AppError("Reinitialization aborted.")

   instances = applyInstanceFilter(servers, instances, nonesOK=False)

   for srv,liids in zip(servers,instances):
      for liid in liids:
         if remove:
            printInfo("Cleaning up old logs and storage files.")
            removeInstDir(srv, liid)

         init(srv,liid,initialize=initialize,online=online)
         if initialize:
            initialize=False


def initSomeInParallel(servers, instances=None,
                       sshConnections=None,
                       force=False, remove=True,
                       initialize=False, online=None):
   '''
   Initialize dirs and links and initialize/register the given instances. It assumes
   that syscat init script was already run. The sets of commands on each server
   are executed in parallel (but each set sequentially).
   '''
   if not force and remove:
      if not confirm('WARNING: This will delete all data and reinitialize storage', False):
         raise AppError("Re-initialization aborted.")

   instances = applyInstanceFilter(servers, instances, nonesOK=False)

   allCmds = []
   for srv,liids in zip(servers,instances):
      srvCmds = []
      for liid in liids:
         iCmds=[]
         if remove:
            printInfo("Cleaning up old logs and storage files.")
            iCmds.append(removeInstDirCmd(srv, liid))

         iCmds.append(init(srv,liid,initialize=initialize,online=online,cmdOnly=True))
         if initialize:
            initialize=False
         srvCmds.append(' && '.join(iCmds))
      allCmds.append(srvCmds)

   allCmds = [' && '.join(x) for x in allCmds]

   printDebug("init command: %s" % str(allCmds))

   sortedSrvList = sorted(servers,key=lambda s: s.getServerId()) #XXXX needed ?
   assert len(sortedSrvList)>0, "No servers specified"

   closeConns = False
   if not sshConnections:
      sshConnections = []
      closeConns = True
   elif len(sshConnections) != len(sortedServerList):
      raise AppError("Number of connection is not equal to the number of remote servers")

   try:
      # Establish ssh connections with all servers (possibly including the
      # local machine).
      if closeConns:
         sshConnections = [sshconnect( x) for x in sortedSrvList]

      # Run all of the commands.
      exits,outputs,errors = parallelRemoteExec(sshConnections,allCmds,waitFlag=True)

      if closeConns:
         map(lambda x: x.close(),sshConnections)
   finally:
      # Close down all of the ssh connections.
      if closeConns:
         sshCloseNoError(sshConnections)

def isCoordinator(srv, liid):
   coordinator = gCtx._srvList[gCtx._coordSrvId] #coordinator
   return  (srv.getServerId() == coordinator.getServerId() and
            liid == coordinator.getServerInstances()[0])

def scidb_error(error_text):
   """Parse error text into short and long error code names.

   @param error_text string containing SciDB error message
   @return ('SCIDB_SE_FOO', 'SCIDB_LE_BAR') from error_text, or (None, None)
   """
   serr = None
   lerr = None
   m = re.search(r'\b(SCIDB_SE_[A-Z0-9_]+)', error_text, re.MULTILINE)
   if m:
      serr = m.group(1)
   m = re.search(r'\b(SCIDB_LE_[A-Z0-9_]+)', error_text, re.MULTILINE)
   if m:
      lerr = m.group(1)
   return serr, lerr

def check_scidb_ready(srv, liid):
   '''
   Check if a query can be executed to confirm that scidb is up.
   '''
   iquery = IQuery(afl=True,
                   no_fetch=True,
                   host=srv.getServerHost(),
                   port=(gCtx._basePort + liid),
                   prog=os.path.join(gCtx._installPath, "bin/iquery"))
   if gCtx._args.auth_file:
      iquery.auth_file = gCtx._args.auth_file

   # listing queries should be very cheap
   _, err = iquery("list('queries')")

   # Authentication problems aren't solved by retrying.
   if iquery.returncode:
      serr, lerr = scidb_error(err)
      if serr == 'SCIDB_SE_AUTHENTICATION':
         raise AppError("Authentication failed for iquery, "
                        "SciDB was started but might not be ready!\n%s" % err)
      if _DBG:
         printWarn("SciDB not ready yet:\n", err)

   return iquery.returncode == 0


def applyInstanceFilter(servers, instances, nonesOK):
   '''
   Construct and/or checks a (sub)set of server instances to be used for some operation.
   instances is a list of instances for each server,
   which must be a valid subset of the actual server instances.
   If instances is None and nonesOK == True, a list of Nones is returned, one for each server.
   If instances is None and nonesOK == False, a list of instance lists is returned, one list for each server.
   '''
   if instances is None:
      if nonesOK:
         instances = [None for srv in servers]
      else:
         instances = [srv.getServerInstances() for srv in servers]
      return instances

   if not instances or len(servers) != len(instances):
      raise AppError("Invalid number of instance filters")

   for srv,liids in zip(servers,instances):
      if not set(liids).issubset(set(srv.getServerInstances())):
         raise AppError("Invalid instance filter for server %d (%s)" %
                        (srv.getServerId(),srv.getServerHost()))
   return instances


def check_scidb_running(
   sshConns=None, # Optional list of ssh connections to servers.
   servers=None,  # Optional list of servers (must match connections).
   instances=None ): # [[instances for srv X], [instances for srv Y] , ...]
                     # ordered by server (i.e. X<Y<...); len(instances)==len(servers)
   '''
   Check if scidb is running on the specified servers.
   Return number of SciDB processes found.
   '''
   if sshConns and not servers:
      raise AppError("Connections do not match servers")

   if not servers:
      servers = gCtx._srvList

   # Prepare some default return values.
   pids = []
   c = 0
   # Sort the server list: currently, passed in servers must be
   # sorted in the exact same way; otherwise their optional ssh
   # connection objects will be mismatched.
   sortedSrvList = sorted(list(servers),key=lambda srv: srv.getServerId())
   instances = applyInstanceFilter(sortedSrvList, instances, nonesOK=True)

   # Prepare the ssh connections.
   needToCloseSSH = False
   if not sshConns:
      needToCloseSSH = True
      sshConns=[]

   try:
      if (needToCloseSSH):
         sshConns = [sshconnect( srv) for srv in sortedSrvList]

      if len(sshConns) != len(servers) or len(sshConns) != len(sortedSrvList):
         raise AppError("Connections don't match servers %d != %d" % (len(sshConns),len(servers)))

      # Gather a list of pid-collection commands for every server.
      random_prefix = 'scidb_' + random_string(10)
      cmds = [getAllScidbPidsCmd(srv, instances=liids, prefix=random_prefix)
              for srv,liids in zip(sortedSrvList,instances)]

      # Run all of the server pid-collecting commands in parallel.
      (ret,out,err) = parallelRemoteExec(sshConns,cmds)

      # Analyze the results.
      pids = [o.splitlines() for o in out]
      pids = [[line.replace(random_prefix,'') for line in lines if random_prefix in line] for lines in pids]

      c = sum([len(p) for p in pids])
      if (c > 0):
         msgs = [ "checking (server %d (%s)) %s..." % (
               tpl[0].getServerId(), tpl[0].getServerHost(), " ".join(tpl[1])) for tpl in zip(sortedSrvList,pids) ]
         printInfo('\n'.join(msgs))

      if (needToCloseSSH): map(lambda x: x.close(),sshConns)
   finally:
      if (needToCloseSSH): sshCloseNoError(sshConns)

   printInfo("Found %d scidb processes" % (c))
   # Can modify the function to return pids also:
   # this would enable more targeted calls to this function
   # (only check servers that had running pids from the
   # previous call).
   return c

# Check that all instances are running the same version.
def check_scidb_version(srv, liid):
   cmdList=[gCtx._installPath+"/bin/scidb", "--version"]
   (ret,out,err) = executeIt(cmdList, srv, liid,
                             useSSH4Local=True, useConnstr=False,
                             useShell=False, ignoreError=False,
                             nocwd=True,stdoutFile=None,
                             stderrFile=None)
   return out


def check_scidb_versions_all():
   '''
   Check that all scidb instances have the same version.
   '''
   version = None
   # Check for any mismatched version strings.
   for srv in gCtx._srvList:
      ver = check_scidb_version(srv, srv.getServerInstances()[0])
      if version is None:
         version = ver
         printInfo("SciDB version of (server %d (%s)) is %s" % \
                   (srv.getServerId(), srv.getServerInstances()[0], version))
      elif ver != version:
         printError("SciDB version mismatch, (server %d (%s) is at %s)" % \
                    (srv.getServerId(), srv.getServerInstances()[0], ver))


def makeRelinkBinaryCmd(srv,liid):
   '''
   Returns a set of commands
   for a single server/instance pair as a string
   (commands are separated by &&)
   '''
   return ' && '.join([
         'rm -f '+ os.path.join(getInstanceDataPath(srv,liid),binFile(srv,liid)),
         'ln -fs '+ gCtx._installPath + '/bin/scidb ' + os.path.join(getInstanceDataPath(srv,liid),binFile(srv,liid))
         ])


def makeRelinkBinaryCommands(servers,instances=None):
   '''
   Create shell commands to first unlink and then re-link all of
   the paths to scidb binary files on the specified servers
   '''
   instances = applyInstanceFilter(servers, instances, nonesOK=False)

   allCommands = [ [makeRelinkBinaryCmd(srv, liid) for liid in liids] \
                   for srv,liids in zip(servers,instances) ]
   return allCommands


def startAllServers():
   '''
   Put together commands to start all scidb instances on given servers in config.ini.
   '''
   checkRedundancy(len(gCtx._srvList))
   startSomeServers(gCtx._srvList)

#XXX TODO: to be removed
def startSomeServersOld(servers, instances=None, checkRunning=True):
   '''
   Put together commands to start some scidb instances on given servers in config.ini.
   '''
   # Allegedly, list comprehensions are faster than the equivalent explicit
   # for-loops.  Hence, the command lists and commands themselves are
   # constructed via the list comprehensions.
   #
   # First, we sort the server list in ascending order: the greater the
   # sequential number of the server, the further down the list it will be
   # placed.
   sortedSrvList = sorted(servers,key=lambda s: s.getServerId()) #XXXX needed ?
   sshConnections = []
   try:
      # Establish ssh connections with all servers (possibly including the
      # local machine).
      sshConnections = [sshconnect( x) for x in sortedSrvList]

      # Check if the scidb is already running: the "cached" ssh connections
      # are used to communicate with all of the scidb servers.
      if checkRunning:
         pidCount = check_scidb_running(sshConnections,servers=sortedSrvList, instances=instances)
         if (pidCount > 0):
            sshCloseNoError(sshConnections)
            raise AppError("SciDB is still running. Try the stopall command before starting.")

      # Create the re-linking commands "on the side": these remove and recreate
      # links to the scidb executables on all servers (will be inserted into
      # command lists at a later time).
      relinkCmds = makeRelinkBinaryCommands(sortedSrvList, instances=instances)

      instances = applyInstanceFilter(sortedSrvList, instances, nonesOK=False)

      allCmds = [[ startCommandOnly(srv,liid) for liid in liids ] \
                    for srv,liids in zip(sortedSrvList,instances)]

      # Create CD commands for each instance:
      cdCmds = [[ 'cd ' + getInstanceDataPath(srv,liid) for liid in liids ] \
                   for srv,liids in zip(sortedSrvList,instances)]

      # Strip out blank command-line args: when command-line argument lists
      # are put together in the statement above, they contain quite a few
      # "blank" options.
      allCmds = [ [[z for z in y if len(z) > 0] for y in x] for x in allCmds]

      # Convert all of the commands into strings.
      allCmds = [[' '.join(y) for y in x] for x in allCmds]

      # Append binary relinking commands to instance-spawning commands.
      allCmds = [[' && '.join(y[0], y[1]) for y in zip(x[0],x[1])] \
                    for x in zip(relinkCmds,allCmds)]
      # Finally, append the CD commands to all instances
      allCmds = [[' && '.join(y[0], y[1]) for y in zip(x[0],x[1])] \
                    for x in zip(cdCmds,allCmds)]
      allCmds = [['(' + y + ' ) &' for y in x] for x in allCmds]

      # Lump all of the instance starting commands for each server into strings:
      # one string with all of the instances per one server.
      allCmds = [' '.join(x) for x in allCmds]

      # Run all of the commands.
      exits,outputs,errors = parallelRemoteExec(sshConnections,allCmds,waitFlag=True)
      map(lambda x: x.close(),sshConnections)
   finally:
      # Close down all of the ssh connections.
      sshCloseNoError(sshConnections)


def appendShellCmds(inputCmds, deltaCmds):
   '''
   Given 2 lists of commands one per each instance per each server (a list of lists of strings)
   merge the lists element-wise using the shell '&&' sign
   '''
   resultCmds = [[y[0] + ' && ' + y[1] for y in zip(x[0],x[1])] \
                 for x in zip(inputCmds,deltaCmds)]
   return resultCmds


def makeRemoteStartCmds(func, sortedSrvList, instances=None, parallel=True):
   '''
   Given a list of commands one per each instance per each server (a list of lists of strings)
   make the necessary transformations to execute the commands in parallel wrt to other servers
   and commands on the same server.
   '''
   instances = applyInstanceFilter(sortedSrvList, instances, nonesOK=False)

   allCmds = [[ func(srv,liid) for liid in liids ] \
                 for srv,liids in zip(sortedSrvList,instances)]

   # Create CD commands for each instance:
   cdCmds = [[ 'cd ' + getInstanceDataPath(srv,liid) for liid in liids ] \
                for srv,liids in zip(sortedSrvList,instances)]

   allCmds = appendShellCmds(cdCmds, allCmds)
   if parallel:
      allBackgroundCmds = [['(' + y + ' ) &' for y in x] for x in allCmds]
      # Lump all of the instance starting commands for each server into strings:
      # one string with all of the instances per one server.
      allCmds = [' '.join(x) for x in allBackgroundCmds]
   else:
      allCmds = [' && '.join(x) for x in allCmds]

   return allCmds


def startSomeServers(servers, instances=None, sshConnections=None, checkRunning=True):
   '''
   Put together commands to start some scidb instances on given servers in config.ini.
   '''
   # Allegedly, list comprehensions are faster than the equivalent explicit
   # for-loops.  Hence, the command lists and commands themselves are
   # constructed via the list comprehensions.
   #
   # First, we sort the server list in ascending order: the greater the
   # sequential number of the server, the further down the list it will be
   # placed.
   assert servers, "No servers specified"
   sortedSrvList = sorted(servers,key=lambda s: s.getServerId()) #XXXX needed ?

   closeConns = False
   if not sshConnections:
      sshConnections = []
      closeConns = True
   elif len(sshConnections) != len(sortedServerList):
      raise AppError("Number connections is not equal to the number of remote servers")

   try:
      # Establish ssh connections with all servers (possibly including the
      # local machine).
      if closeConns:
         sshConnections = [sshconnect( x) for x in sortedSrvList]

      # Check if the scidb is already running: the "cached" ssh connections
      # are used to communicate with all of the scidb servers.
      if checkRunning:
         pidCount = check_scidb_running(sshConnections,servers=sortedSrvList, instances=instances)
         if pidCount:
            sshCloseNoError(sshConnections)
            raise AppError("SciDB is still running. Try the stopall command before starting.")

      # Create the re-linking commands "on the side": these remove and recreate
      # links to the scidb executables on all servers (will be inserted into
      # command lists at a later time).

      makeCmd = lambda srv,liid: makeRelinkBinaryCmd(srv,liid) + ' && ' + \
          " ".join([s for s in startCommandOnly(srv,liid) if len(s)>0])

      allCmds = makeRemoteStartCmds(makeCmd, sortedSrvList, instances=instances, parallel=True)

      # Run all of the commands.
      exits,outputs,errors = parallelRemoteExec(sshConnections,allCmds,waitFlag=True)

      if closeConns:
         map(lambda x: x.close(),sshConnections)
   finally:
      # Close down all of the ssh connections.
      if closeConns:
         sshCloseNoError(sshConnections)


def useValgrind():
   use_valgrind = False
   return use_valgrind

def start(srv, liid, dryRun=False):
   '''
   Start a given instance or return a shell command for that purpose if dryRun.
   '''
   printInfo("start(%s)"%(validateInstance(srv,liid)))

   scidb_switches = gCtx._scidb_start_switches

   use_valgrind = useValgrind()

   printInfo("Starting SciDB server%s."%(" with valgrind" if use_valgrind else ""))

   cmdList = [get_ld_library_path(addVarName=True)]

   preloadLib=gCtx._configOpts.get('preload')
   if preloadLib:
      cmdList = [' ','LD_PRELOAD='+str(preloadLib)]
   cmdList += [' ','exec']

   if use_valgrind:
      assert (not preloadLib), str(preloadLib)+" cannot be preloaded for valgrind"
      assert os.path.exists('/usr/bin/valgrind'), "Missing /usr/bin/valgrind"
      if not gCtx._configOpts.get('no-watchdog',False):
         scidb_switches.append('--no-watchdog')
      # Name compatibility for z_valgrind test, which expects only 1 instance.
      vg_log = ('/tmp/valgrind.%s.log' % liid) if liid else '/tmp/valgrind.log'
      # WARNING: Place only tool-agnostic options here!  See below.
      vg_cmd = [ '/usr/bin/valgrind',
                 '-v',
                 '--num-callers=50',
                 '--log-file=%s' % vg_log
                 ]
      # If config.ini contains 'vg-foo-bar = baz', then add '--foo-bar=baz'
      # option to valgrind.  If config.ini contains 'vg-foo-0 = blah',
      # 'vg-foo-1 = bleh', etc.  then add multiple --foo options to
      # valgrind.  (Our config.ini parser won't allow more than one 'vg-foo'
      # option.)
      for key in gCtx._configOpts.keys():
         if key[:3] == 'vg-':
            vg_opt = re.sub(r'-[0-9]+$', '', key[3:])
            vg_val = gCtx._configOpts.get(key)
            vg_cmd.append('--%s=%s' % (vg_opt, vg_val))
      # Not all valgrind tools allow all options, be careful!
      # E.g. only memcheck allows --track-origins.
      vg_tool = gCtx._configOpts.get('vg-tool')
      if not vg_tool or vg_tool.lower() == 'memcheck':
         if 'vg-track-origins' not in gCtx._configOpts:
            vg_cmd.append('--track-origins=yes')
      cmdList += vg_cmd

   cmdList += [
                getInstanceDataPath(srv,liid)+'/'+binFile(srv, liid),
                "-i", srv.getServerHost(),
                "-p", str(gCtx._basePort+liid),
                "-k",
                "-l",gCtx._configOpts.get('logconf'),
                "-s", getInstanceDataPath(srv,liid) + '/storage.cfg17'
              ]
   cmdList += scidb_switches

   if (not dryRun):
       cmdList=[" ".join(cmdList)]
       executeIt(cmdList, srv, liid, waitFlag=False,
                 stdoutFile="scidb-stdout.log",
                 stderrFile="scidb-stderr.log",
                 useSSH4Local=True,
                 useShell=True)
       return None
   else:
       return cmdList

def startCommandOnly(srv, liid):
   cmdList = start(srv,liid,dryRun=True)
   cmdList.extend(['-c',createConnstr(remote=True, want_password=False)])
   cmdList.extend(['1>',os.path.join(getInstanceDataPath(srv,liid),'scidb-stdout.log')])
   cmdList.extend(['2>',os.path.join(getInstanceDataPath(srv,liid),'scidb-stderr.log')])
   return cmdList


def stopAll(force=False):
   '''
   Stop the whole system.
   '''
   stopSomeServers(gCtx._srvList, force=force)


def stopSomeServers(servers,instances=None,force=False):
   '''
   Stop instances on given servers.
   '''
   instances = applyInstanceFilter(servers,instances, nonesOK=True)

   map(lambda pair: stop(pair[0],instances=pair[1], force=force),zip(servers,instances))


def collectDbgAll(mode='full'):
   '''
   Collect debug information.  Loop through all servers and instances, coordinator last.
   '''
   now = datetime.datetime.now()
   dt = now.strftime("%Y%m%d-%H%M%S")

   for srv in reversed(gCtx._srvList):
      for liid in reversed(srv.getServerInstances()):
         if not isCoordinator(srv, liid):
            collectDbg(srv, liid, dt, mode)
   coordinator = gCtx._srvList[gCtx._coordSrvId] #coordinator
   collectDbg(coordinator, coordinator.getServerInstances()[0], dt, mode)

def collectDbg(srv, liid, dt, mode='full'):
   validateInstance(srv,liid)
   subdir = dt
   wanted = ['*.log*', 'mpi_*/*', os.path.join(subdir, 'stack*'),
             '*{0}-system.rpt'.format(dt)]
   if mode != 'stacksonly':
      wanted.append('core*')    # Cores too!
   filelist = "`ls %s 2>/dev/null`" % ' '.join(wanted)
   conn = sshconnect(srv)
   who = "(srv %d (%s) local instance %d)" % (srv.getServerId(), srv.getServerHost(), liid)
   system_report_py = os.path.join(gCtx._installPath, 'bin', 'system_report.py')
   scidb_cores = os.path.join(gCtx._installPath, 'bin', 'scidb_cores')

   coordinator = gCtx._srvList[gCtx._coordSrvId] #coordinator
   try:
      # this is called after all other liids.
      printInfo("collect logs, cores, install files %s" % who)
      if isCoordinator(srv,liid):
         name0 = "srv-" + "%d" % srv.getServerId() + "-" + "%d" % liid + "-" + dt
         name = "%s.tgz" % name0
         tgzname     = subdir+"/" + name
         instgzname  = subdir+"/install-" + name
         instlsname  = subdir+"/install-" + dt + ".txt"

         tgzfiles    = subdir+"/*"+ dt +"*.tgz"
         remote_tgzs = "*"+         dt +"*.tgz"
         allname     = "all-"+      dt +".tar"
         installRoot = gCtx._configOpts.get('install_root')

         commands = [
            ['mkdir', '-p', subdir],
            ['mv', remote_tgzs, subdir],
            [system_report_py, '-t', name0, "%s-system.rpt" % name0]
            ]
         if mode != 'stacksonly':
            commands.append(["tar", "cvPfz", instgzname, installRoot])
         else:
            commands.extend([
                  ["(find "+installRoot+" | xargs ls -l 1> "+instlsname+")"],
                  ["tar", "cvPfz", instgzname, installRoot+"/"+"etc ",
                       installRoot+"/"+"share ", instlsname]
                  ])
         commands.extend([
               [scidb_cores, dt],
               ["tar", "cvPfz", tgzname, filelist],
               ['tar', 'cvPf', allname, tgzfiles],
               ['rm', "-rf", subdir]
               ])

         try:
            for i, cmd in enumerate(commands):
               executeIt(cmd, srv, liid, sshc=conn, useConnstr=False,
                         useSSH4Local=True, stdoutFile='/dev/null',
                         ignoreError=(i != 0))
         except IOError, detail:
            if detail.errno != errno.ENOENT:
               raise AppError("I/O error, collectDbg%s: %s" % (who, detail))
         except AppError:
            raise               # Already wrapped it, let it through.

      else: #not coordinator

         name0 = "srv-" + "%d" % srv.getServerId() + "-" + "%d" % liid + "-" + dt
         name = "%s.tgz" % name0
         instname = "install-" + name # oink
         tgzname     = subdir+"/"+name
         instgzname  = subdir+"/"+instname
         instlsname  = subdir+"/install-" + dt + ".txt"
         installRoot = gCtx._configOpts.get('install_root')

         commands = [
            ['mkdir', '-p', subdir],
            [system_report_py, '-t', name0, "%s-system.rpt" % name0],
            ["(find "+installRoot+" | xargs ls -l 1> "+instlsname+")"],
            ["tar", "cvPfz", instgzname, installRoot+"/"+"etc", installRoot+"/"+"share", instlsname],
            [scidb_cores, dt],
            ["tar", "cvPfz", tgzname, filelist],
            # DON'T remove subdir yet!!!
            ]
         prefix = getInstanceDataPath(srv, liid)

         try:
            for cmd in commands:
               executeIt(cmd, srv, liid, sshc=conn, useConnstr=False,
                         useSSH4Local=True, stdoutFile="/dev/null",
                         ignoreError=True)

            sftp = None
            sftp = paramiko.SFTPClient.from_transport(conn.get_transport())

            remotep = prefix + "/" + tgzname
            localp = getInstanceDataPath(coordinator, coordinator.getServerInstances()[0]) + "/" + name

            printInfo("Running sftp remote: %s -> local: %s" % (remotep, localp))
            sftp.get(remotep, localp)

            remotep = prefix + "/" + instgzname
            localp = getInstanceDataPath(coordinator, coordinator.getServerInstances()[0]) + "/" + instname

            printInfo("Running sftp remote: %s -> local: %s" % (remotep, localp))
            sftp.get(remotep, localp)

            sftp.close()
            sftp = None
            # Now remove subdir.
            executeIt(['rm', "-rf", subdir], srv, liid, sshc=conn, useConnstr=False,
                      useSSH4Local=True, stdoutFile="/dev/null", ignoreError=True)
         except IOError, detail:
            if detail.errno != errno.ENOENT:
               raise RemoteAppError("I/O error, collectDbg%s: %s" % (who, detail))
         except AppError:
            raise               # Already wrapped it, let it through.
         except Exception, e1:
            if (sftp is None):
               msg_list = [
                  'Could not establish sftp connection to {0}: {1}'.format(who, e1),
                  'One possible cause is a known problem with scp/sftp service: echo statements in ~/.bashrc.',
                  'Please try removing echo statements from ~/.bashrc and re-run dbginfo command.',
                  ]
               raise RemoteAppError('\n'.join(msg_list))
            raise RemoteAppError("collectDbg%s: %s" % (who, e1))
         finally:
            if sftp: sshCloseNoError([sftp])
         conn.close()
         conn=None
   finally:
      if conn: sshCloseNoError([conn])


def getAllScidbPidsCmd(srv,instances=None,prefix=''):
   '''
   Returns a "ps"-type command to get pids of all running
   scidb processes.
   The returned command is for one server:
   it finds all scidbs running on that machine
   unless instances is specified in which case only
   the specified instances are found.

   Returned value is a string that does *not* end
   in a newline (since caller typically wants to
   pipe this into xargs or whatever).
   '''
   path = getSrvDataPath(srv)
   pid_prefix = prefix
   if (pid_prefix != ''):
      pid_prefix = '\"' + pid_prefix + '\"'

   if useValgrind():
      #XXX note: instances are ignored OK ?
      # When using valgrind, 'path' will not match the first cmd token... so
      # the awk script searches for 'path' anywhere in the line.
      return ''.join(('ps --no-headers -e -o pid,cmd |',
                      '''awk 'BEGIN { pat = "''', path, '''" ;
                                      gsub("/", "\\/", pat) }
                              /awk/ { next }
                              $0 ~ pat { print ''',pid_prefix,'''$1 }' ''')) # No newline!
   else:
      # Easy, the 'path' is the first cmd token so return that pid.
      cmd = 'ps --no-headers -e -o pid,cmd | awk \'{print $1 \" \" $2}\' | grep "' + path +'*"'
      filterStr = ''
      if instances:
         assert set(instances).issubset(set(srv.getServerInstances())), \
         "Invalid instance filter for server %d (%s)" % (srv.getServerId(),srv.getServerHost())

         filter = [ "/%d/%d/%s" % (srv.getServerId(),liid,binFile(srv,liid)) for liid in instances ]
         filterStr = ' | grep \'' + '\|'.join(filter) + '\''
      cmd = cmd + filterStr
      cmd = cmd + ' | awk \'{print ' + pid_prefix + '$1}\''

      printDebug("Pids cmd: %s" % cmd)
      return cmd


def stop(srv, instances=None, force=False):
   '''
   Stop a particular server.
   '''
   if (not force):
      printInfo("stop(server %d (%s))"%(srv.getServerId(),srv.getServerHost()))
      cmdList = [getAllScidbPidsCmd(srv,instances=instances) + ' | xargs kill']
   else:
      cmdList = [getAllScidbPidsCmd(srv,instances=instances) + ' | xargs kill -9']

   liid = srv.getServerInstances()[0] # any valid liid would do, we stop all liids on srv
   executeIt(cmdList, srv, liid, waitFlag=True, ignoreError=True,
             useConnstr=False, nocwd=True, useShell=True, useSSH4Local=True)


def checkMaxPostgresConns(n_instances):
   """checkMaxPostgresConns: test if user requested more scidb instances than
   max number of connections allowed by Postgres.  The function will raise an
   exception in these situations:
   1) Postgres is unreachable/not running
   2) it cannot determine Postgres max_connections setting by querying Postgres
   3) max_connections value is less than the total number of scidb instances
      requested in config.ini
   """
   coordinator = gCtx._srvList[gCtx._coordSrvId] #coordinator
   cmd_list = [ ' '.join(('psql -h %s -p %d',
                          '--username %s --dbname %s -t',
                          '-c "SELECT * FROM pg_settings WHERE name = \'max_connections\';"')) % (
         gCtx._pgHost,
         gCtx._pgPort,
         gCtx._configOpts.get('db_user'),
         gCtx._configOpts.get('db_name')) ]

   postgresErrMsg = textwrap.dedent("""
      Please make sure that Postgres max_connections value is greater than the
      total number of scidb instances in config.ini.  To modify max number of
      Postgres connections, locate the postgresql.conf file and alter the
      max_connections value there.  For more information please consult the
      Postgres web site:

          https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server

      Note that after changing the max_connections setting, the Postgres
      service must be restarted.
      """)

   ret,out,err = executeIt( # Run Postgres query command.
      cmd_list,
      coordinator,
      coordinator.getServerInstances()[0],
      useConnstr=False,
      useSSH4Local=True,
      nocwd=True,
      useShell=True
      )

   if (ret != 0):
      msgs = [
         'Error: Postgres max_connections query failed!',
         postgresErrMsg
         ]
      raise AppError('\n'.join(msgs))

   lines = out.split('\n')
   maxConnLines = [line for line in lines if 'max_connections' in line]

   if not maxConnLines:
      msgs = [
         'Error: cannot extract result of Postgres max_connections query!',
         postgresErrMsg
         ]
      raise AppError('\n'.join(msgs))

   tokens = [t.strip() for t in maxConnLines[0].split('|')] # Split the line into individual values.

   if (tokens[0] != 'max_connections'):
      msgs = [
         'Error: could not extract value for Postgres max_connections!',
         postgresErrMsg
         ]
      raise AppError('\n'.join(msgs))

   max_conns = -1

   try:
      max_conns = int(tokens[1]) # Convert the max_connections value into a number.
   except:
      msgs = [
         'Error: non-integer value found in Postgres max_connections - {0}!'.format(tokens[1]),
         postgresErrMsg
         ]
      raise AppError('\n'.join(msgs))

   if (max_conns < n_instances):
      msgs = [
         "Cannot create {0} scidb instances: Postgres (max_connections) currently allows only {1} connections!".format(
            n_instances, max_conns),
         postgresErrMsg
         ]
      raise AppError('\n'.join(msgs))

def checkRedundancy(nServers):
   if gCtx._configOpts.get('redundancy'):
      red = int(gCtx._configOpts.get('redundancy'))
      if not 0 <= red < nServers:
         raise AppError("Redundancy (%d) must be >= 0 and < number of servers (%d)" % (red, nServers))


 # Display SciDB installation version
def displayVersion():
   cmdList = [ os.path.join(gCtx._installPath, "bin", "scidb"), '-V' ]
   p = subprocess.Popen(cmdList)
   p.wait()
   if p.returncode != 0 :
      raise AppError("Cannot display SciDB version, return code: %s" % p.returncode)

class CmdExecutor:
    def __init__(self, ctx):
       self._ctx = ctx

    def waitToStop(self, servers, errorStr, instances=None):
       attempts=0
       conns = []
       try:
          conns = [sshconnect(srv) for srv in servers]
          pidCount = check_scidb_running(sshConns=conns,servers=servers,instances=instances)
          while pidCount > 0:
             if (not useValgrind()):
                attempts += 1
                if attempts>5:
                   stopSomeServers(servers, instances=instances, force=True)
                if attempts>10:
                   raise AppError(errorStr)
             time.sleep(1)
             pidCount = check_scidb_running(sshConns=conns,servers=servers,instances=instances)
          map(lambda conn: conn.close(),conns)
       finally:
          sshCloseNoError(conns)

    def version(self):
       displayVersion()

    def init_syscat(self):
       '''
       Initialize the system catalog on the local host.
       Must be executed with the postgres OS user privileges.
       '''
       if not isLocalHost(self._ctx._pgHost):
          raise AppError("System catalog host %s does NOT appear to be this (local) host!" % self._ctx._pgHost)

       user   = self._ctx._configOpts.get('db_user')
       db     = self._ctx._configOpts.get('db_name')
       passwd = self._ctx._configOpts.get('db_passwd')
       port   = str(self._ctx._pgPort)
       pguser = 'postgres'
       if self._ctx._args.pguser:
          pguser = self._ctx._args.pguser
       else:
          pguser = 'postgres'
       if self._ctx._pgdb:
          pgdb = self._ctx._pgdb
       else:
          pgdb = 'postgres'

       # Permission checks.
       try:
          pg_uid = pw.getpwnam(pguser).pw_uid
       except KeyError:
          raise AppError("Postgres user (%s) does not exist" % pguser)
       if os.geteuid() != pg_uid:
          raise AppError("You must run this command as owner of PostgreSQL!")

       # Wipe database if it already exists.
       psql = Psql(user=pguser, port=port, debug=_DBG, database=pgdb)
       exists = psql(
          "select 1 from pg_catalog.pg_database where datname = '%s'" % db)
       if exists:
          printInfo("Deleting %s..." % db)
          psql("drop database %s" % db)

       # Create database user if not already present.
       exists = psql(
          "select 1 from pg_catalog.pg_user where usename = '%s'" % user)
       if not exists:
          printInfo("Adding user %s..." % user)
          psql("create role %s with login password '%s'" % (
                user, passwd))

       # Create the database within the Postgres cluster.
       cmd = "createdb -p {0} --owner {1} {2}".format(port, user, db)
       p = subprocess.Popen(cmd.split(),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
       _, err = p.communicate()
       if p.returncode:
          raise AppError("Cannot create database %s: %s" % (db, err))

       # Add support for database triggers.
       exists = psql("select 1 from pg_language where lanname = 'plpgsql'")
       if not exists:
          printInfo("Installing language plpgsql for database %s" % db)
          cmd = "createlang -p {0} plpgsql {1}".format(port, db)
          p = subprocess.Popen(cmd.split(),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
          _, err = p.communicate()
          if p.returncode:
             raise AppError(
                "Cannot add plpgsql language to database '%s': %s" % (db, err))
       psql.database = db
       psql("update pg_language set lanpltrusted = true where lanname = 'c'")
       psql("grant usage on language c to %s" % user)

       return None

    def init_all(self):
       validate_config_init_settings(self._ctx._configOpts)
       initAll(force=self._ctx._args.force)

    def init_all_force(self):
       validate_config_init_settings(self._ctx._configOpts)
       initAll(force=True)

    def start_all(self):
       validate_config_init_settings(self._ctx._configOpts)
       startAllServers()
       coordinator = self._ctx._srvList[self._ctx._coordSrvId] #coordinator

       attempts=0
       while not check_scidb_ready(coordinator,coordinator.getServerInstances()[0]):
          attempts += 1
          if attempts >= self._ctx._args.max_attempts:
             raise AppError("Failed to start SciDB!")
          time.sleep(1)

    def stop_all(self):
       stopAll() # plumb connections into stopAll()
       serversToStop = self._ctx._srvList
       self.waitToStop(serversToStop, "Failed to stop SciDB!")

    def dbginfo(self):
       mode = "full"
       if self._ctx._args.light:
          mode="stacksonly"
       collectDbgAll(mode)

    def dbginfo_lt(self):
       collectDbgAll(mode='stacksonly')

    def check_pids(self):
       check_scidb_running()

    def check_version(self):
       check_scidb_versions_all()



def getContext(args, argv):
   ctx = Context()
   ctx._args = args

   assert argv, "Missing command line argument vector in getContext"
   installPath = os.path.dirname(os.path.abspath(os.path.dirname(argv[0])))

   # XXXX hack ???
   if args.subparser_name in ("service_add",
                              "service_remove",
                              "version"):
      ctx._installPath = installPath
      printDebug("Installation path: %s"%(ctx._installPath))
      return ctx

   if not args.config_file:
      ctx._config_file = os.path.join(installPath, "etc", "config.ini")
   else:
      ctx._config_file = args.config_file
   ctx._scidb_name = args.scidb_name

   parseConfig(ctx)

   if ctx._configOpts.get('install_root'):
      ctx._installPath = ctx._configOpts.get('install_root')
   else:
      ctx._installPath = installPath
      printWarn("Missing specification for 'install-root'")

   if ctx._installPath != installPath:
      printWarn("'install_root' configuration option: '%s' does not match the location of '%s': '%s'" %
                (ctx._installPath, _PGM, installPath))

   if not ctx._configOpts.get('base-path'):
      raise AppError("Missing specification for 'base-path'")
   ctx._baseDataPath = ctx._configOpts.get('base-path')

   ctx._dataDirPrefix = ctx._configOpts.get('data-dir-prefix')

   if not ctx._configOpts.get('base-port'):
      raise AppError("Missing specification for 'base-port'")
   ctx._basePort = int(ctx._configOpts.get('base-port'))

   if ctx._configOpts.get('ssh-port'):
      ctx._sshPort = int(ctx._configOpts.get('ssh-port'))

   if ctx._configOpts.get('pg-port'):
      ctx._pgPort = int(ctx._configOpts.get('pg-port'))

   if ctx._configOpts.get('db_port'):
      ctx._pgPort = int(ctx._configOpts.get('db_port'))

   if ctx._configOpts.get('key-file-list'):
      ctx._keyFilenameList = ctx._configOpts.get('key-file-list').split(',')

   if not ctx._srvList:
      raise AppError("Missing specification for servers 'server-#'")

   if ctx._configOpts.get('db_host'):
      ctx._pgHost = ctx._configOpts.get('db_host')
      ctx._coordSrvId = None
      for index,srv in enumerate(ctx._srvList):
         if ctx._pgHost == srv.getServerHost():
            ctx._coordSrvId = index #coordinator server index
            break
      if ctx._coordSrvId is None:
         raise AppError("Invalid specification for postgres host %s." % (ctx._pgHost) +
                        " It has to be the same as one of the server hosts.")
   else:
      ctx._coordSrvId = 0 #coordinator server index
      ctx._pgHost = ctx._srvList[ctx._coordSrvId].getServerHost()

   printDebug("Coordinator: %s" %
               validateInstance(ctx._srvList[ctx._coordSrvId],
                                ctx._srvList[ctx._coordSrvId].getServerInstances()[0]))

   # Find the pgpass file: first config.ini, then environment, then $HOME
   if ctx._configOpts.get('pgpassfile'):
      pgpassfile = ctx._configOpts.get('pgpassfile')
   else:
      pgpassfile = os.environ.get("PGPASSFILE", "")
      if not pgpassfile:
         pwent = pw.getpwnam(getpass.getuser())
         pgpassfile = os.sep.join((pwent.pw_dir, ".pgpass"))
   os.environ['PGPASSFILE'] = pgpassfile

   if hasattr(args, 'db_password') and args.db_password:
      ctx._configOpts['db_passwd'] = args.db_password
   else:
      ctx._configOpts['db_passwd'] = get_db_password(
         ctx._pgHost,
         ctx._pgPort,
         ctx._configOpts.get('db_name'),
         ctx._configOpts.get('db_user'),
         pgpassfile)

   return ctx


def handle(superParser, superArgs, cmdArgs=[], argv=None):
   global _DBG
   if not _DBG:
      _DBG = superArgs.verbose

   global gCtx                  # ...needed since we assign to it below.
   cmdExec = CmdExecutor(gCtx)
   modName="scidb"
   parser = argparse.ArgumentParser(prog=superParser.prog+" [-m "+modName+']')

   subparsers = parser.add_subparsers(dest='subparser_name',
                                      title="Module '%s'"%(modName),
                                      description="""
SciDB administration and configuration.
Use no options to display top-level help.
Use -h/--help with a particular subcommand from the list below to learn its usage.
NOTE:\nSeveral subcommands rely on the so called coordinator' server and/or instance.
The coordinator server has the same hostname/IP as the value of the 'db_host' configuration option.
This implies that at least one SciDB instance needs to be configured on the 'db_host' machine.
If 'db_host' is not specified, the hostname/IP of the server with the lowest 'server-' ID is used.
The coordinator instance is the instance with the lowest server instance ID on the corrdinator server.
""")

   subParser = subparsers.add_parser('version', description="Check SciDB version")
   subParser.set_defaults(func=cmdExec.version)

   subParser = subparsers.add_parser('init_syscat', description="Initialize system catalog. DEPRECATED, use init-syscat.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.init_syscat)

   subParser = subparsers.add_parser('init-syscat', description="""
Initialize system catalog. It must be invoked on the same host where the system catalog Postgres server runs.
It must also be invoked with the privileges of the postgres OS user.
""")
   subParser.add_argument('-p', '--db-password', default=None, help="database password to use")
   subParser.add_argument('--pguser', help="postgresql database owner, default is postgres")
   subParser.add_argument('--pgdb',   help="postgresql administrative database, default is postgres")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.init_syscat)

   subParser = subparsers.add_parser('init_all', description="Initialize SciDB instances. DEPRECATED, use init-all.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.add_argument('-f','--force', action='store_true', help="automatically confirm any old state/directory cleanup")
   subParser.set_defaults(func=cmdExec.init_all)

   subParser = subparsers.add_parser('initall', description="Initialize SciDB instances. DEPRECATED, use init-all")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.add_argument('-f','--force', action='store_true', help="automatically confirm any old state/directory cleanup")
   subParser.set_defaults(func=cmdExec.init_all)

   subParser = subparsers.add_parser('init-all', description="Initialize SciDB instances.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.add_argument('-f','--force', action='store_true', help="automatically confirm any old state/directory cleanup")
   subParser.set_defaults(func=cmdExec.init_all)

   subParser = subparsers.add_parser('initall-force', description="Initialize SciDB instances.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.init_all_force)

   # -m is used by handle(..) as a module flag
   subParser = subparsers.add_parser('start_all', description="Start all SciDB instances. DEPRECATED, use start-all.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.add_argument('-A', '--auth-file', default=None, nargs='?', help="name of file containing authentication info")
   subParser.add_argument('-M', '--max-attempts', type=int, choices=xrange(1, 50), default=30, nargs='?', help="max number of attempts to validate SciDB availability after start")
   subParser.set_defaults(func=cmdExec.start_all)

   subParser = subparsers.add_parser('startall', description="Start all SciDB instances. DEPRECATED, use start-all.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.add_argument('-A', '--auth-file', default=None, nargs='?', help="name of file containing authentication info")
   subParser.add_argument('-M', '--max-attempts', type=int, choices=xrange(1, 50), default=30, nargs='?', help="maximum number of functional checks performed by scidb")
   subParser.set_defaults(func=cmdExec.start_all)

   subParser = subparsers.add_parser('start-all', description="Start all SciDB instances.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.add_argument('-A', '--auth-file', default=None, nargs='?', help="name of file containing authentication info")
   subParser.add_argument('-M', '--max-attempts', type=int, choices=xrange(1, 50), default=30, nargs='?', help="maximum number of functional checks performed by scidb")
   subParser.set_defaults(func=cmdExec.start_all)

   subParser = subparsers.add_parser('stop_all', description="Stop all SciDB instances. DEPRECATED, use stop-all.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.stop_all)

   subParser = subparsers.add_parser('stopall', description="Stop all SciDB instances. DEPRECATED, use stop-all.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.stop_all)

   subParser = subparsers.add_parser('stop-all', description="Stop all SciDB instances.")
   subParser.add_argument('scidb_name', help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.stop_all)


   subParser = subparsers.add_parser('dbginfo', description="""
Collect debug information form all SciDB instances and deposit it on the coordinator instance.
""")
   subParser.add_argument('scidb_name',  help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.add_argument('-l','--light', action='store_true', help="skip large objects such as binaries & cores")
   subParser.set_defaults(func=cmdExec.dbginfo)

   subParser = subparsers.add_parser('dbginfo-lt', description="""
Collect debug information from all SciDB instances and deposit it on the coordinator instance
while skipping large objects such as binaries & cores. DEPRECATED, use dbginfo.
""")
   subParser.add_argument('scidb_name',  help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.dbginfo_lt)

   subParser = subparsers.add_parser('check_pids', description="Display pids of runing SciDB instances. DEPRECATED, use check-pids.")
   subParser.add_argument('scidb_name',  help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.check_pids)

   subParser = subparsers.add_parser('check-pids', description="Display pids of runing SciDB instances.")
   subParser.add_argument('scidb_name',  help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.check_pids)

   subParser = subparsers.add_parser('check_version', description="Check that all SciDB instances are on the same version.  DEPRECATED, use check-version")
   subParser.add_argument('scidb_name',  help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.check_version)

   subParser = subparsers.add_parser('check-version', description="Check that all SciDB instances are on the same version.")
   subParser.add_argument('scidb_name',  help="SciDB name as specified in config.ini")
   subParser.add_argument('config_file', default=None, nargs='?', help="config.ini file to use, default is /opt/scidb/<version>/etc/config.ini")
   subParser.set_defaults(func=cmdExec.check_version)

   args = parser.parse_args(cmdArgs)

   try:
      gCtx = getContext(args, argv)
      cmdExec._ctx = gCtx
      args.func()
   except AppError:
      raise
   except Exception, e:
      traceback.print_exc()
      sys.stderr.flush()
      raise AppError("Command %s failed: %s" % (args.subparser_name, e))

def main(argv=None):
   if argv is None:
      argv = sys.argv

   global _PGM
   _PGM = "%s:" % os.path.basename(argv[0]) # colon for easy use by print

   parser = argparse.ArgumentParser(add_help=False, usage="%(prog)s [-m MODULE] [-v] module-specific-options")
   parser.add_argument('-m','--module', help="module for requested functionality, default is scidb")
   parser.add_argument('-v','--verbose', action='store_true', help="display verbose output")
   (args, modArgs) = parser.parse_known_args(argv[1:])

   if not args.module and not modArgs:
      parser.print_help()
      return 1

   if not args.module:
      args.module = "scidb"

   global _DBG
   _DBG = os.environ.get("SCIDB_DBG", False)
   if not _DBG:
      _DBG = args.verbose
   printDebug("Debug logging is on!") # If indeed it is...

   try:
      func = handle
      modName = args.module
      if modName != "scidb":
         module = __import__(modName)
         func = module.handle
   except Exception, e:
      # Likely an ImportError or AttributeError.
      printError("Module {0}: {1}".format(args.module, e))
      if _DBG:
         traceback.print_exc()
      return 1

   try:
      func(parser, args, modArgs, argv)
   except AppError, e:
      if isinstance(e, RemoteAppError):
         printError("(REMOTE)", e)
      else:
         printError(e)
      if _DBG:
         traceback.print_exc()
      return 1
   except Exception, e:
      # If we haven't handled or wrapped it by now, we want the backtrace.
      printError("Unhandled exception:", e)
      printError("...while running module", args.module, "with", modArgs)
      traceback.print_exc()
      return 1
   else:
      return 0

# Whether this module is run as main() or imported, it always needs to
# have a default global context object.
gCtx = Context()

if __name__ == "__main__":
   sys.exit(main())