#!/usr/bin/python

"""
An easy-to-use wrapper for the iquery SciDB client.
"""

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

import argparse
import json
import os
import subprocess as subp
import tempfile
import textwrap


class IQueryError(Exception):
    pass


class _UnsupportedAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        raise IQueryError("Option %s is not supported" % option_string)


class IQuery(object):

    # Must be a r"raw" docstring, else doctests fail (because they
    # need the embedded backslashes)!
    r"""An easy-to-use wrapper for the iquery SciDB client.

    IQuery objects are callables that invoke the iquery client and
    return a 2-tuple of strings: the command's stdout and stderr.  The
    command's exit status can be read from the returncode attribute.

    IQuery object attributes correspond to iquery command line
    options, and can be set directly or by passing a dictionary to the
    constructor or the update() method.  Attributes can also be set
    temporarily on a per-call basis, either by using keyword arguments
    or by command line options in the argument list.

    Attributes/keywords that correspond to iquery command line options
    are:

        admin - connect using reserved system resources
        afl - use AFL query language, default False
        auth_file - authentication file used by namespaces library
        format - output format, default 'dcsv'
        host - SciDB host, default 'localhost'
        no_fetch - use -n to suppress output, default False
        pluginsdir - plugin directory, default "use iquery default"
        port - SciDB host's TCP port, default 1239
        precision - precision, default "use iquery default"
        timer - use -t to print execution time, default False
        verbose - use -v to print debug info, default False

    These attributes and keywords are also supported:

        auth - ('user', 'password') tuple used to set up a temporary
                 auth_file with the given credentials (or with string
                 argument, an alias for auth_file)
        debug - enable debugging output for the IQuery object itself
        namespace - call set_namespace(...) prior to executing queries
        ns - alias for namespace
        prog - iquery client program to run, default 'iquery'
        quiet - alias for no_fetch
        returncode - exit status of last iquery invocation (read-only)
        stderr - must be subprocess.PIPE (the default) or subprocess.STDOUT

    EXAMPLES

    The following examples assume that:
      - the 'namespaces' library was loaded,
      - SciDB was restarted with security=password configured,
      - no arrays named 'A' or 'B' exist,
      - the SCIDB_INSTALL_PATH environment variable is set, and
      - the SCIDB_CONFIG_USER environment variable is NOT set.
    (Otherwise the doctests will fail.)

    A bare iquery object is not much good in namespaces mode:

        >>> iquery = IQuery(afl=True)
        >>> out, err = iquery("list()")
        >>> "SCIDB_LE_AUTHENTICATION_ERROR" in err
        True

    You can set up authorization credentials in several ways.  First,
    let's set up an auth_file these examples can use:

        >>> import os
        >>> import textwrap
        >>> with open('/tmp/iqauth.ini', 'w') as F:
        ...     print >>F, textwrap.dedent('''
        ...          [security_password]
        ...          user-name=scidbadmin
        ...          user-password=Paradigm4'''.lstrip('\n'))
        >>> _ = os.system("chmod 600 /tmp/iqauth.ini")

    We now support and prefer JSON auth_files, so let's make one of
    those too.

        >>> import json
        >>> with open('/tmp/iqauth.json', 'w') as F:
        ...     print >>F, json.dumps({
        ...          'user-name': 'scidbadmin',
        ...          'user-password': 'Paradigm4'})
        >>> _ = os.system("chmod 600 /tmp/iqauth.json")

    You can use the auth_file either as a keyword argument or on the
    "command line":

        >>> iquery("create array A<v:int64>[i]",
        ...        auth_file='/tmp/iqauth.ini')
        ('Query was executed successfully\n', '')
        >>> iquery("-A", "/tmp/iqauth.ini", "-otsv",
        ...     "project(filter(list(), name='A'), name, schema)")
        ('A\tA<v:int64> [i=0:*,1000000,0]\n', '')
        >>> iquery("-A", "/tmp/iqauth.json", "-otsv",
        ...     "project(filter(list(), name='A'), name, schema)")
        ('A\tA<v:int64> [i=0:*,1000000,0]\n', '')

    But there is no need to create your own auth_file.  IQuery objects
    will do it for you if you provide an auth=('user','password') tuple:

        >>> iquery("create array B<v:int64>[i]",
        ...        auth=('scidbadmin', 'Paradigm4'))
        ('Query was executed successfully\n', '')

    But keyword arguments and "command line"-style options only affect
    the current invocation.  If you omit them, the IQuery object
    resumes its default behavior:

        >>> out, err = iquery("list()")
        >>> "SCIDB_LE_AUTHENTICATION_ERROR" in err
        True

    To make options permanent, you need to set the corresponding
    object attributes.  For authentication, if you have an existing
    auth_file handy then use that:

        >>> iquery.auth_file = '/tmp/iqauth.json'

    But if not, set the auth attribute and a temporary auth_file will
    be created (and deleted when the IQuery object is garbage
    collected).  Either way, from then on you can invoke the object
    without worrying about authentication options:

        >>> iquery.auth = ('scidbadmin', 'Paradigm4')
        >>> iquery("-otsv",
        ...        "project(filter(list(), name='A'), name, schema)")
        ('A\tA<v:int64> [i=0:*,1000000,0]\n', '')

    If you provide command line switches in the argument list, they
    affect only that call.  Running the previous query without "-otsv"
    reverts back to the default 'dcsv' format:

        >>> iquery("project(filter(list(), name='A'), name, schema)")
        ("{No} name,schema\n{0} 'A','A<v:int64> [i=0:*,1000000,0]'\n", '')

    As with auth and auth_file, you can set the format for all future
    calls by setting the like-named attribute.

        >>> iquery.format = 'tsv'
        >>> iquery("project(filter(list(), name='A'), name, schema)")
        ('A\tA<v:int64> [i=0:*,1000000,0]\n', '')

    There are settable attributes for nearly all iquery command line
    options.  You can set them directly as above, or you can set them
    using a dictionary.  Here we update our existing iquery object
    with a dictionary, but you could pass a similar dictionary to the
    IQuery constructor.

        >>> iquery.update(
        ...     {'format': 'csv',
        ...      'prog': '/'.join((os.environ['SCIDB_INSTALL_PATH'],
        ...                        'bin/iquery'))
        ...     })
        >>> iquery("project(filter(list(), name='A'), name, schema)")
        ("'A','A<v:int64> [i=0:*,1000000,0]'\n", '')

    You can check the return code of the last iquery command:

        >>> iquery.returncode
        0
        >>> _, _ = iquery("no(such(operators()))")
        >>> iquery.returncode
        1

    You can mix stdout and stderr results by setting the stderr
    attribute to subprocess.STDOUT.  That's not too useful because the
    iquery program tends to write stdout or stderr but not both, but
    it's supported:

        >>> out, err = iquery("bogus(query)", stderr=subp.STDOUT)
        >>> err is None
        True
        >>> "Error id:" in out
        True
        >>> iquery.returncode
        1

    The only supported values for stderr are subprocess.STDOUT and
    subprocess.PIPE.  (Use of file objects is not supported and has
    not been tested, so stick to these two.)

        >>> saved_exception = None
        >>> try:
        ...     _, _ = iquery("list()", stderr="Bad, no fileno attribute!")
        ... except Exception as e:
        ...     saved_exception = e
        >>> isinstance(saved_exception, AttributeError)
        True

    Finally, clean up so these doctests can be run repeatedly.  (And
    just FYI, this docstring is r"raw" because of all the backslashes
    in the expected query output.)

        >>> _, _ = iquery("remove(A) ; remove(B)")
        >>> _ = os.system("/bin/rm /tmp/iqauth.ini")
        >>> _ = os.system("/bin/rm /tmp/iqauth.json")
    """

    # Class methods use a single object kept here.
    _iqobj = None

    # Some attributes have short (or long) aliases.
    _aliases = {
        'ns': 'namespace',
        'quiet': 'no_fetch',
    }

    def _dbg(self, *args):
        """Print debugging information."""
        if self.debug:
            print "DBG:", ' '.join(map(str, args))

    def __init__(self, config=None, **kwargs):
        """Initialize attributes from config dict and keywords."""
        # Public attributes in alpha order for easy maintenance.
        # Attributes that take their values from command line options
        # MUST have names that match the corresponding iquery client
        # option (as it would translate to Python, e.g. --auth-file
        # becomes auth_file).  See _parse_inline_options() below.
        # Other public attributes (like 'prog') are permitted if they
        # do not conflict with a command line option.
        self.admin = False
        self.auth = None
        self.auth_file = os.environ.get('SCIDB_CONFIG_USER', None)
        self.format = None
        self.host = os.environ.get('IQUERY_HOST', None)
        self._default_host = self.host
        self.afl = False
        self.namespace = None
        self.pluginsdir = None
        self.port = os.environ.get('IQUERY_PORT', None)
        self._default_port = self.port
        self.precision = None
        self.no_fetch = False
        self.stderr = subp.PIPE
        self.timer = False
        self.verbose = False
        # Public attributes *not* corresponding to iquery command options.
        self.debug = False
        self.prog = 'iquery'
        self.lastquery = None
        self.json_auth_file = True
        # Override the defaults if we were given a dict.
        if kwargs or (config is not None):
            self.update(config, **kwargs)
        # Private attributes:
        self._auth_tempfile = None
        self._dirty = True
        self._cached_command = None
        self._session = None     # NYI
        self._exitstatus = None  # Last subprocess returncode

    @staticmethod
    def setenv(ns):
        """Set IQUERY_HOST and IQUERY_PORT from an argparse.Namespace.

        Command line options override environment.  Environment
        overrides hard-coded defaults.  This is easier than reaching
        inside a test class to poke its IQuery object(s).
        """
        if ns.host:
            os.environ['IQUERY_HOST'] = ns.host
        elif 'IQUERY_HOST' not in os.environ:
            os.environ['IQUERY_HOST'] = 'localhost'
        if ns.port:
            os.environ['IQUERY_PORT'] = str(ns.port)
        elif 'IQUERY_PORT' not in os.environ:
            os.environ['IQUERY_PORT'] = '1239'

    def _ingest(self, k, v):
        """Set an existing attribute k to value v, with some twists."""
        k1 = IQuery._aliases.get(k, k)
        if k1.startswith('_'):
            raise IQueryError("Cannot modify private attribute '%s'" % k)
        if k1 == 'port':
            v = self._default_port if v is None else str(int(v))
        elif k1 == 'host' and v is None:
            v = self._default_host
        # *Not* setattr(), else IPython has inspect difficulties.
        object.__setattr__(self, k1, v)

    def update(self, params=None, **kwargs):
        """Update this IQuery's attributes from params dict/iterable.

        @param params dict/iterable with parameter name/value pairs
        @param kwargs keyword name/value pairs
        @returns None
        @throws IQueryError if attribute with 'name' does not exist

        @description
        You can always set individual public attributes directly, but
        if you want to set several all at once, this is the way.  If
        you want to set attributes for one call only, use keyword
        arguments on that call.

        This is intended to work just like dict.update(), except that
        you may not update private attributes.  Doing so raises an
        IQueryError.
        """
        if params is not None:
            try:
                # Maybe it's a dict?
                for key in params.keys():
                    self._ingest(key, params[key])
            except AttributeError:
                # Not a dict (no keys() attribute), try iterable-of-pairs.
                for k, v in params:
                    self._ingest(k, v)
        for k in kwargs:
            self._ingest(k, kwargs[k])
        self._dirty = True
        return None

    def __setattr__(self, name, value):
        """Set attribute and also set the 'dirty' flag."""
        # Make sure public attribute updates set the _dirty flag.  We
        # want to know when these change, so we can update the cached
        # command arg list.
        name = IQuery._aliases.get(name, name)
        if not name.startswith('_'):
            object.__setattr__(self, '_dirty', True)
        object.__setattr__(self, name, value)

    def __getattr__(self, name):
        """Expose read-only returncode attribute.

        Some "logical" public attributes, like returncode, are derived
        from private ones.
        """
        # XXX BUG: The _aliases dict is not heeded here.  (Maybe attribute
        # aliases are a bad idea.)
        if name == 'returncode':
            return self._exitstatus

    def __call__(self, *args, **kwargs):
        """Invoke iquery to run a SciDB query."""
        self._dbg("IQuery object", id(self), "called:", args, kwargs)
        self._dbg("My attributes:", "\n\t".join([str((x, self.__dict__[x]))
                                                 for x in self.__dict__]))
        if not args or not args[0]:
            raise IQueryError("No query")

        # If we have per-call options, make a one-time surrogate to
        # absorb them so they won't affect future calls.  This "slow
        # path" is backward compatible with scidblib.util.iquery(), in
        # that options can appear in the argument list.
        if args[0][0].startswith('-') or kwargs:
            self._dbg("surrogate needed")
            # Clone our public attributes only, that way no worries about
            # open _auth_tempfile, _dirty state, etc.
            surrogate = IQuery()
            surrogate.update([(x, self.__dict__[x]) for x in self.__dict__
                              if not x.startswith('_')])
            surrogate._is_surrogate = True  # for debug
            if args[0][0].startswith('-'):
                args = surrogate._parse_inline_options(args)
            if kwargs:
                surrogate.update(kwargs)
            result = surrogate(*args)
            self._exitstatus = surrogate.returncode
            self.lastquery = surrogate.lastquery
            return result

        # All our attributes are set, we're ready to execute the rest
        # of the args list as queries.
        n = len(args)
        if n == 0:
            raise IQueryError("Session mode is not yet implemented")
        elif n == 1:
            return self._run(args[0])
        else:
            outs = []
            errs = []
            for arg in args:
                out, err = self._run(arg)
                outs.append(out)
                errs.append(err)
            return outs, errs

    def _setup_auth_file(self):
        """Set up a temporary auth_file based on auth=(user, password) kwarg.

        You get to specify authorization parameters two ways: by
        setting the auth_file attribute, or by passing an
        auth=('username', 'password') tuple.  Here we handle the
        latter case by setting up a temporary auth_file.
        """
        assert self.auth, "Cannot set up auth_file without an auth attribute"
        if self._auth_tempfile:
            self._auth_tempfile.close()
        self._auth_tempfile = tempfile.NamedTemporaryFile(prefix='iqauth.')
        if self.json_auth_file:
            print >>self._auth_tempfile, json.dumps({
                'user-name': self.auth[0],
                'user-password': self.auth[1]})
        else:
            print >>self._auth_tempfile, textwrap.dedent("""
                [security_password]
                user-name={0}
                user-password={1}""".format(*self.auth).lstrip('\n'))
        self._auth_tempfile.flush()  # required since we're not closing it yet
        self.auth_file = self._auth_tempfile.name

    def _parse_inline_options(self, args):
        """Extract attribute settings from inline command options.

        We construct an argparse parser that mimics the iquery client
        (at least for its typical usage), then use it to extract
        configuration values.  This is way better than inventing our
        own argument parsing, even though we might not mimic iquery
        exactly (but no problems found yet!).
        """
        # We need not pay attention to niceties like help strings.
        # Options are in the same order that "iquery -h" lists them.
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('-w', '--precision', type=int)
        parser.add_argument('-c', '--host')
        parser.add_argument('-p', '--port')
        parser.add_argument('-q', '--query')
        parser.add_argument('-f', '--query-file', action=_UnsupportedAction)
        parser.add_argument('-r', '--result', action=_UnsupportedAction)
        parser.add_argument('-o', '--format')
        parser.add_argument('-v', '--verbose', action='store_true',
                            default=None)
        parser.add_argument('-t', '--timer', action='store_true', default=None)
        parser.add_argument('-n', '--no-fetch', action='store_true',
                            default=None)
        parser.add_argument('-a', '--afl', action='store_true', default=None)
        parser.add_argument('--admin', action='store_true', default=None)
        parser.add_argument('-u', '--pluginsdir')
        parser.add_argument('-h', '--help', action=_UnsupportedAction)
        parser.add_argument('-V', '--version', action=_UnsupportedAction)
        # Implement this one along with session mode:
        parser.add_argument('--ignore-errors', action=_UnsupportedAction)
        parser.add_argument('-A', '--auth-file')
        # Assume any remaining args are query strings (in case -q was
        # inadvertently omitted).
        parser.add_argument('rest', nargs='*')

        # Parse 'em and weep.  IMPORTANT: self.update() relies on the
        # attributes of the resulting argparse.Namespace object being
        # a subset of our own attribute names!!!
        ns = parser.parse_args(args)
        options = [(x, ns.__dict__[x]) for x in ns.__dict__
                   if x not in ('query', 'rest') and
                   ns.__dict__[x] is not None]
        self._dbg("updating with", options)
        self.update(options)
        return [ns.query] if ns.query else ns.rest

    def _strip_qwex_line(self, s):
        """Get rid of "Query was executed successfully" junk.

        Because we don't (yet) have session mode, we implement
        namespaces by tacking on a set_namespace() query up front.
        That means the query output is going to start with a bogus
        "Query was executed..." line.  Get rid of it, so that users
        need not care.
        """
        assert self.namespace, 'Stripping "qwex" line but no namespace!'
        qwex = "Query was executed successfully\n"
        return s[len(qwex):] if s.startswith(qwex) else s

    def _run(self, query):
        """Run a single query string using current attributes."""
        # Build the command args.
        if self._dirty:
            self._dbg("dirty, rebuilding")
            if isinstance(self.auth, basestring):
                self.auth_file = self.auth
                self.auth = None
            cmd = [self.prog]
            if self.host:
                cmd.extend(['-c', self.host])
            if self.port:
                cmd.extend(['-p', str(self.port)])
            if self.auth:
                self._setup_auth_file()
            if self.auth_file:
                cmd.extend(['-A', os.path.expanduser(self.auth_file)])
            if self.admin:
                cmd.append("--admin")
            if self.format:
                cmd.append("-o%s" % self.format)
            if self.timer:
                cmd.append('-t')
            if self.verbose:
                cmd.append('-v')
            if self.pluginsdir:
                cmd.extend(['-u', self.pluginsdir])
            qopts = ['-']
            if self.no_fetch:
                qopts.append('n')
            if self.afl:
                qopts.append('a')
            qopts.append('q')
            cmd.append(''.join(qopts))
            self._cached_command = cmd
            self._dirty = False
        # Build final query and run it.
        set_ns = self.namespace and self.namespace != 'public'
        if set_ns:
            query = "set_namespace('{0}'); {1}".format(self.namespace, query)
        self.lastquery = query
        cmd = list(self._cached_command)
        cmd.append(query)
        self._dbg("Cmd:", cmd)
        p = subp.Popen(map(str, cmd), stdout=subp.PIPE, stderr=self.stderr)
        out, err = p.communicate()
        self._exitstatus = p.returncode
        if set_ns:
            out = self._strip_qwex_line(out)
        return out, err


if __name__ == '__main__':
    # Run module doctests.
    import doctest
    doctest.testmod()
