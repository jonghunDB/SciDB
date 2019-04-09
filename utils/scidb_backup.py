#!/usr/bin/python

# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2018 SciDB, Inc.
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

r"""Backup and restore utility for SciDB array data.

This script saves (or restores) SciDB arrays into (or from) an archive
folder on the coordinator host.  If the --parallel option is used, the
array data is saved into per-instance folders on all cluster hosts,
and the actual folder names will be the given archive folder name plus
a per-instance suffix.

The script records the options used to make a backup archive in the
archive itself, and will automatically use those same options when
restoring that archive.

Normally the script saves all arrays in all namespaces.  You can
change this behavior using the -f/--filter option to filter on array
names.  You can also use the --exclude-ns and --include-ns options to
constrain the script to desired namespaces.  These options work with
both --save and --restore.

If you encounter problems, the --ping option can help diagnose
problems with SSH connectivity, directory access, etc.

Examples
========

 1. Save all arrays into /tmp/Bkp on the coordinator host in
    gzip-compressed tab-separated-values format.

        $ scidb_backup.py --save tsv+ --zip /tmp/Bkp

 2. Restore the data saved in the previous example.  The data format
    is recorded in the folder, but for backward compatibility the
    --restore option still takes a format name... you can use "undef"
    for that if you forgot the format.

        $ scidb_backup.py --restore undef /tmp/Bkp

 3. Save arrays in the "gm" and "ford" namespaces in parallel opaque
    format.

        $ scidb_backup.py --save opaque --parallel \
        > --include-ns=gm,ford  /tmp/Customers

 4. Restore only the "gm" arrays from a previously saved archive.

        $ scidb_backup.py --restore undef --include-ns=gm /tmp/Customers

 5. Save everything not in the 'public' or 'scratch' namespaces.

        $ scidb_backup.py --save binary --parallel \
        > --exclude-ns=public,scratch /backups/$(date --iso-8601).bkp

 6. Use --ping to gain confidence that a parallel save will work.

        $ scidb_backup.py --ping --parallel

 7. Use --ping to gain confidence that a parallel restore from /tmp/Bkp.*
    per-instance archive folders will work.

        $ scidb_backup.py --ping --parallel /tmp/Bkp

Caveats
=======

 - The iquery SciDB client program should be in the PATH, and the
   scidblib Python library must be in the sys.path or PYTHONPATH.

 - The script need not run on one of the SciDB cluster hosts, so long
   as all cluster hosts are accessible via SSH.  However, this is not
   (yet) well-tested.
"""

import argparse
import multiprocessing as mp
import os
import re
import sys
import tempfile
import textwrap
import traceback

from collections import namedtuple
from contextlib import contextmanager

from scidblib.iquery_client import IQuery
from scidblib.ssh_runner import SshRunner
import scidblib.scidb_schema as Schema
from scidblib.scidb_psf import confirm
from scidblib.util import make_table
from scidblib import AppError

# IMPLEMENTATION NOTES
# ====================
#
# Pool Workers
# ------------
#
# We use the multiprocessing module to dispatch work in parallel to
# each instance via multiprocessing.Pool objects.  The pool worker
# functions must follow these rules:
#
# - By convention they all begin with '_mpw_', e.g. _mpw_foo().
#
# - Unfortunately they cannot be defined as closures or inner
#   functions, because they have to be pickleable and the 'pickle'
#   module can't handle that.  (The pool objects use pickle to
#   transmit worker code and arguments to pool processes.)
#
# - InstanceInfo namedtuples also cannot be pickled, but they can be
#   turned into pickleable dictionaries.  That's the reason for the
#   calls to x._asdict() to supply 'ii' parameters to the workers.
#
# - Worker functions cannot be decorated: this also thwarts pickling.
#
# - Exceptions that escape worker functions make debugging difficult,
#   so every worker function must have a try/except block to catch and
#   log them.  (Would have been nice to use a decorator, oh well.)
#
# For all of these reasons, I recommend that if you need a new worker
# function you start by copying an existing one.
#
# Sometimes an _mpw_func is called directly from the main process to
# avoid code duplication.  Sometimes an _mpw_func1 is called from
# _mpw_func2 for the same reason.  This should continue to work.
#
# Cost of Remote Ssh
# ------------------
#
# In earlier SciDB releases, this script tried to jam as many shell
# commands as possible into the stdin of ssh, in a misguided effort to
# amortize the cost of remote interactions across as many commands as
# possible.  That was a false economy and further proof, if any were
# needed, that premature optimization is the root of all evil.  On all
# but the most trivial data sets, the time-cost of anything this
# script does is dwarfed by the time needed for the SciDB save() or
# input() queries to run.  Optimizing remote interactions here is
# pointless, and made the previous script difficult to debug when
# anything went wrong.
#
# Special Treatment of Local Backups
# ----------------------------------
#
# One goal of the rewrite was to allow this script to run on a
# non-cluster host: *where* the script is run should not matter, so
# long as it has SSH access to all the servers.  However, this is not
# yet fully tested.  There may be one or two undiscovered baked-in
# assumptions.  So, execution on a non-cluster host is not yet
# supported.
#
# Onward!


_args = None                    # argparse namespace
_pgm = None                     # program name
_verb = None                    # program action
_iquery = None                  # iquery client
_instances = None               # list('instances') output
_cfg = None                     # bunch of config derived from _args
_iolock = mp.Lock()             # avoid intermingled subprocess output
_main_proc = mp.current_process()
_stderr = sys.stderr            # Test harness?  Have to use stdout, grrr.
_use_caches = False             # OK to use "mutable default idiom" caches?

LOG_LINE_SEP = "\n   ....:   "  # Used for continuation of long log messages
ZIP_POOL_REFRESH = 50           # Every so often, get a new set of pool procs
ZIP_ERROR_TIMEOUT = 20.0        # On error don't wait forever for zip/unzip

SAVE_FORMATS = ('binary', 'opaque', 'tsv+')
RESTORE_FORMATS = (
    'binary',
    'text',                     # UNSUPPORTED
    'store',                    # UNSUPPORTED
    'tsv+',
    'opaque',
    'undef',                    # dummy (restore can get format from manifest)
)


@contextmanager
def ScopedLock(lk=None):
    """Used for locking console I/O."""
    if lk:
        lk.acquire()
    yield
    if lk:
        lk.release()


def _log(*args, **kwargs):
    """Internal routine for status logging."""
    try:
        stream = kwargs['file']
    except KeyError:
        stream = sys.stdout
    if mp.current_process() is _main_proc:
        prefix = _pgm
    else:
        prefix = '[%s]' % mp.current_process().name
    try:
        lock = None if kwargs['nolock'] else _iolock
    except KeyError:
        lock = _iolock
    with ScopedLock(lock):
        print >>stream, prefix, ' '.join(str(x) for x in args)
        stream.flush()


def dbg(*args, **kwargs):
    """If verbose, print to stderr with locking."""
    if _args.verbose:
        kwargs['file'] = _stderr
        a = ["[D]"]
        a.extend(args)
        _log(*a, **kwargs)


def warn(*args, **kwargs):
    """Print to stderr with locking."""
    kwargs['file'] = _stderr
    a = ["[W]"]
    a.extend(args)
    _log(*a, **kwargs)


def prt(*args, **kwargs):
    """Bare print with locking."""
    _log(*args, **kwargs)


def wrap(x, linesep=LOG_LINE_SEP):
    """Wrap and fill long lists of things to be logged"""
    if not isinstance(x, basestring):
        x = ', '.join(str(y) for y in x)
    lines = textwrap.wrap(x, width=(72 - len(linesep)))
    return linesep.join(lines)


class Bunch(object):
    """See _Python Cookbook_ 2d ed., "Collecting a Bunch of Named Items"."""
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def no_ver_name(name):
    """Strip version suffix from array name."""
    return name.split('@')[0]


def pretty_iid(iid):
    """Make less-than-pretty raw instance_id number into a nice string."""
    return "srv-{0}-inst-{1}".format(iid >> 32, iid & 0xFFFFFFFF)


def is_linear_format(fmt):
    """Return True iff ingesting this backup format needs a redimension."""
    # Saved in "tsv+" means ingested in "tsv".
    return fmt.startswith('(') or fmt in ('tsv+', 'tsv', 'binary')


def is_deprecated_format(fmt):
    """Return True iff this backup format is no longer supported.

    For --restore we'll try to read it, but no promises.
    """
    return fmt in ('text', 'store')


def convert(x):
    """Attempt to convert types to Python equivalents."""
    try:
        return int(x)
    except ValueError:
        pass
    try:
        return long(x)  # Brian is even more paranoid than me!
    except ValueError:
        pass
    try:
        return float(x)
    except ValueError:
        pass
    xx = x.lower().strip()
    if xx == 'true':
        return True
    if xx == 'false':
        return False
    return x


def make_unpacked_schema(schema):
    """Transform an input schema as unpack() would.

    When saving or restoring binary format, the "ingest schema" is a
    1-D schema containing the dimensions of the original array as
    attributes.  This is what the unpack() operator does, but unpack()
    also re-chunks the array, changing its distribution.  That's slow.
    Instead, we'll save using the same ingest schema that unpack()
    would have generated, but without redistributing the chunks.
    Since we're getting rid of unpack() from the save query, we may as
    well generate the unpacked schema (that is, the ingest schema) by
    hand too.
    """
    attrs, dims = Schema.parse(schema)
    new_attrs = []
    chunk_size = 1
    for d in dims:
        new_attrs.append(Schema.Attribute(
            d.name, 'int64', False, None, None, None))
        try:
            # If we can match the logical chunk size of the array,
            # the save should go even faster.
            chunk_size *= d.chunk
        except TypeError:
            # If array is empty it could still be auto-chunked, i.e. '*'
            chunk_size = 0
    if not chunk_size:
        chunk_size = 1000000
    new_dim = Schema.Dimension('__row', 0, '*', chunk_size, 0)
    new_attrs.extend(attrs)
    result = Schema.unparse(new_attrs, [new_dim])
    dbg("Unpack", schema, "as", result)
    return result


def make_binary_save_query(schema):
    """Build an AFL query to unpack() an array with this schema.

    Per make_unpacked_schema() above, we'll avoid using the actual
    unpack() operator for efficiency reasons.  The '_dim_' prefix is
    necessary because a schema can't contain two variables with the
    same name.
    """
    attrs, dims = Schema.parse(schema)
    apply_pairs = [("_dim_{0}".format(d.name), d.name) for d in dims]
    a_names = [x[0] for x in apply_pairs]
    a_names.extend([a.name for a in attrs])
    query = "project(apply({{0}}, {0}), {1})".format(
        ', '.join(','.join(x) for x in apply_pairs),
        ', '.join(a_names))
    dbg("Saving", schema, "with", query)
    return query


def reject(ns='', array='', _regex=[]):
    """Should array or namespace be rejected based on command line options?"""
    assert ns or array, "Internal error: bad reject() call"
    what = '.'.join((ns if ns else '*', array if array else '*'))
    if ns:
        if _args.include_ns and ns not in _args.include_ns:
            dbg("Reject", what, "(ns not included)")
            return True
        if _args.exclude_ns and ns in _args.exclude_ns:
            dbg("Reject", what, "(ns excluded)")
            return True
    if array:
        if _args.all_versions and '@' not in array:
            # Must reject the unversioned array, otherwise we get two
            # copies of the latest version.
            dbg("Reject", what, "(unversioned)")
            return True
        if _args.filter:
            if not _regex or not _use_caches:
                del _regex[:]
                _regex.append(re.compile(_args.filter))
            if not _regex[0].match(array):
                dbg("Reject", what, "(filter)")
                return True
    return False                # Not rejected, yay!


def local_archive(ii):
    """Derive the local archive folder name on instance ii."""
    if _args.parallel:
        field = 'No' if _args.sequential_ids else 'instance_id'
        try:
            iid = ii[field]
        except TypeError:
            iid = ii._asdict()[field]
        folder = '.'.join((_args.folder, str(iid)))
    else:
        folder = _args.folder
    return folder


# This is a normalized Manifest entry.
ArrayMetadata = namedtuple(
    'ArrayMetadata',
    ("name",                    # array name
     "ns",                      # namespace, default 'public'
     "format",                  # format, may be per-array for binary backups
     "uaid",                    # unversioned array id (v0 compatibility)
     "file_name",               # where array data is kept in archive folder
     "schema",                  # schema
     "ingest_schema"))          # flat schema of unpacked data (if needed)


class Manifest(object):

    """
    A class to represent archive folder .manifest files.
    """

    _BEGIN_METADATA = "%{"
    _END_METADATA = "%}"

    def __init__(self, filename=None):
        self.clear()
        if filename is not None:
            self.load(filename)

    def clear(self):
        """Reset internal state to freshly minted object."""
        self.metadata = dict()
        self.filename = None
        self.items = []
        self._lineno = 0

    @property
    def format(self):
        if 'format' in self.metadata:
            return self.metadata.get('format')
        if 'save-options' in self.metadata:
            m = re.search(r'--save\s+(\S+)', self.metadata.get('save-options'))
            if m:
                return m.group(1)
        return None

    def _get_simple_property(self, foo):
        """Common code for extracting most properties from the metadata dict.

        Originally the metadata only contained the 'version' number
        and a 'save-options' string generated by _buildSaveOpts().
        Now that --restore parses the manifest to get the format etc.,
        we don't want to be reparsing the 'save-options' all the time,
        so individual options now have their own entries in the
        metadata.  We still have to be able to read old manifests
        though, so if the desired option doesn't have its own entry we
        try to find it in 'save-options'.  This is simple to do for
        all options other than 'format', which must do more work (see
        above).  Ditto for --allVersions below.
        """
        if foo in self.metadata:
            return self.metadata.get(foo)
        if 'save-options' in self.metadata:
            return ("--%s" % foo) in self.metadata.get('save-options').split()
        return None

    @property
    def all_versions(self):
        # Have to support --allVersions variant, mumble...
        result = self._get_simple_property('all-versions')
        if result is None:
            result = self._get_simple_property('allVersions')
        return result

    @property
    def zipped(self):
        return self._get_simple_property('zip')

    @property
    def parallel(self):
        return self._get_simple_property('parallel')

    @property
    def sequential_ids(self):
        return self._get_simple_property('sequential-ids')

    @property
    def folder(self):
        return self.metadata.get('_folder')

    @folder.setter
    def folder(self, value):
        # We mark the manifest with the folder we fetched it from, but
        # that info's not part of the manifest *per se*---hence the
        # leading underscore.
        assert isinstance(value, basestring)
        self.metadata['_folder'] = value

    @property
    def version(self):
        return self.metadata.get('version')

    @version.setter
    def version(self, value):
        assert isinstance(value, int)
        self.metadata['version'] = value

    def __len__(self):
        return len(self.items)

    def __nonzero__(self):
        return bool(self.items)

    __bool__ = __nonzero__      # python3 compatibility (maybe some day)

    def __iter__(self):
        return iter(self.items)

    def update(self, *args, **kwargs):
        """Update metadata section's key/value pairs."""
        self.metadata.update(*args, **kwargs)

    def append(self, item):
        if isinstance(item, ArrayMetadata):
            self.items.append(item)
        else:
            raise ValueError("Cannot append {0} to Manifest".format(item))

    def extend(self, item_list):
        def bad_item(x):
            return not isinstance(x, ArrayMetadata)
        if any(map(bad_item, item_list)):
            raise ValueError("Cannot extend {0} into Manifest".format(
                item_list))
        self.items.extend(item_list)

    def prune(self):
        """Throw away all rejected items."""
        count = len(self.items)
        dbg("Pruning", count, "manifest items")
        new_items = []
        for item in self.items:
            if not reject(ns=item.ns, array=item.name):
                new_items.append(item)
        self.items = new_items
        dbg("Pruned", len(self.items) - count, "of", count, "manifest items")

    def load(self, filename):
        """Read and parse a .manifest file.

        If the first line of the file is _BEGIN_METADATA, parse the metadata
        and continue.  Otherwise this is a version 0 manifest file.
        """
        self.clear()
        self.filename = filename
        with open(filename) as F:
            line1 = F.readline().strip()
            self._lineno = 1
            if line1 == Manifest._BEGIN_METADATA:
                self._parse_metadata(F)
                assert 'version' in self.metadata, (
                    "%s: No version found in metadata" % filename)
                method = "_load_v%d" % self.metadata['version']
                try:
                    Manifest.__dict__[method](self, F)
                except KeyError:
                    raise RuntimeError(
                        "Manifest file version {0} not supported".format(
                            self.version))
            elif '|' in line1:
                # A version 1 file, but written before we implemented
                # the metadata section.  No problem.
                self.metadata['version'] = 1
                self._load_v1(F, line1)
            else:
                self.metadata['version'] = 0
                self._load_v0(F, line1)

    def _parse_metadata(self, FH):
        """Parse metadata section of .manifest file.

        The format of the metadata section is as similar to config.ini
        as we can make it without actually pulling in a config.ini
        parser.  (If we eventually do, please make it
        https://wiki.python.org/moin/ConfigObj .)

            %{
            # Comments are OK.
            version = 1
            %}
        """
        line = None
        while True:
            line = FH.readline()
            if not line:
                raise RuntimeError(
                    "Manifest file metadata section is malformed")
            line = line.strip()
            if not line:
                continue        # Blank line.
            if line == Manifest._END_METADATA:
                break
            key, _, value = line.partition('=')
            key = key.strip()
            if key.startswith('#'):
                continue        # Comments allowed.
            self.metadata[key] = convert(value.strip())

    def _load_v0(self, FH, line1):
        """Read and parse a version 0 .manifest file (15.7 and earlier).

        @param FH    file handle open for read
        @param line1 first line of file (was already read in)

        @note The v0 file format did not explicitly record binary schemas, so
              these must be derived later.  See fix_v0_schemas().
        """
        NS = "public"
        fmt = _args.restore
        INGEST_SCHEMA = ''
        line = line1
        while True:
            # Use partition and not split because schema contains commas.
            array, _, rest = line.partition(',')
            uaid, _, schema = rest.partition(',')
            self.items.append(ArrayMetadata(array, NS, fmt, int(uaid),
                                            array, schema, INGEST_SCHEMA))
            line = FH.readline()
            if not line:
                break
            self._lineno += 1

    def _load_v1(self, FH, line1=None):
        """Read and parse a version 1 .manifest file (15.12 and maybe later).

        @param FH    file handle open for read
        @param line1 first line for older v1 manifests that had no metadata
        """
        def _load_v1_line(line):
            fields = line.strip().split('|')
            fields.insert(3, None)  # No uaid in v1 manifests, use None.
            if len(fields) + 1 == len(ArrayMetadata._fields):
                # ingest_schema (fka bin_schema) was optional in early
                # v1 manifests.
                fields.append(None)
            elif len(fields) != len(ArrayMetadata._fields):
                raise RuntimeError(
                    "{0}:{1} - {2} fields but expecting {3}: {4}".format(
                        self.filename, self._lineno, len(fields),
                        len(ArrayMetadata._fields), line))
            self.items.append(ArrayMetadata._make(fields))
        if line1:
            _load_v1_line(line1)
        for line in FH:
            self._lineno += 1
            _load_v1_line(line)

    def __str__(self):
        """Print in v1 format."""
        # Build metadata section.  It would be nice if entries were
        # displayed in some deterministic order, but for now the
        # dict() hash order is fine.  See collections.OrderedDict.
        if 'version' not in self.metadata:
            # Probably because this Manifest was built with
            # extend()/append() rather than load().
            self.metadata['version'] = 1
        lines = [Manifest._BEGIN_METADATA]
        # Metadata items beginning with '_' are not written.
        lines.extend([" = ".join((k.lower(), str(self.metadata[k])))
                      for k in self.metadata if not k.startswith('_')])
        lines.append(Manifest._END_METADATA)

        # Build per-array data lines.
        for item in self.items:
            lines.append('|'.join((item.name,
                                   item.ns,
                                   item.format,
                                   item.file_name,
                                   item.schema,
                                   item.ingest_schema)))
        return '\n'.join(lines)


def make_array_metadata(name, schema, ns='public'):
    """Create an ArrayMetadata object for array with 'name' and 'schema'."""

    # I hesitate to make this a Manifest method because there's just too
    # much unnecessary coupling here.  So I leave it as an external
    # method.  All this code was formerly bundled into class
    # array_metadata, in its constructor no less.  Ugh.

    def strip_array_name(schema):
        return re.sub(r'^[^<]*<', '<', schema, count=1)

    schema = strip_array_name(schema)
    ingest_schema = ''
    if ns == 'public':
        file_name = name
    else:
        file_name = '.'.join((name, ns))

    # We only need to label metadata with a format when using --save.
    # Other verbs won't bother to look at md.format or its derivatives.
    fmt = _args.save if _args.save else None

    if fmt == 'binary':
        # For binary, we also need to generate:
        # - the full binary format string for the unpacked array, and
        # - its corresponding unpacked 1-D schema.
        # See the SciDB Reference Guide sections on the unpack() operator
        # and on "Binary Format Strings".

        ingest_schema = make_unpacked_schema(schema)

        # Build up the binary format strings from the attribute types.
        attrs, _ = Schema.parse(ingest_schema)
        types = [x.type for x in attrs]
        nulls = [" null" if x.nullable else "" for x in attrs]
        fmt = "({0})".format(
            ','.join((''.join(x) for x in zip(types, nulls))))

    elif fmt == 'tsv+':
        # Similary for tsv+ we must generate the unpacked 1-D schema of the
        # data as saved: one int64 attribute prepended for each
        # dimension, and exactly one artificial __row dimension.

        ingest_schema = make_unpacked_schema(schema)

    # Finally, construct and return some ArrayMetadata.
    return ArrayMetadata._make((name, ns, fmt, None,  # uaid
                                file_name, schema, ingest_schema))


def fix_v0_schemas(manifest):
    """Fix a v0 manifest: resolve binary schemas, adjust attribute nullability.

    @description The v0 .manifest file format does not contain explicit
    binary schemas, so we may need to derive them at runtime using the same
    queries used by the 15.7 version of this script.

    @p In addition, in 15.12 (v1), attributes became nullable by
    default.  All schemas from the v0 manifest have to be adjusted
    accordingly to preserve the semantics of the original array.

    @param manifest a v0 Manifest object

    @return v0 manifest with fixed 'format', 'schema', and
            'ingest_schema' values
    """
    assert manifest.version == 0, (
        "Implicit binary schema for v%d manifest??" % manifest.version)
    assert _iquery.format == 'tsv', "Internal error, I want my TSV"

    SHOW_QUERY = r"show('unpack(input({0}, \'/dev/null\'),__row)', 'afl')"

    result = Manifest()
    for md in manifest:
        assert md.ns == 'public', "Non-public namespace in v0 manifest?!"

        # The v0 md.schema was recorded by 15.7, with attributes NOT
        # NULL by default.  Now attributes default to nullable, so we
        # must reconstruct the schema to get correct nullability.
        attrs, dims = Schema.parse(md.schema, default_nullable=False)
        schema = Schema.unparse(attrs, dims, default_nullable=True)

        ingest_schema = md.ingest_schema
        format_ = md.format
        if md.format == 'binary':

            # Run 'show' query to infer binary backup file's schema.
            # (We intentionally do not use make_unpacked_schema() here:
            # this is backward compatibility code, so why mess with it.)
            assert not md.ingest_schema
            _iquery.namespace = None  # md.ns == 'public', see above
            query = SHOW_QUERY.format(schema)
            ingest_schema, err = _iquery(query)
            if _iquery.returncode:
                raise AppError("iquery: {0}\nError: {1}".format(query, err))

            # Derive the binary format string from the binary schema.
            attrs, dims = Schema.parse(ingest_schema)
            types = [x.type for x in attrs]
            nulls = [" null" if x.nullable else "" for x in attrs]
            format_ = "({0})".format(
                ','.join((''.join(x) for x in zip(types, nulls))))

        result.append(
            ArrayMetadata._make((md.name, md.ns, format_, md.uaid,
                                 md.file_name, schema, ingest_schema)))
    result.version = 0
    return result


def set_options_from_manifest(manifest):
    """On --restore, acquire options from the archive we are restoring."""

    # It was silly to require a format argument to the --restore
    # option.  Ignore it, but warn if it doesn't match.
    if _args.restore not in (None, "undef", manifest.format):
        warn("Backup saved in", manifest.format, "format (and not",
             _args.restore, "format), proceeding anyway...")

    if is_deprecated_format(manifest.format):
        warn("Format", manifest.format, "unsupported, proceeding anyway...")

    _args.restore = manifest.format
    dbg("Set --restore={0} from manifest".format(_args.restore))

    if manifest.all_versions is not None:
        _args.all_versions = manifest.all_versions
        dbg("Set --all-versions={0} from manifest".format(_args.all_versions))
    if manifest.zipped is not None:
        _args.zip = manifest.zipped
        dbg("Set --zip={0} from manifest".format(_args.zip))
    if manifest.parallel is not None:
        _args.parallel = manifest.parallel
        dbg("Set --parallel={0} from manifest".format(_args.parallel))
    if manifest.sequential_ids is not None:
        _args.sequential_ids = manifest.sequential_ids
        _args.raw_ids = not _args.sequential_ids
        dbg("Set --sequential_ids={0} from manifest".format(
            _args.sequential_ids))


def unbounded(schema, format_):
    """Edit schema's 0-th dimension high bound to '*' if 'format' needs it.

    @description
    Parallel input() operator now requires an unbounded
    first dimension, since new chunk hashing/partitioning schemes are
    not guaranteed to evenly distribute chunks across instances.  The
    slowest-changing dimension boundary must be relaxed, so that each
    instance can always find a place to put chunks from the local
    input file.  (However, this is not a requirement for 'opaque'
    format parallel loads, so this function is a no-op for that case.)

    @see SDB-3245
    @see SDB-5611
    """
    if format_.lower() == 'opaque':
        return schema
    attrs, dims = Schema.parse(schema)
    d0 = Schema.Dimension(dims[0].name,
                          dims[0].lo,
                          '*',
                          dims[0].chunk,
                          dims[0].overlap)
    dims[0] = d0
    dbg("Using unbounded schema dimensions:", dims)
    return Schema.unparse(attrs, dims)


def list_namespaces():
    """Return list of existing namespaces."""
    out, err = _iquery("project(list('namespaces'), name)")
    if _iquery.returncode:
        raise AppError("Cannot list namespaces:\n{0}".format(err))
    return out.splitlines()


def prepare_namespace(ns, _namespaces=set(), _warned=[]):
    """If namespace doesn't exist, try to create it.

    Called exclusively during --restore operations to create missing
    namespaces.  There are several possibilities:

    - ns already exists.  No action, return True.
    - ns successfully created.  Return True (namespace is prepared).
    - ns create failed: no plugin.  Print one-time warning and return False.
    - ns create failed: other reason.  Raise an AppError.

    This function uses Python mutable defaults to store state across
    calls.  (Search for "Python mutable defaults" on StackOverflow if
    this technique is unfamiliar.)  But when run from the
    backup.features test (_use_caches == False) we can't use this
    caching technique, since the test code might change the valid
    namespaces out from under us.
    """
    # Paranoid.  One day namespaces may nest, so always create new ones
    # from the "top" namespace, which presumably is None.
    saved_ns = _iquery.namespace
    _iquery.namespace = None
    try:
        if not _namespaces or not _use_caches:
            _namespaces.clear()
            for x in list_namespaces():
                _namespaces.add(x)
            assert _namespaces, "No namespaces listed?!"  # Not even public?!
        if ns in _namespaces:
            return True
        if _warned:
            return False        # Already tried and failed: plugin missing.
        _, err = _iquery("create_namespace('%s')" % ns)
        if _iquery.returncode:
            if 'SCIDB_SE_QPROC::SCIDB_LE_LOGICAL_OP_DOESNT_EXIST' in err:
                # Operator missing means plugin not loaded.
                if not _warned:
                    _warned.append(True)
                    warn(textwrap.dedent("""
                        The namespace management plugin is not loaded.
                        Arrays in non-public namespaces will not be
                        restored.  To restore them, load the
                        'namespaces' plugin or try the --use-public-ns
                        option.""").strip())
                return False
            else:
                raise AppError(
                    "Cannot create namespace '{0}':\n{1}".format(ns, err))
        else:
            # Successfully created ns!
            _namespaces.add(ns)
            return True
    finally:
        _iquery.namespace = saved_ns


def _gather_manifest_from_scidb():
    """Collect array metadata for arrays in SciDB.

    If this is a --save we'll ignore arrays that don't match the
    command line selection criteria, otherwise we want everything.

    @return a Manifest object reflecting the selected arrays
    """
    assert _iquery.format == 'tsv', (
        "I want my... I want my... I want my TSV!!!")  # That ain't workin'.

    if _args.all_versions:
        # The sort keeps versions of the same array together.
        query = "sort(filter(list('arrays', true), temporary=false), aid)"
    else:
        query = "filter(list('arrays'), temporary=false)"

    # Extract array name listings from each namespace.
    manifest = Manifest()
    for ns in list_namespaces():
        if _verb == 'save' and reject(ns=ns):
            continue
        try:
            arrays = make_table('_'.join(("ArrayEntry", ns)),
                                query, namespace=ns)
        except RuntimeError as e:
            raise AppError(
                "Cannot list arrays in namespace '{0}':\n{1}".format(
                    ns, e))

        for a in arrays:
            if _verb == 'save' and reject(array=a.name):
                continue
            md = make_array_metadata(a.name, a.schema, ns)
            manifest.append(md)

    return manifest


def make_folders():
    """Create backup folders prior to --save.

    This function creates the backup folder(s) needed to save data.
    If one or more of them already exist, that's an error.  If the
    --force option was given, we'll remove them and recreate them from
    scratch.

    We use the same SSH-in-a-pool-process code path for both local
    non-parallel and remote parallel cases.  That's intentional, to
    reduce code duplication and to allow the script to run on
    non-cluster hosts (not yet fully tested, however!).  XXX Test it.
    """
    assert _args.folder, "No folder given?!"

    if _args.parallel:
        assert _instances, "Parallel operation but no instances?!"
        remotes = _instances
    else:
        assert _cfg.coordinator, "Uncoordinated!"
        remotes = [_cfg.coordinator]

    pool = mp.Pool(len(remotes))
    args = [x._asdict() for x in remotes]
    results = pool.map(_mpw_make_folders, args)
    pool.close()
    pool.join()
    if not all(results):
        raise AppError("Cannot create archive {0}[.<instance>] on one"
                       " or more instances".format(_args.folder))


def _mpw_make_folders(ii):
    """Background worker for make_folders().

    @param ii a dict() created from an InstanceInfo namedtuple
    """
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        folder = local_archive(ii)
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        ssh.run("test -e %s" % folder)
        if ssh.ok():
            if _args.force:
                _, err = ssh.run("rm -rf %s" % folder)
                if ssh.failed():
                    warn("Cannot remove {0}: {1}".format(folder, err))
                    return False
            else:
                warn("Folder", folder, "already exists on server", ii['name'],
                     ", use --force to overwrite or --delete to remove")
                return False
        _, err = ssh.run("mkdir -p %s" % folder)
        if ssh.failed():
            warn("Cannot create folder {0} on server {1}: {2}".format(
                folder, ii['name'], err))
            return False

        # All good!
        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def check_folders():
    """Look for backup folders prior to --restore.

    This function makes sure that the backup folder(s) needed for the
    restore are in place.  (If they are not, one reason may be that the
    wrong --sequential-ids/--raw-ids option was used.)
    """
    assert _args.folder, "No given folder?!"

    if _args.parallel:
        assert _instances, "Parallel operation but no instances?!"
        remotes = _instances
    else:
        assert _cfg.coordinator, "Uncoordinated!"
        remotes = [_cfg.coordinator]

    assert _instances, "Parallel operation but no instances?!"
    pool = mp.Pool(len(remotes))
    args = [x._asdict() for x in remotes]
    results = pool.map(_mpw_check_folders, args)
    pool.close()
    pool.join()
    if not all(results):
        raise AppError(
            "Folder {0}.<instance> missing from one or more instances".format(
                _args.folder))


def _mpw_check_folders(ii):
    """Remote worker for check_folders().

    @param ii a dict() created from an InstanceInfo namedtuple
    """
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        folder = local_archive(ii)
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        if not check_one_folder(ssh, folder):
            return False        # error already logged

        # All looks good.
        dbg("Folder", folder, "present and writeable on server", ii['name'])
        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def check_one_folder(ssh, folder):
    """Use an SshRunner to access check the given folder."""
    ssh.run("test -d %s" % folder)
    if ssh.failed():
        warn("Folder", folder, "on server", ssh.host,
             "missing or not a directory")
        return False
    # Per-process filename prevents inter-process races.
    testfile = "{0}/.access_check_{1}".format(
        folder, mp.current_process().name)
    _, err = ssh.run("touch {0} && rm {0}".format(testfile))
    if ssh.failed():
        warn("Folder", folder, "is not writeable on server", ssh.host)
        return False
    return True


class MadeSymlinks(object):

    """
    Context manager for creation and cleanup of --parallel symbolic links.

    Suppose the archive folder is "/some/path/bkp42".  In --parallel
    mode, each instance's data directory must contain a symbolic link
    "bkp42" to its per-instance backup folder "/some/path/bkp42.<N>"
    so that a parallel save() (or input()) query can use a relative
    path "bkp42/filename" to write (or read) an array to that backup
    folder (and not just leave it in the data directory).

    Without --parallel, this context manager is a no-op.

    When --zip is used, symbolic links are not needed.  See MadePipes
    below.
    """

    def __enter__(self):
        """Create symbolic links from data directory to backup directory."""
        if not _args.parallel:
            return self
        assert _instances, "Parallel operation but no instances?!"
        pool = mp.Pool(len(_instances))
        args = [x._asdict() for x in _instances]
        results = pool.map(_mpw_make_symlinks, args)
        pool.close()
        pool.join()
        if not all(results):
            raise AppError(
                "Cannot create symbolic link '{0}' on one or more"
                " instances".format(os.path.basename(_args.folder)))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Remove backup folder symlinks from all instances."""
        if not _args.parallel:
            return False
        assert _instances, "Parallel operation but no instances?!"
        pool = mp.Pool(len(_instances))
        args = [x._asdict() for x in _instances]
        results = pool.map(_mpw_remove_symlinks, args)
        pool.close()
        pool.join()
        if not all(results):
            if exc_val is None:
                raise AppError(
                    "Cannot remove symlink to {0}.<instance> on one or more"
                    " instances".format(_args.folder))
            else:
                # Don't mask the original exception being raised, just log it.
                warn("Cannot remove symlink to {0}.<instance> "
                     "on one or more instances".format(_args.folder))
        return False            # exc_val not handled


def _mpw_make_symlinks(ii):
    """Background worker for entering MadeSymlinks context."""
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        assert _args.parallel, "Symlinks only needed if --parallel"
        folder = local_archive(ii)
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        # Create the symlink in the instance data directory.  I guess
        # it's OK (but weird) if a correct symlink already exists.
        slink = os.sep.join((ii['instance_path'],
                             os.path.basename(_args.folder)))
        ssh.run("test -e %s" % slink)
        if ssh.ok():
            # Symlink path exists!  Is it a symlink?
            out, err = ssh.run("readlink -n %s" % slink)
            if ssh.ok():
                # It is!  Is it the *right* symlink?
                if out.strip() == folder:
                    dbg("Symbolic link", slink, "already present but looks OK")
                    return True
                if not _args.force:
                    # Wrong symlink and not empowered to do anything about it.
                    warn("Stale symbolic link", slink, "-->", out.strip(),
                         "should point at", folder, "use --force to overwrite")
                    return False
                # Remove it!
                _, err = ssh.run("rm -rf %s" % slink)
                if ssh.failed():
                    warn("Cannot remove {0}: {1}".format(slink, err))
                    return False

        # Symlink missing or removed above.  Create it!
        _, err = ssh.run("ln -snf {0} {1}".format(folder, slink))
        if ssh.failed():
            warn("Cannot create symbolic link {0}: {1}".format(slink, err))
            return False

        # All good!
        dbg("(datadir)/{0} --links-to--> {1}".format(
            os.path.basename(slink), folder))
        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def _mpw_remove_symlinks(ii):
    """Background worker for leaving MadeSymlinks context.

    @param ii a dict() created from an InstanceInfo namedtuple
    """
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        assert _args.parallel, "Symlinks only needed if --parallel"
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)
        slink = os.sep.join((ii['instance_path'],
                             os.path.basename(_args.folder)))

        _, err = ssh.run("rm -f %s" % slink)
        if ssh.failed():
            warn("Cannot remove symlink {0}:\n{1}".format(slink, err))
            return False

        dbg("Symlink %s removed from data directory" % os.path.basename(slink))
        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


class MadePipes(object):

    """
    Context manager for creation and cleanup of --zip mode pipes.

    Used only when --zip is specified.  For an array ns1.ALPHA we will
    create a FIFO in the data directory named ALPHA.ns1 and the gzip
    process on the other end of the pipe will redirect to ALPHA.ns1 in
    the archive folder.

    Because the gzip process handles redirection to/from the archive
    folder, there's never a need for symbolic links when --zip is
    used.  See MadeSymlinks above.
    """

    def __init__(self, remotes, pipe_name, pool=None):
        assert _args.zip, "Not zipping, so no pipes allowed!"
        assert _verb in ('save', 'restore'), "Uh oh, I'm verb-challenged"
        self.remotes = remotes
        self.name = pipe_name
        if pool is None:
            self.pool = mp.Pool(len(remotes))
            self.my_pool = True
        else:
            # Have to trust that pool has a worker proc for each
            # remote, since mp.Pool doesn't support len().
            self.pool = pool
            self.my_pool = False

    def _launch_workers(self, create=None):
        args = zip([x._asdict() for x in self.remotes],
                   [self.name] * len(self.remotes),
                   [create] * len(self.remotes))
        return self.pool.map(_mpw_pipe_worker, args)

    def __enter__(self):
        """Create target pipes."""
        if not all(self._launch_workers(create=True)):
            raise AppError("Cannot create {0} fifo on one or more instances"
                           .format(self.name))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up pipes on all remotes."""
        results = self._launch_workers(create=False)
        if self.my_pool:
            dbg("Closing my pipe_worker pool")
            self.pool.close()
            self.pool.join()
        if not all(results):
            raise AppError("Cannot remove {0} fifo on one or more instances"
                           .format(self.name))
        return False            # exception not handled


def _mpw_pipe_worker(args):
    """Make (or remove) a fifo in the data directory."""
    ii, pipe_name, create = args
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        path = os.path.join(ii['instance_path'], pipe_name)
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        _, err = ssh.run("rm -f %s" % path)
        if ssh.failed():
            warn("Cannot remove {0} from data directory: {1}".format(
                pipe_name, err))
            return False

        if create:
            _, err = ssh.run("mkfifo %s" % path)
            if ssh.failed():
                warn("Cannot create fifo {0} in data directory: {1}".format(
                    pipe_name, err))
                return False

        if create:
            dbg("Created fifo", path)
        else:
            dbg("Removed fifo", path)
        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def _buildSaveOpts():
    """Record options used during the save operation to prevent
       acidental option mismatches during the restore operations."""
    text = []
    if _args.save:
        text.extend(['--save', _args.save])
    if _args.parallel:
        text.append('--parallel')
    if _args.sequential_ids:
        text.append('--sequential-ids')
    if _args.all_versions:
        text.append('--all-versions')
    if _args.filter:
        text.extend(['--filter', _args.filter])
    if _args.zip:
        text.append('-z')
    assert _args.folder, "Saving but no folder?!"
    text.append(_args.folder)
    return ' '.join(text)


def fetch_manifest(coordinator, throw=False):
    """Retrieve the manifest from the coordinator.

    We were given an archive _args.folder argument, we know the
    coordinator, and we know there's a manifest out there somewhere.
    But there's a chicken/egg problem: unless we know the state of
    several flags used when writing the manifest (--parallel,
    --sequential-ids), we don't know where the manifest is.  But to
    learn how the archive was saved, we have to find its manifest.

    Break the chicken/egg cycle!  Go out to the coordinator, look for
    the manifest in all possible locations, and bring back the first
    one you find.
    """

    # Prefer bare folder name over --sequential-ids name over
    # --raw-ids name.
    paths = [
        _args.folder,
        '.'.join((_args.folder, str(coordinator.No))),
        '.'.join((_args.folder, str(coordinator.instance_id)))
    ]

    ssh = SshRunner(host=coordinator.name, port=_args.ssh_port)

    for path in paths:
        filename = os.path.join(path, ".manifest")
        with tempfile.NamedTemporaryFile() as TMP:
            _, err = ssh.get_file(filename, TMP.name)
            if ssh.failed():
                continue
            manifest = Manifest(TMP.name)
            if not manifest:
                raise AppError("Empty manifest in %s" % path)
            if manifest.version == 0:
                manifest = fix_v0_schemas(manifest)
            manifest.folder = path
            return manifest

    # Still here?  Ouch.
    error = "Manifest for {0} missing on {1}, not in any of: {2}".format(
        _args.folder, coordinator.name, paths)
    if throw:
        raise AppError(error)
    else:
        warn(error)
        return None


def setup():
    """Common initialization based on program arguments etc."""

    dbg("Main process pid", os.getpid())

    # Instance information!
    global _instances
    _instances = make_table('InstanceInfo', "list('instances')")
    assert _instances, "Cannot list SciDB instances?!"

    # Collect derived configuration here.
    global _cfg
    _cfg = Bunch()

    # We know make_table() succeeded, so use coordinator host and port
    # from that if we didn't get more explicit guidance.  The _iquery
    # object obeyed IQUERY_HOST and IQUERY_PORT envariables.
    def first_of(foo):
        return [x for x in foo if x][0]
    coord_host = first_of((_args.host, _iquery.host, _instances[0].name))
    coord_port = int(first_of((_args.port, _iquery.port, _instances[0].port)))

    # Which instance am I using as query coordinator?
    _cfg.coordinator = None
    for inst in _instances:
        if inst.port == coord_port and inst.name == coord_host:
            _cfg.coordinator = inst
    if _cfg.coordinator is None:
        raise AppError(
            "Coordinator {0} is not an instance?!\n".format(coord_host) +
            textwrap.dedent("""
                (Maybe default iquery host does not match cluster
                configuration.  For example, '127.0.0.1' != 'localhost',
                or 'foo.example.com' != 'foo'.  Check "list('instances')"
                output and try again with the -c/--host option.)
            """).strip())
    dbg("Coordinator:",
        ':'.join((_cfg.coordinator.name, str(_cfg.coordinator.port))))

    # Error injection for testing --ping.
    if 'PING_BAD_HOST' in os.environ:
        bad = _instances[-1]._replace(name='bad-host.example.com')
        _instances.append(bad)

    # Normalize the base folder and compute derived folders and paths.
    if _args.folder:
        _args.folder = os.path.normpath(_args.folder)

        if _verb == 'save':
            # Easy, we're going to write it here:
            _cfg.manifest = None
            _cfg.manifest_folder = local_archive(_cfg.coordinator)
            _cfg.manifest_path = os.sep.join((_cfg.manifest_folder,
                                              '.manifest'))
        else:
            _cfg.manifest = fetch_manifest(_cfg.coordinator)
            if _cfg.manifest:
                _cfg.manifest_folder = _cfg.manifest.folder
                _cfg.manifest_path = os.sep.join((_cfg.manifest_folder,
                                                  '.manifest'))
            elif _verb in ('ping', 'delete'):
                # No manifest?   No problem.
                _cfg.manifest_folder = None
                _cfg.manifest_path = None
            else:
                raise AppError(
                    "Cannot find manifest for archive folder {0} on {1}"
                    .format(_args.folder, _cfg.coordinator.name))

    else:
        # Fill in corresponding Nones to avoid AttributeErrors.
        _cfg.manifest = None
        _cfg.manifest_folder = None
        _cfg.manifest_path = None

        # These verbs require a folder argument, but we cannot enforce
        # it via argparse without switching to using subcommands and
        # so breaking backward compatibility.  Enforce it here
        # manually.
        if _verb in ('save', 'restore', 'delete'):
            raise AppError(
                "The --{0} option requires a FOLDER argument".format(_verb))


def make_restore_query(md):
    """Create AFL query for restoring manifest entry md's array."""
    iid = '-1' if _args.parallel else '-2'
    if _args.zip:
        # Always datadir-relative: md.file_name will be a pipe.
        folder = ''
    elif _args.parallel:
        # Pre-made relative symbolic link in datadir.
        folder = os.path.basename(_args.folder)
    else:
        # Single target folder on coordinator.
        folder = _args.folder

    # Replace "tsv+" with "tsv".  There's no "tsv+" input format
    # because "+" (adorn with coordinates) only makes sense for
    # output.  The Right Thing will happen if we use plain old "tsv"
    # and the ingest_schema.
    fmt = "tsv" if md.format == "tsv+" else md.format

    # Set up the query and its arguments.
    if is_linear_format(fmt):
        # Linear formats need a redimension from ingest_schema to schema.
        query = """
            store(
              redimension(
                input({ingest}, '{path}', {iid}, '{fmt}'),
                {schema}),
              {array})
        """.format(ingest=md.ingest_schema,
                   path=os.path.join(folder, md.file_name),
                   iid=iid,
                   fmt=fmt,
                   schema=md.schema,
                   array=no_ver_name(md.name))
    else:
        if _args.parallel:
            schema = unbounded(md.schema, md.format)
        else:
            schema = md.schema
        query = """
            store(input({schema}, '{path}', {iid} ,'{fmt}'), {array})
        """.format(schema=schema,
                   path=os.path.join(folder, md.file_name),
                   iid=iid,
                   fmt=fmt,
                   array=no_ver_name(md.name))

    return query.strip()


def _mpw_restore_query(md):
    """Run restore query for manifest entry 'md' in the background."""
    mp.current_process().name = "restore({0})".format(md.file_name)
    try:
        query = make_restore_query(md)
        iquery = IQuery(afl=True, no_fetch=True)
        iquery.namespace = None if md.ns == 'public' else md.ns

        dbg("Querying:", query)
        _, err = iquery(query)
        if iquery.returncode:
            warn("Cannot restore array {0}.{1}:\n{2}".format(
                md.ns, md.name, err))
            return False

        dbg("Query done")
        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def _mpw_unzipper(args):
    """Decompress array data from archive into fifo.

    No symbolic link is needed, we decompress from the archive file
    into a fifo in the data directory.
    """
    ii, filename = args
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        # See MadePipes above re. how we know what to unzip, and where
        # to put it.
        pipe = os.path.join(ii['instance_path'], filename)
        source = os.path.join(local_archive(ii), filename)

        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        _, err = ssh.run("gzip -d < {0} > {1}".format(source, pipe))
        if ssh.failed():
            warn("Gunzip from {0} to {1} failed: {2}".format(
                source, pipe, err))
            return False

        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def restore_with_zip(manifest):
    """Restore arrays named in the manifest, uncompressing them."""

    if _args.parallel:
        remotes = _instances
    else:
        remotes = [_cfg.coordinator]

    restored_arrays = 0
    pool = mp.Pool(len(remotes))
    query_pool = mp.Pool(1)

    saved_ns = _iquery.namespace
    try:
        for i, md in enumerate(manifest):

            if ((i + 1) % ZIP_POOL_REFRESH) == 0:
                # Everybody out of the pool!
                dbg("Start pool refresh")
                pool.close()
                pool.join()
                query_pool.close()
                query_pool.join()
                pool = mp.Pool(len(remotes))
                query_pool = mp.Pool(1)
                dbg("Pool refresh done")

            # Handle any namespace difficulties.
            if _args.use_public_ns:
                md.ns = 'public'
            if not prepare_namespace(md.ns):
                warn("SKIPPED", '.'.join((md.ns, md.name)), "(no plugin)")
                continue
            _iquery.namespace = md.ns

            prt('Restoring zipped', '.'.join((md.ns, md.name)))

            with MadePipes(remotes, md.file_name, pool=pool):

                # Launch the restore query asynchronously in the
                # background.  It will block waiting for pipe writers.
                dbg("Launching restore query in background")
                query_result = []
                async_handle = query_pool.map_async(
                    _mpw_restore_query, [md], callback=query_result.extend)

                # Did we fail early for some reason?  Query should be blocked!
                async_handle.wait(1.0)
                if async_handle.ready():
                    raise AppError("Restore query aborted: %s" % query_result)

                # Launch the gzip pipe writers synchronously.
                dbg("Starting fifo writers")
                args = zip([x._asdict() for x in remotes],
                           [md.file_name] * len(remotes))
                results = pool.map(_mpw_unzipper, args)
                if not all(results):
                    raise AppError(
                        "One or more gzip writers failed for {0}".format(
                            md.file_name))

                # Collect the query process result.
                dbg("Waiting on query subprocess completion...")
                try:
                    async_handle.wait(ZIP_ERROR_TIMEOUT)
                except mp.TimeoutError:
                    warn("Timed out waiting for restore query to finish")
                else:
                    dbg("Restore query done:", query_result)
                    assert query_result, "Query done but result list empty?!"
                    if not all(query_result):
                        raise AppError(
                            "Restore of {0} reported failure".format(
                                md.file_name))

            restored_arrays += 1
    finally:
        _iquery.namespace = saved_ns
        prt('Restored', restored_arrays, 'of', len(manifest), 'arrays')


def restore_arrays(manifest):
    """Restore arrays named in the manifest, without decompression."""

    saved_ns = _iquery.namespace
    restored_arrays = 0

    try:
        for md in manifest:

            # Handle any namespace difficulties.
            if _args.use_public_ns:
                md.ns = 'public'

            if not prepare_namespace(md.ns):
                warn("SKIPPED", '.'.join((md.ns, md.name)), "(no plugin)")
                continue
            _iquery.namespace = md.ns

            prt('Restoring', '.'.join((md.ns, md.name)))
            query = make_restore_query(md)
            dbg("Query:", query)
            _, err = _iquery('--no-fetch', query)
            if _iquery.returncode:
                warn("Cannot restore {0}:\n{1}".format(
                    '.'.join((md.ns, md.name)), err))
            else:
                restored_arrays += 1

    finally:
        _iquery.namespace = saved_ns
        prt('Restored', restored_arrays, 'of', len(manifest), 'arrays')


def do_restore():
    """Restore array data."""
    dbg("Verb: restore (%s)" % _verb)

    # We should have retrieved the manifest during setup().
    if not _cfg.manifest:
        raise AppError("Cannot --restore without a manifest")
    set_options_from_manifest(_cfg.manifest)

    # Backup folder(s) present everywhere?
    check_folders()

    # Make sure all needed files are present in the archive folder(s).
    do_verify(_cfg.manifest)

    # Honor --include-ns/--exclude-ns/--filter options.
    _cfg.manifest.prune()

    if _args.force:
        # Big hammer... remove 'em all.
        saved_ns = _iquery.namespace  # paranoid
        try:
            for md in _cfg.manifest:
                _iquery.namespace = None if md.ns == 'public' else md.ns
                _, err = _iquery("remove({0})".format(no_ver_name(md.name)))
                if (_iquery.returncode and
                        'SCIDB_LE_ARRAY_DOESNT_EXIST' not in err):
                    raise AppError("Cannot remove {0}.{1}:\n{2}".format(
                        md.ns, no_ver_name(md.name), err))
        finally:
            _iquery.namespace = saved_ns
    else:
        # Complain if we're about to overwrite an existing array.
        got = set(['.'.join((md.ns, no_ver_name(md.name)))
                   for md in _gather_manifest_from_scidb()])
        incoming = set(['.'.join((md.ns, no_ver_name(md.name)))
                        for md in _cfg.manifest])
        clobber = got & incoming
        if clobber:
            warn("The restore would overwrite the following arrays:",
                 LOG_LINE_SEP, wrap(clobber), LOG_LINE_SEP,
                 "Remove them manually, or use --force")
            return 1

    if _args.zip:
        # Symlinks not needed; zcat does the redirection.
        restore_with_zip(_cfg.manifest)
    else:
        with MadeSymlinks():
            restore_arrays(_cfg.manifest)


def make_save_query(md):
    """Create AFL query for saving manifest entry md's array."""
    iid = '-1' if _args.parallel else '-2'
    if _args.zip:
        # Always datadir-relative: md.file_name will be a pipe.
        folder = ''
    elif _args.parallel:
        # Pre-made relative symbolic link in datadir.
        folder = os.path.basename(_args.folder)
    else:
        # Single target folder on coordinator.
        folder = _args.folder

    what = "{0}"
    if _args.save == 'binary':
        what = make_binary_save_query(md.schema)
    query = "save({0}, '{1}', {2}, '{3}')".format(
        what.format(md.name),
        os.path.join(folder, md.file_name),
        iid,
        md.format)
    return query


def _mpw_save_query(md):
    """Run save() query for manifest entry 'md' in the background."""
    mp.current_process().name = "save({0})".format(md.file_name)
    try:
        query = make_save_query(md)
        iquery = IQuery(afl=True, no_fetch=True)
        iquery.namespace = None if md.ns == 'public' else md.ns
        dbg("Querying:", query)
        _, err = iquery(query)
        if iquery.returncode:
            warn("Cannot save array {0}.{1}:\n{2}".format(
                md.ns, md.name, err))
            return False

        dbg("Query done")
        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def _mpw_zipper(args):
    """Compress array data from fifo into like-named file in archive folder."""
    ii, filename = args
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        target = os.path.join(local_archive(ii), filename)
        pipe = os.path.join(ii['instance_path'], filename)
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        _, err = ssh.run("gzip -c < {0} > {1}".format(pipe, target))
        if ssh.failed():
            warn("Gzip from {0} to {1} failed: {2}".format(
                pipe, target, err))
            return False

        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def save_with_zip(manifest):
    """Save arrays named in the manifest, using compression."""

    if _args.parallel:
        remotes = _instances
    else:
        remotes = [_cfg.coordinator]

    saved_arrays = 0
    pool = mp.Pool(len(remotes))
    query_pool = mp.Pool(1)

    for i, md in enumerate(manifest):
        prt('Archiving zipped', '.'.join((md.ns, md.name)))

        if ((i + 1) % ZIP_POOL_REFRESH) == 0:
            # Everybody out of the pool!
            dbg("Start pool refresh")
            pool.close()
            pool.join()
            query_pool.close()
            query_pool.join()
            pool = mp.Pool(len(remotes))
            query_pool = mp.Pool(1)
            dbg("Pool refresh done")

        with MadePipes(remotes, md.file_name, pool=pool):

            # Launch the save() query asynchronously in the background.
            # It will block waiting for pipe readers.
            dbg("Launching save query in background")
            query_result = []
            async_handle = query_pool.map_async(_mpw_save_query, [md],
                                                callback=query_result.extend)

            # Did we fail early for some reason?  Query should be blocked!
            async_handle.wait(1.0)
            if async_handle.ready():
                raise AppError("Save query aborted: %s" % query_result)

            # Launch the gzip pipe readers synchronously.  (Same args from
            # above is OK.)
            dbg("Starting fifo readers")
            args = zip([x._asdict() for x in remotes],
                       [md.file_name] * len(remotes))
            results = pool.map(_mpw_zipper, args)
            if not all(results):
                raise AppError(
                    "One or more gzip readers failed for {0}".format(
                        md.file_name))

            # Collect the query process result.
            dbg("Waiting on query subprocess completion...")
            try:
                async_handle.wait(ZIP_ERROR_TIMEOUT)
            except mp.TimeoutError:
                warn("Timed out waiting for save query to finish")
            else:
                dbg("Save query done:", query_result)
                assert query_result, "Query done but result list empty?!"
                if not all(query_result):
                    raise AppError("Save of {0} reported failure".format(
                        md.file_name))

        saved_arrays += 1

    prt('Saved', saved_arrays, 'of', len(manifest), 'arrays')
    pool.close()
    pool.join()
    query_pool.close()
    query_pool.join()
    dbg("Final pool close/joins done")


def save_arrays(manifest):
    """Save arrays named in the manifest, without compression."""

    saved_ns = _iquery.namespace
    try:
        saved_arrays = 0

        for md in manifest:
            prt('Archiving', '.'.join((md.ns, md.name)))
            query = make_save_query(md)
            _iquery.namespace = None if md.ns == 'public' else md.ns
            out, err = _iquery('--no-fetch', query)
            if _iquery.returncode:
                raise AppError("Cannot save array {0}.{1}:\n{2}".format(
                    md.ns, md.name, err))
            saved_arrays += 1

    finally:
        _iquery.namespace = saved_ns
        prt('Saved', saved_arrays, 'of', len(manifest), 'arrays')


def do_save():
    """Save array data."""
    dbg("Verb: save (%s)" % _verb)
    make_folders()

    # Program arguments dictate how the manifest is gathered.
    manifest = _gather_manifest_from_scidb()
    manifest.update({
        "save-options": _buildSaveOpts(),  # historical
        "format": _args.save,
        "zip": bool(_args.zip),
        "all-versions": bool(_args.all_versions),
        "parallel": bool(_args.parallel),
        "sequential-ids": bool(_args.sequential_ids)
    })

    # Write manifest to the backup folder on the coordinator.
    # (Probably local, but may not be in future.)
    ssh = SshRunner(host=_cfg.coordinator.name, port=_args.ssh_port)
    with tempfile.NamedTemporaryFile() as TMP:
        print >>TMP, manifest
        TMP.flush()
        _, err = ssh.put_file(TMP.name, _cfg.manifest_path)
        if ssh.failed():
            raise AppError("Cannot write manifest to {0}:{1} - {2}".format(
                _cfg.coordinator.name, _cfg.manifest_folder, err))

    # XXX Temporary for testing: write a .save_opts file so we can
    # prove that the old script can read the new script's backups.
    if os.environ.get("WRITE_SAVE_OPTS_FILE", False):
        opts_file = os.path.join(os.path.dirname(_cfg.manifest_path),
                                 '.save_opts')
        with tempfile.NamedTemporaryFile() as TMP:
            print >>TMP, "Nobody reads this file.  Sorry to have troubled you."
            TMP.flush()
            _, err = ssh.put_file(TMP.name, opts_file)
            if ssh.failed():
                warn("Could not write {0}: {1} (but who cares?)".format(
                    opts_file, err))

    if _args.zip:
        save_with_zip(manifest)
    else:
        with MadeSymlinks():
            save_arrays(manifest)

    do_verify(manifest)


def do_delete():
    """Delete the archive folder and all its minions, if any."""
    dbg("Verb: delete (%s)" % _verb)
    if not _args.folder:
        raise AppError("No folder specified for --delete")
    if not _args.force:
        ask = "Delete backup folder(s) {0}* on all SciDB servers?".format(
            _args.folder)
        yes = confirm(ask, resp=False)
        if not yes:
            return
    assert _instances, "Empty instance list?!"
    pool = mp.Pool(len(_instances))
    args = [x._asdict() for x in _instances]
    results = pool.map(_mpw_delete_backups, args)
    pool.close()
    pool.join()
    if not all(results):
        raise AppError(
            "Cannot delete folder {0} or {0}.<instance> on one or"
            " more instances".format(_args.folder))


def _mpw_delete_backups(ii):
    """Delete both the named folder *and* any per-instance folders.

    Casts a wide net and deletes folders for both --sequential-ids and
    --raw-ids.

    It would be nice to ensure that the folders being removed are
    actually backup folders, but for now only the coordinator folder
    gets a .manfest file, so there's not really any good way to check.
    XXX Put .manifest files everywhere... why not?
    """
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        # Base folder, per-instance w/ --sequential-ids, --raw-ids.
        victims = [_args.folder]
        for field in ('No', 'instance_id'):
            victims.append('.'.join((_args.folder, str(ii[field]))))

        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)
        fails = 0
        dead = 0
        for folder in victims:
            # Separate test and rm steps so that we don't claim to
            # delete folders that never existed (and thereby cause
            # anxiety).
            ssh.run("test -e %s" % folder)
            if ssh.failed():
                continue
            _, err = ssh.run("rm -rf %s" % folder)
            if ssh.failed():
                warn("Cannot delete {0}: {1}".format(folder, err))
                fails += 1
            else:
                dead += 1
                prt("Deleted", folder)

        if not dead:
            prt("Nothing to delete")

        return fails == 0

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def do_verify(manifest=None):
    """Verify the archive folder(s) all agree with the manifest."""

    # Other verbs call this, so _verb may differ.
    dbg("Verb: verify (%s)" % _verb)

    if not _args.folder:
        raise AppError("Folder argument required")

    prt("Verifying", _args.folder)

    if manifest is None:
        # Use the one retrieved during setup().
        if _cfg.manifest is None:
            raise AppError("Cannot proceed without a manifest")
        dbg("Using manifest found during setup()")
        manifest = _cfg.manifest
    set_options_from_manifest(manifest)

    # Check that all folders contain all arrays listed in the manifest.
    if _args.parallel:
        remotes = _instances
    else:
        remotes = [_cfg.coordinator]
    pool = mp.Pool(len(remotes))
    args = zip([x._asdict() for x in remotes],
               [manifest] * len(remotes))
    results = pool.map(_mpw_verify_backup, args)
    pool.close()
    pool.join()
    if not all(results):
        raise AppError("Backup {0} NOT verified!".format(_args.folder))
    prt("Backup", _args.folder, "verified")


def _mpw_verify_backup(args):
    """Remote worker for verify_backups()."""
    ii, manifest = args
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        folder = local_archive(ii)
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        # All files in the manifest must appear in the backup folder.
        fails = 0
        for md in manifest:
            path = os.sep.join((folder, md.file_name))
            ssh.run("test -e %s" % path)
            if ssh.failed():
                warn("Missing archive file:", path)
                fails += 1
        return fails == 0

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def do_ping():
    """Diagnose connectivity and directory access problems."""
    dbg("Verb: ping (%s)" % _verb)
    if not _instances:
        raise AppError("No instances to ping")

    # If we fetched a manifest from setup, grab the --parallel setting
    # so that we'll look for the archive folder in the right place.
    if _cfg.manifest:
        _args.parallel = _cfg.manifest.parallel

    pool = mp.Pool(len(_instances))
    args = [x._asdict() for x in _instances]
    results = pool.map(_mpw_ping, args)
    pool.close()
    pool.join()
    if not all(results):
        raise AppError(
            "FAILED: Connectivity or folder problems on one or more instances")

    # Anything not actually True means folder not found on instance.
    # (If --parallel, we should have already raised.)
    some_missing = [x for x in results if x is not True]
    assert not (some_missing and _args.parallel), (
        "Internal error in some-missing logic")
    if len(some_missing) == len(results):
        # Huh, they're *all* missing.
        raise AppError("Archive folder not found on any instance")


def _mpw_ping(ii):
    """Check connectivity to instance."""
    mp.current_process().name = pretty_iid(ii['instance_id'])
    try:
        ssh = SshRunner(host=ii['name'], port=_args.ssh_port)

        # Can I contact it at all?
        out, err = ssh.run("env | grep SSH_")
        if ssh.failed():
            warn("No SSH access to {0}: {1}".format(ii['name'], err))
            return False        # No point in going on.

        with ScopedLock(_iolock):
            for line in out.splitlines():
                prt(line, nolock=True)

        # Check folder presence and access for data directory and, if
        # given, archive folder.
        if check_one_folder(ssh, ii['instance_path']):
            prt("Data directory is present and writeable")
        else:
            return False

        # We're asked to see if a backup folder is present...
        if _args.folder:
            what = local_archive(ii)
            if check_one_folder(ssh, what):
                prt("Archive folder", what, "is present and writeable")
            elif _args.parallel:
                return False
            else:
                # Can't say whether it's truly an error until all instances
                # report in.  Return something "true-ish" that "is not True".
                return "Archive missing"

        return True

    except Exception as e:
        with ScopedLock(_iolock):
            warn("Unhandled exception:", e, nolock=True)
            traceback.print_exc()   # always want this if exception unexpected
        return False


def _do_verb():
    """Initial _do_verb callable throws an internal error."""
    raise AppError("Internal error, no verb defined")


class VerbAction(argparse.Action):

    """
    Make the "verb" options choose a subcommand callable.

    For historical reasons we use a "--save FORMAT" switch instead of
    a "save" argparse subcommand with a "--format FORMAT" switch.
    This small glue class tries to ease the pain by automatically
    setting up the _do_verb callable.

    This is not perfect: the FOLDER positional must now become
    nargs='?'  rather than nargs=1 because some verbs don't need it.
    We should use argparse subparsers, but that would break
    compatibility.
    """

    def __init__(self, option_strings, dest, **kwargs):
        super(VerbAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        funcname = 'do_' + self.dest
        global _do_verb, _verb
        try:
            _do_verb = globals()[funcname]
        except IndexError:
            raise AppError("No %s() callable found" % funcname)
        if not callable(_do_verb):
            raise AppError("%s is not callable" % funcname)
        _verb = self.dest
        setattr(namespace, self.dest, values)


def main(argv=None):
    """Argument parsing and last-ditch exception handling."""
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "[%s]" % os.path.splitext(os.path.basename(argv[0]))[0]

    parser = argparse.ArgumentParser(
        description="Backup and restore utility for SciDB array data",
        epilog="""Type "pydoc {0}" for more information.""".format(
            os.path.basename(argv[0])))

    # General options
    parser.add_argument('-A', '--auth-file', help="""
        Authentication file for SciDB user""")
    parser.add_argument('-c', '--host', help="SciDB server")
    parser.add_argument('--port', help="""TCP port for connecting to SciDB.
        (Beware, -p short option means --parallel!)""")
    parser.add_argument('--ssh-port', type=int, help="""
        TCP port for contacting remote SSH daemons""")
    parser.add_argument('-v', '--verbose', action='count', help="""
        Increase logging, -v: debug, -vv: extra debug""")

    # Verbs
    verbs = parser.add_mutually_exclusive_group(required=True)
    verbs.add_argument('-s', '--save', choices=SAVE_FORMATS, action=VerbAction,
                       help="Save array data.")
    verbs.add_argument('-r', '--restore', choices=RESTORE_FORMATS,
                       action=VerbAction, help="Restore array data.")
    verbs.add_argument('--ping', action=VerbAction, nargs=0,
                       help="""Check SSH connectivity to SciDB servers,
                       including folder access if one is specified.""")
    verbs.add_argument('-d', '--delete', action=VerbAction, nargs=0,
                       help="""Delete saved array data files and folders on
                       all SciDB servers.""")
    verbs.add_argument('--verify', action=VerbAction, nargs=0,
                       help="""Verify an existing backup folder.""")

    # Options that apply only to save or restore verbs.  Positionals
    # must be optional, since other verbs like --ping don't use them.
    parser.add_argument('-a', '--all-versions', '--allVersions',
                        action='store_true', help="""
        Save or restore all array versions (potentially a lot of data).
        The --allVersions form is for backward compatibility and will go
        away eventually.""")
    parser.add_argument('-f', '--filter', metavar='PATTERN', help="""
        Save or restore only arrays with names matching the given regular
        expression.""")
    parser.add_argument('-p', '--parallel', action='store_true', help="""
        Save or restore arrays in parallel mode (to all SciDB instances at
        once).""")
    parser.add_argument('-z', '--zip', action='store_true', help="""
        Use gzip when saving or restoring array data.""")
    parser.add_argument('--force', action='store_true', help="""
        For --restore, silently remove arrays before restoring them.
        For --save, overwrite any previously saved backup folder.
        For --delete, don't ask for confirmation.""")
    parser.add_argument('--use-public-ns', action='store_true', help="""
        Ignore namespace information in a backup archive and try to restore
        all arrays into the public namespace.""")

    # Either --sequential-ids or --raw-ids but not both.
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--sequential-ids', action='store_true', help="""
        Use instance ID ranks rather than the IDs themselves to identify
        instance-specific folders.  The ranks of the instances ordered by
        their instance IDs are sequential cluster-independent identifiers
        which can be used to transfer data between clusters.
        This is the default.""")
    id_group.add_argument('--raw-ids', action='store_true', help="""
        Use raw instance IDs to identify instance-specific folders.
        May be needed to read older --parallel backups.""")

    # Either --include-ns or --exclude-ns but not both.
    ns_group = parser.add_mutually_exclusive_group(required=False)
    ns_group.add_argument('--include-ns', metavar='NAMESPACES', help="""
        Comma-separated list of SciDB namespaces to include when saving
        or restoring.""")
    ns_group.add_argument('--exclude-ns', metavar='NAMESPACES', help="""
        Comma-separated list of SciDB namespaces to exclude when saving
        or restoring.""")

    parser.add_argument('folder', metavar='FOLDER', nargs='?', help="""
        Folder for saving/restoring arrays.  When --parallel is used, the
        actual folder will have a per-instance suffix appended.""")

    global _args
    _args = parser.parse_args(argv[1:])

    # Sigh.
    global _stderr
    if 'P4_TESTCASES_DIR' in os.environ:
        _stderr = sys.stdout
        dbg("Running under the test harness.")

    if not _args.verbose and 'SCIDB_DBG' in os.environ:
        try:
            _args.verbose = int(os.environ['SCIDB_DBG'])
        except ValueError:
            raise AppError("SCIDB_DBG={0} but must be int".format(
                os.environ['SCIDB_DBG']))

    # Settable from the environment for easy testing.
    global ZIP_POOL_REFRESH
    if "ZIP_POOL_REFRESH" in os.environ:
        try:
            ZIP_POOL_REFRESH = int(os.environ["ZIP_POOL_REFRESH"])
        except Exception:
            warn("Bad ZIP_POOL_REFRESH ignored")
        else:
            if ZIP_POOL_REFRESH < 1:
                warn("Non-positive ZIP_POOL_REFRESH ignored")
                ZIP_POOL_REFRESH = 50

    # The default is now --sequential-ids .
    if not _args.raw_ids and not _args.sequential_ids:
        _args.sequential_ids = True

    # Normalize --exclude-ns/--include-ns lists.
    if _args.include_ns:
        _args.include_ns = [x.strip() for x in _args.include_ns.split(',')]
    if _args.exclude_ns:
        _args.exclude_ns = [x.strip() for x in _args.exclude_ns.split(',')]

    # Create the global iquery client used throughout.
    global _iquery
    _iquery = IQuery(afl=True, auth_file=_args.auth_file,
                     host=_args.host, port=_args.port,
                     format='tsv',
                     debug=(_args.verbose > 2))

    # Here we go!
    try:
        setup()
        _do_verb()
        dbg(_verb, "complete")
    except AppError as e:
        print >>_stderr, _pgm, "Error:", str(e)
        return 1
    except Exception as e:
        print >>_stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc(file=_stderr)   # always if exception unexpected
        return 2
    else:
        return 0


if __name__ == '__main__':
    _use_caches = True          # Running as the main script, OK to use caches.
    sys.exit(main())
