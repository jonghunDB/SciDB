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

"""
SciDB schema parsing and unparsing in Python.
"""

import re
from collections import namedtuple

# See include/array/Coordinate.h
MAX_COORDINATE = (2 ** 62) - 1
MIN_COORDINATE = - MAX_COORDINATE

Attribute = namedtuple('Attribute', ('name', 'type', 'nullable', 'default',
                                     'compression', 'reserve'))

Dimension = namedtuple('Dimension', ('name', 'lo', 'hi', 'chunk', 'overlap'))


def split_unquoted(s, delim, quote="'"):
    """Split s on occurences of delim that don't occur inside quotes."""
    # I am certain there is a very tricky regex that will do this, but...
    inside = False
    result = []
    token = []
    last = None
    for ch in s:
        if ch == delim:
            if inside:
                token.append(ch)
            else:
                result.append(''.join(token))
                token = []
        elif ch == quote:
            token.append(ch)
            if inside:
                if last != '\\':
                    inside = False
            else:
                inside = True
        else:
            token.append(ch)
        last = ch
    if token:
        result.append(''.join(token))
    return result


def _parse_old_dimensions(dims):
    """Parse old-style dimensions."""
    # Each regex match peels off a full dimension
    # spec from the left of the dimensions part.  AFL allows arbitrary
    # expressions for many of these parameters, but we restrict to
    # "reasonable" values.  Fix when and if needed.
    result = []
    rgx = (r'\s*([^=\s]+)\s*='
           r'\s*([-+]{,1}\s*\d+|\?)\s*:\s*(\?|\*|[-+]{,1}\s*\d+)'
           r'\s*,\s*(\?|\*|\d+)\s*,\s*(\?|\*|\d+)\s*')
    first_time = True
    while dims:
        m = re.match(rgx, dims, re.MULTILINE)
        if not m:
            raise ValueError("Bad old-style dimension(s): '%s'" % dims)
        low = m.group(2) if m.group(2) == '?' else long(m.group(2))
        high = m.group(3) if m.group(3) in ('?', '*') else long(m.group(3))
        chunk = m.group(4) if m.group(4) in ('?', '*') else long(m.group(4))
        overlap = m.group(5) if m.group(5) in ('?', '*') else long(m.group(5))
        result.append(Dimension(m.group(1), low, high, chunk, overlap))
        if first_time:
            rgx = '[;,]%s' % rgx   # subsequent matches need group separator
            first_time = False
        dims = dims[len(m.group(0)):]
    return result


def _parse_dimensions(dims):
    """Parse 16.x-and-later dimensions."""
    result = []
    for dim in (x.strip() for x in dims.split(';')):
        parts = [x.strip() for x in dim.split(':')]
        assert parts, "Splitting a dimension spec should never come up empty"
        if not parts[0]:
            raise ValueError("Empty or nameless dimension in '%s'" % dims)
        if len(parts) == 1:
            # Lone identifier.
            if '=' in parts[0]:
                raise ValueError("Malformed identifier '%s' in '%s'" % (
                    parts[0], dims))
            result.append(Dimension(parts[0], 0, '*', '*', 0))
            continue
        if len(parts) > 4:
            raise ValueError("Bad dimension '%s': parameter count" % dim)
        nm, lo = [x.strip() for x in parts[0].split('=')]
        if not nm or not lo:
            raise ValueError("Malformed name or low bound in '%s'" % parts[0])
        parts.extend([None] * 4)  # pad with plenty of None
        lo = lo if lo == '?' else long(lo)
        hi = parts[1] if parts[1] in ('?', '*') else long(parts[1])  # !None
        co = parts[2] if parts[2] in (None, '?', '*') else long(parts[2])
        ci = parts[3] if parts[3] in (None, '?', '*') else long(parts[3])
        result.append(Dimension(nm, lo, hi, ci, co))
    return result


def parse(schema, default_nullable=True):
    """Parse a SciDB schema into lists of attributes and dimensions.

    @param schema array schema to parse
    @param default_nullable default nullability of attributes
    @throws ValueError on malformed schema
    @return attr_list, dim_list

    @description
    Returned attribute and dimension lists are namedtuples whose
    elements can be accessed by Python attribute name.  For an
    attr_list element 'attr', you can access:

        attr.name
        attr.type
        attr.nullable
        attr.default
        attr.compression
        attr.reserve

    For a dim_list element 'dim', you can access:

        dim.name
        dim.lo
        dim.hi
        dim.chunk
        dim.overlap

    For dimension parameters, numeric values are returned as longs,
    but special values '?' and '*' are returned intact.  Callers must
    handle these values appropriately.  For example, if the 'hi'
    attribute is '*' but a numeric value is needed, calling code must
    test for '*' and substitute MAX_COORDINATE.

    In the new dimension syntax (16.9 and later), the chunk and
    overlap dimension parameters are optional.  They are returned as
    None if not specified.

    Attributes became nullable by default in 15.12.  When parsing
    schema strings written by pre-15.12 software, set default_nullable
    to False to preserve the intended semantics.
    """

    # Start by cracking schema into attributes and dimensions parts.
    m = re.search(r'<([^>]+)>\s*\[([^\]]+)\]', schema, re.MULTILINE)
    if not m:
        raise ValueError("Bad schema: '%s'" % schema)
    attrs = m.group(1).strip()
    dims = m.group(2).strip()
    if not attrs:
        raise ValueError("Missing attributes: '%s'" % schema)
    if not dims:
        raise ValueError("Missing dimensions: '%s'" % schema)

    # Parse attributes.  The attribute delimiter is comma, but it can
    # also appear in a quoted string's DEFAULT value.  Break up attrs
    # on delimiting commas, not quoted commas.
    attr_descs = split_unquoted(attrs, ',')
    attr_list = []
    for desc in attr_descs:

        # Split out attribute name and type specification.
        nm, ty = map(str.strip, desc.split(':', 1))
        if not nm or not ty:
            raise ValueError("Bad attribute: '%s'" % desc)

        # Split out the base type.
        m = re.match(r'^\S+', ty, re.MULTILINE)
        if not m:
            raise ValueError("Missing attribute type: '%s'" % desc)
        attr_type = m.group().lower()
        ty = ty[m.end():]

        # Split out type qualifiers, starting from the righthand end
        # of 'ty' (since the DEFAULT expression is arbitrarily
        # complex).  First, split out "RESERVE <integer>" .
        m = re.search(r'\breserve\s+([+-]?\s*\d+)\s*$', ty,
                      re.MULTILINE | re.IGNORECASE)
        if m:
            reserve = int(m.group(1))
            ty = ty[:m.start()]
        else:
            reserve = None

        # The "COMPRESSION 'string'" qualifier is next, again on the right.
        m = re.search(r"\bcompression\s+'(([^']|(?<=\\)')*)'\s*$", ty,
                      re.MULTILINE | re.IGNORECASE)
        if m:
            compression = m.group(1)
            ty = ty[:m.start()]
        else:
            compression = None

        # Done with qualifiers that might be to the right of the
        # tricky DEFAULT qualifier.  Now pick off qualifiers from the
        # left, which mercifully is just "null" or "not null".
        nullable = None
        m = re.match(r'\s*null\b', ty, re.MULTILINE | re.IGNORECASE)
        if m:
            nullable = True
            ty = ty[m.end():]
        else:
            m = re.match(r'\s*not\s+null\b', ty, re.MULTILINE | re.IGNORECASE)
            if m:
                nullable = False
                ty = ty[m.end():]
            else:
                nullable = default_nullable
        assert nullable is not None

        # Finally, tackle the "DEFAULT <tricky_expression>" qualifier.
        m = re.match(r'\s*default\s+(.+)\s*$', ty,
                     re.MULTILINE | re.IGNORECASE | re.DOTALL)
        if m:
            default_value = m.group(1).strip()
            ty = ty[m.end():]
        else:
            default_value = None

        # Given the AFL grammar as of 15.12, ty should now be empty.
        if ty and not ty.isspace():
            raise ValueError("Unrecognized attribute qualifier: '%s'" % ty)

        # And now the Attribute tuple can be built.
        attr_list.append(Attribute(nm, attr_type, nullable, default_value,
                                   compression, reserve))

    # Parse dimensions.  There are no commas in the new dimension
    # syntax, but commas are required in the old syntax.  Don't look
    # at quoted commas, since [i=0:sizeof(',,,,,,,')] is legal.
    if len(split_unquoted(dims, ',')) > 1:
        dim_list = _parse_old_dimensions(dims)
    else:
        dim_list = _parse_dimensions(dims)

    return attr_list, dim_list


def _make_attribute(a, null, notnull):
    """Recreate attribute specification string from Attribute 'a'."""
    return ''.join((
        a.name,
        ':',
        a.type,
        (null if a.nullable else notnull),
        (' DEFAULT ' + a.default if a.default is not None else ''),
        (" COMPRESSION '%s'" % a.compression
         if a.compression is not None else ''),
        (" RESERVE %d" % a.reserve if a.reserve else '')))


def _make_old_dimension(dim):
    """Recreate dimension specification in 15.12 compatibility format."""
    return "%s=%s:%s,%s,%s" % (
        dim.name, str(dim.lo),
        ('*' if dim.hi >= MAX_COORDINATE else str(dim.hi)),
        str(dim.chunk) if dim.chunk else '*',
        str(dim.overlap) if dim.overlap else '0')


def _make_dimension(dim):
    """Recreate dimension specification string."""
    ret = "%s=%s:%s" % (dim.name,
                        str(dim.lo),
                        ('*' if dim.hi >= MAX_COORDINATE else str(dim.hi)))
    if dim.overlap or dim.chunk:
        overlap = dim.overlap if dim.overlap else 0  # handle None, False
        ret += ":%s" % str(overlap)
    if dim.chunk:
        ret += ":%s" % str(dim.chunk)
    return ret


def unparse(attrs=None, dims=None, default_nullable=True, compat=True):
    """Inverse of parse: turn attributes and dimensions into a schema string.

    @param attrs list of Attribute namedtuples as returned by #parse
    @param dims list of Dimension namedtuples as returned by #parse
    @param default_nullable default nullability of attributes
    @param compat use 15.12-compatible dimension syntax
    @returns schema string constructed from given attributes and dimensions
    """
    if default_nullable:
        null = ''
        notnull = ' NOT NULL'
    else:
        null = ' NULL'
        notnull = ''
    if compat:
        make_dim = _make_old_dimension
        delim = ','
    else:
        make_dim = _make_dimension
        delim = "; "
    if isinstance(attrs, Attribute):
        attrs = [attrs]
    if isinstance(dims, Dimension):
        dims = [dims]
    aa = '' if attrs is None else ','.join(_make_attribute(x, null, notnull)
                                           for x in attrs)
    dd = '' if dims is None else delim.join([make_dim(y) for y in dims])
    return "<{0}>[{1}]".format(aa, dd)


def main(args=None):
    if args is None:
        args = sys.argv

    for arg in args[1:]:
        print '----', arg
        alist, dlist = parse(arg)
        for i, attr in enumerate(alist):
            print ' '.join(("Attribute[%d]:" % i, attr.name, attr.type,
                            "*" if attr.nullable else ""))
        for i, dim in enumerate(dlist):
            print ' '.join(map(str, ("Dimension[%d]:" % i, dim.name, dim.lo,
                                     "*" if dim.hi == sys.maxsize else dim.hi,
                                     dim.chunk, dim.overlap)))
        adict = dict([(x.name, x) for x in alist])
        if "foo" in adict:
            print "Foo is such a boring name for an attribute."

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
