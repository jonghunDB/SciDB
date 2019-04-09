#!/usr/bin/python
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

import ply.lex as lex
import ply.yacc as yacc

from scidb_config import Context
from scidb_config import parseConfig

from ConfigParser import RawConfigParser

def parse_disable_file(disable_file='',
                       new_disable_file='',
                       instances=1,
                       nodes=1,
                       redundancy=0,
                       build='assert',
                       security='trust'):

    reserved = {
        'instances'      : 'INSTANCES',
        'nodes'          : 'NODES',
        'redundancy'     : 'REDUNDANCY',
        'build'          : 'BUILD',
        'assert'         : 'ASSERT',
        'relwithdebinfo' : 'RELWITHDEBINFO',
        'security'       : 'SECURITY',
        'trust'          : 'TRUST',
        'namespace'      : 'NAMESPACE',
        'if'             : 'IF',
        'unless'         : 'UNLESS',
        'and'            : 'AND',
        'or'             : 'OR'
        }

    tokens = [
        'COMMA',
        'EQUAL',
        'NOT_EQUAL',
        'LESS_THAN',
        'GREATER_THAN',
        'NUMBER',
        'TESTNAME',
        ] + list(reserved.values())

    def t_NUMBER(t):
        r'\d+'
        t.value = int(t.value)
        return t

    def t_TESTNAME(t):
        r'[0-9A-z._/-]+'
        t.type = reserved.get(t.value,'TESTNAME')    # Check for reserved words
        return t

    # Regular expression rules for simple tokens
    def t_ignore_COMMENT(t):
        r'\#.*'
        pass

    t_COMMA = r','

    t_EQUAL        = r'=='
    t_NOT_EQUAL    = r'!='
    t_LESS_THAN    = r'<'
    t_GREATER_THAN = r'>'

    # Ignore newlines
    def t_newline(t):
        r'\n+'
        pass

    # A string containing ignored characters (spaces and tabs)
    t_ignore  = ' \t'


    # Grammar production rules.  For example given BNF
    #    X ::= Y PLUSOP Z
    # you might
    #    return p[0] = p[1] + p[2]

    # Error handling rule
    def t_error(t):
        print("Illegal character '%s'" % t.value[0])
        t.lexer.skip(1)

    def p_disable_test_condition_comma(p):
        'disable : TESTNAME COMMA condition_list'
        if p[3]:
            p[0] = p[1]

    def p_disable_test_condition_if(p):
        'disable : TESTNAME IF condition_list'
        if p[3]:
            p[0] = p[1]

    def p_disable_test_condition_unless(p):
        'disable : TESTNAME UNLESS condition_list'
        if p[3] is not True:
            p[0] = p[1]

    def p_disable_test(p):
        'disable : TESTNAME'
        p[0] = p[1]

    def p_disable_(p):
        'disable : '
        pass

    def p_condition_condition_list_comma(p):
        'condition_list : condition COMMA condition_list'
        p[0] = (p[1] and p[3])

    def p_condition_condition_list_and(p):
        'condition_list : condition AND condition_list'
        p[0] = (p[1] and p[3])

    def p_condition_condition_list_or(p):
        'condition_list : condition OR condition_list'
        p[0] = (p[1] and p[3])

    def p_condition_list(p):
        'condition_list : condition'
        p[0] = p[1]

    def p_condition_instances_equal(p):
        'condition : INSTANCES EQUAL NUMBER'
        p[0] =  (instances == p[3])

    def p_condition_instances_not_equal(p):
        'condition : INSTANCES NOT_EQUAL NUMBER'
        p[0] =  (instances != p[3])

    def p_condition_instances_less_than(p):
        'condition : INSTANCES LESS_THAN NUMBER'
        p[0] =  (instances < p[3])

    def p_condition_instances_greater_than(p):
        'condition : INSTANCES GREATER_THAN NUMBER'
        p[0] =  (instances > p[3])

    def p_condition_nodes_equal(p):
        'condition : NODES EQUAL NUMBER'
        p[0] = (nodes == p[3])

    def p_condition_nodes_not_equal(p):
        'condition : NODES NOT_EQUAL NUMBER'
        p[0] = (nodes != p[3])

    def p_condition_nodes_less_than(p):
        'condition : NODES LESS_THAN NUMBER'
        p[0] = (nodes < p[3])

    def p_condition_nodes_greater_than(p):
        'condition : NODES GREATER_THAN NUMBER'
        p[0] = (nodes > p[3])

    def p_condition_redundancy_equal(p):
        'condition : REDUNDANCY EQUAL NUMBER'
        p[0] = (redundancy == p[3])

    def p_condition_redundancy_not_equal(p):
        'condition : REDUNDANCY NOT_EQUAL NUMBER'
        p[0] = (redundancy != p[3])

    def p_condition_redundancy_less_than(p):
        'condition : REDUNDANCY LESS_THAN NUMBER'
        p[0] = (redundancy < p[3])

    def p_condition_redundancy_greater_than(p):
        'condition : REDUNDANCY GREATER_THAN NUMBER'
        p[0] = (redundancy > p[3])

    def p_condition_build_equal_assert(p):
        'condition : BUILD EQUAL ASSERT'
        p[0] = (build == p[3])

    def p_condition_build_not_equal_assert(p):
        'condition : BUILD NOT_EQUAL ASSERT'
        p[0] = (build != p[3])

    def p_condition_build_equal_relwithdebinfo(p):
        'condition : BUILD EQUAL RELWITHDEBINFO'
        p[0] = (build == p[3])

    def p_condition_build_not_equal_relwithdebinfo(p):
        'condition : BUILD NOT_EQUAL RELWITHDEBINFO'
        p[0] = (build != p[3])

    def p_condition_security_equal_trust(p):
        'condition : SECURITY EQUAL TRUST'
        p[0] = (security == p[3])

    def p_condition_security_not_equal_trust(p):
        'condition : SECURITY NOT_EQUAL TRUST'
        p[0] = (security != p[3])
        if security != p[3]:
            p[0] = True
        else:
            p[0] = False

    def p_condition_security_equal_namespace(p):
        'condition : SECURITY EQUAL NAMESPACE'
        p[0] = (security == p[3])

    def p_condition_security_not_equal_namespace(p):
        'condition : SECURITY NOT_EQUAL NAMESPACE'
        p[0] = (security != p[3])

# Error rule for syntax errors
    def p_error(p):
        if p:
            # Just discard the token and tell the parser it's okay.
            parser.errok()
        else:
            print("Syntax error at EOF")

    # Build the lexer
    lexer = lex.lex()

    # Build the parser
    parser = yacc.yacc(debug=0, outputdir='/tmp')

    results = []

    with open(disable_file) as fp:
        for line in fp:
            result = parser.parse(line)
            if result:
                results.append(result)


    with open(new_disable_file, 'w') as fp:
        for res in results:
            fp.write(res + '\n')

    return results

