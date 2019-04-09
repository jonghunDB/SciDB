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

/****************************************************************************/

#include <query/Query.h>                                 // For Query
#include <system/SystemCatalog.h>                        // For SystemCatalog
#include <util/arena/ScopedArena.h>                      // For ScopedArena
#include <util/arena/Vector.h>                           // For mgd::vector
#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  @brief      Eliminates syntacic sugar by rewriting derived constructs into
 *              the kernel language.
 *
 *  @details    Currently handles:
 *
 *              - create array() => create_array_as()
 *              - load()         => store(input())
 *
 *  @author     jbell@paradigm4.com.
 */
class Desugarer : public Visitor
{
 public:
                           // Construction
                              Desugarer(Factory&,Log&,const QueryPtr&);

 private:                  // From class Visitor
    virtual void              onApplication(Node*&);

 private:                  // Load:
            void              onLoad            (Node*&);

 private:                  // Implementation
            bool              isApplicationOf   (Node*,name)            const;

 private:                  // Representation
            Factory&          _fac;                      // The node factory
            Log&              _log;                      // The error log
            SystemCatalog&    _cat;                      // The system catalog
            ScopedArena       _mem;                      // The local heap
            QueryPtr  const   _qry;                      // The query context
};

/**
 *
 */
Desugarer::Desugarer(Factory& f,Log& l,const QueryPtr& q)
             : _fac(f),
               _log(l),
               _cat(*SystemCatalog::getInstance()),
               _mem(arena::Options("parser::Desugarer")),
               _qry(q)
{}

/**
 *
 */
void Desugarer::onApplication(Node*& pn)
{
    assert(pn!=0 && pn->is(application));                // Validate arguments

 /* Is this a top level application of the 'load()' operator?...*/

    if (isApplicationOf(pn,"Load"))
    {
        onLoad(pn);                                      // ...rewrite the app
    }

    Visitor::onApplication(pn);                          // Process as before
}

/****************************************************************************/

/**
 *  Translate:
 *
 *      LOAD    (array,<a>)
 *
 *  into:
 *
 *      STORE   (INPUT(array,<a>)),array)
 *
 *  where:
 *
 *      array = names the target array to be stored to
 *
 *      a     = whatever remaining arguments the 'input' operator may happen
 *              to accept.
 */
void Desugarer::onLoad(Node*& pn)
{
    assert(pn->is(application));                         // Validate arguments

    location w(pn->getWhere());                          // The source location
    cnodes   a(pn->getList(applicationArgOperands));     // Operands for input

 /* Is the mandatory target array name missing? If so,  the application will
    certainly fail to compile, but we'll leave it to the 'input' operator to
    report the error...*/

    if (a.empty())                                       // No target array?
    {
        pn = _fac.newApp(w,"Input");                     // ...yes, will fail
    }
    else
    {
        pn = _fac.newApp(w,"Store",                      // store(
             _fac.newApp(w,"Input",a),                   //   input(<a>),
             a.front());                                 //   a[0])
    }
}

/****************************************************************************/

/**
 *  Return true if the application node 'pn' represents an application of the
 *  operator-macro-function named 'nm'.
 */
bool Desugarer::isApplicationOf(Node* pn,name nm) const
{
    assert(pn->is(application) && nm!=0);                // Validate arguments

    return strcasecmp(nm,pn->get(applicationArgOperator)->get(variableArgName)->getString())==0;
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Traverse the abstract syntax tree in search of derived constructs that are
 *  to be rewritten into the kernel syntax.
 */
Node*& desugar(Factory& f,Log& l,Node*& n,const QueryPtr& q)
{
    return Desugarer(f,l,q)(n);                          // Run the desugarer
}

/****************************************************************************/
}}
/****************************************************************************/
