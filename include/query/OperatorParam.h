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
 * @file OperatorParam.h
 *
 * This file contains class declarations for the OperatorParam* classes.
 */

#ifndef OPERATOR_PARAM_H_
#define OPERATOR_PARAM_H_

#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <stdio.h>
#include <utility>
#include <memory>
#include <boost/format.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp> // needed for serialization of string parameter
#include <unordered_map>

// TBD: many of these can be eliminated if we eliminate the unnecessary/problematic inline implementations

#include <array/Array.h>
#include <array/ArrayDistribution.h>
#include <array/MemArray.h>
#include <array/MemoryBuffer.h>
#include <array/MultiStreamArray.h>
#include <query/TypeSystem.h>
#include <query/LogicalExpression.h>
#include <query/Expression.h>
#include <query/OperatorContext.h>
#include <query/OperatorID.h>
#include <rbac/NamespaceDesc.h>
#include <system/Config.h>
#include <util/InjectedError.h>
#include <util/ThreadPool.h>

namespace scidb
{

class Query;
class Aggregate;

/*
 * Don't forget to update the 'HELP' operator when adding placeholders.
 */
enum OperatorParamPlaceholderType
{
        PLACEHOLDER_INPUT            = 1,
        PLACEHOLDER_ARRAY_NAME       = 2,
        PLACEHOLDER_ATTRIBUTE_NAME   = 4,
        PLACEHOLDER_DIMENSION_NAME   = 8,
        PLACEHOLDER_CONSTANT         = 16,
        PLACEHOLDER_EXPRESSION       = 32,
        PLACEHOLDER_VARIES           = 64,
        PLACEHOLDER_SCHEMA           = 128,
        PLACEHOLDER_AGGREGATE_CALL   = 256,
        PLACEHOLDER_NS_NAME          = 512,
        PLACEHOLDER_END_OF_VARIES    = 1024 // Must be last!
};

enum PlaceholderArrayName
{
    PLACEHOLDER_ARRAY_NAME_VERSION = 1,
    PLACEHOLDER_ARRAY_NAME_INDEX_NAME = 2
};

class OperatorParamPlaceholder
{
public:
    OperatorParamPlaceholder(
            OperatorParamPlaceholderType placeholderType,
            Type requiredType,
            bool inputScheme,
            int flags
    ) :
        _placeholderType(placeholderType),
        _requiredType(requiredType),
        _inputSchema(inputScheme),
        _flags(flags)
    {}

    virtual ~OperatorParamPlaceholder() {}

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
     * @param[out] stream to write to
     * @param[in]  indent number of spacer characters to start every line with.
     */
    virtual void toString(std::ostream &out, int indent = 0) const;

    OperatorParamPlaceholderType getPlaceholderType() const
    {
        return _placeholderType;
    }

    const Type& getRequiredType() const
    {
        return _requiredType;
    }

    bool isInputSchema() const
    {
        return _inputSchema;
    }

    int getFlags() const
    {
        return _flags;
    }

private:
    OperatorParamPlaceholderType _placeholderType;
    Type _requiredType;
    bool _inputSchema;
    int _flags;
};

typedef std::shared_ptr<OperatorParamPlaceholder> PlaceholderPtr;
typedef std::vector<PlaceholderPtr> Placeholders;
typedef std::multimap<std::string, PlaceholderPtr> KeywordPlaceholders;

#define PARAM_IN_ARRAY_NAME() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ARRAY_NAME,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        0))

#define PARAM_IN_ARRAY_NAME2(flags) \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ARRAY_NAME,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        flags))

#define ADD_PARAM_IN_ARRAY_NAME() \
        addParamPlaceholder(PARAM_IN_ARRAY_NAME());

#define ADD_PARAM_IN_ARRAY_NAME2(flags) \
        addParamPlaceholder(PARAM_IN_ARRAY_NAME2(flags));

#define PARAM_OUT_ARRAY_NAME() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ARRAY_NAME,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_OUT_ARRAY_NAME() \
        addParamPlaceholder(PARAM_OUT_ARRAY_NAME());

#define PARAM_IN_NS_NAME() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_NS_NAME,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        0))

#define ADD_PARAM_IN_NS_NAME() \
        addParamPlaceholder(PARAM_IN_NS_NAME());

#define PARAM_OUT_NS_NAME() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_NS_NAME,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_OUT_NS_NAME() \
        addParamPlaceholder(PARAM_OUT_NS_NAME());

#define PARAM_INPUT() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_INPUT,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        0))

#define ADD_PARAM_INPUT() \
        addParamPlaceholder(PARAM_INPUT());

#define ADD_PARAM_VARIES() \
        addParamPlaceholder(\
        std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
            scidb::PLACEHOLDER_VARIES,\
            scidb::TypeLibrary::getType("void"),\
            false,\
            0)));

#define PARAM_OUT_ATTRIBUTE_NAME(type) \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ATTRIBUTE_NAME,\
        scidb::TypeLibrary::getType(type),\
        false,\
        0))

#define ADD_PARAM_OUT_ATTRIBUTE_NAME(type) \
        addParamPlaceholder(PARAM_OUT_ATTRIBUTE_NAME(type));

#define PARAM_IN_ATTRIBUTE_NAME(type) \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_ATTRIBUTE_NAME,\
        scidb::TypeLibrary::getType(type),\
        true,\
        0))

#define ADD_PARAM_IN_ATTRIBUTE_NAME(type) \
        addParamPlaceholder(PARAM_IN_ATTRIBUTE_NAME(type));

#define PARAM_IN_DIMENSION_NAME() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_DIMENSION_NAME,\
        scidb::TypeLibrary::getType("void"),\
        true,\
        0))

#define ADD_PARAM_IN_DIMENSION_NAME() \
        addParamPlaceholder(PARAM_IN_DIMENSION_NAME());

#define PARAM_OUT_DIMENSION_NAME() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_DIMENSION_NAME,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_OUT_DIMENSION_NAME() \
        addParamPlaceholder(PARAM_OUT_DIMENSION_NAME());


#define PARAM_EXPRESSION(type) \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_EXPRESSION,\
        scidb::TypeLibrary::getType(type),\
        false,\
        0))

#define ADD_PARAM_EXPRESSION(type) \
        addParamPlaceholder(PARAM_EXPRESSION(type));

#define PARAM_CONSTANT(type) \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_CONSTANT,\
        scidb::TypeLibrary::getType(type),\
        false,\
        0))

#define ADD_PARAM_CONSTANT(type) \
        addParamPlaceholder(PARAM_CONSTANT(type));

#define PARAM_SCHEMA() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_SCHEMA,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_SCHEMA() \
    addParamPlaceholder(PARAM_SCHEMA());

#define PARAM_AGGREGATE_CALL() \
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_AGGREGATE_CALL,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

#define ADD_PARAM_AGGREGATE_CALL() \
    addParamPlaceholder(PARAM_AGGREGATE_CALL());

#define END_OF_VARIES_PARAMS()\
    std::shared_ptr<scidb::OperatorParamPlaceholder>(new scidb::OperatorParamPlaceholder(\
        scidb::PLACEHOLDER_END_OF_VARIES,\
        scidb::TypeLibrary::getType("void"),\
        false,\
        0))

enum OperatorParamType
{
    PARAM_UNKNOWN,
    PARAM_ARRAY_REF,
    PARAM_ATTRIBUTE_REF,
    PARAM_DIMENSION_REF,
    PARAM_LOGICAL_EXPRESSION,
    PARAM_PHYSICAL_EXPRESSION,
    PARAM_SCHEMA,
    PARAM_AGGREGATE_CALL,
    PARAM_NS_REF,
    PARAM_ASTERISK
};

/**
 * Base class for parameters to both logical and physical operators.
 *
 * @note If you add a new child class of OperatorParam, you must add the child
 * class to registerLeafDerivedOperatorParams(), near the end of this file.
 */
class OperatorParam
{
public:
    OperatorParam() :
        _paramType(PARAM_UNKNOWN)
    {
    }

    OperatorParam(OperatorParamType paramType,
                  const std::shared_ptr<ParsingContext>& parsingContext) :
        _paramType(paramType),
        _parsingContext(parsingContext)
    {
    }

    OperatorParamType getParamType() const
    {
        return _paramType;
    }

    std::shared_ptr<ParsingContext> getParsingContext() const
    {
        return _parsingContext;
    }

    //Must be strongly virtual for successful serialization
    virtual ~OperatorParam()
    {
    }

protected:
    OperatorParamType _paramType;
    std::shared_ptr<ParsingContext> _parsingContext;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _paramType;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

class OperatorParamReference: public OperatorParam
{
public:
    OperatorParamReference() : OperatorParam(),
        _arrayName(""),
        _objectName(""),
        _inputNo(-1),
        _objectNo(-1),
        _inputScheme(false)
    {
    }

    OperatorParamReference(
            OperatorParamType paramType,
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName,
            const std::string& objectName, bool inputScheme):
        OperatorParam(paramType, parsingContext),
        _arrayName(arrayName),
        _objectName(objectName),
        _inputNo(-1),
        _objectNo(-1),
        _inputScheme(inputScheme)
    {}

    const std::string& getArrayName() const
    {
        return _arrayName;
    }

    const std::string& getObjectName() const
    {
        return _objectName;
    }

    int32_t getInputNo() const
    {
        return _inputNo;
    }

    int32_t getObjectNo() const
    {
        return _objectNo;
    }

    void setInputNo(int32_t inputNo)
    {
        _inputNo = inputNo;
    }

    void setObjectNo(int32_t objectNo)
    {
        _objectNo = objectNo;
    }

    bool isInputScheme() const
    {
        return _inputScheme;
    }

private:
    std::string _arrayName;
    std::string _objectName;

    int32_t _inputNo;
    int32_t _objectNo;

    bool _inputScheme;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        OperatorParam::serialize(ar, version);
        ar & _arrayName;
        ar & _objectName;
        ar & _inputNo;
        ar & _objectNo;
        ar & _inputScheme;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

class OperatorParamArrayReference: public OperatorParamReference
{
public:
    OperatorParamArrayReference() :
        OperatorParamReference(), _version(0)
    {
        _paramType = PARAM_ARRAY_REF;
    }

    OperatorParamArrayReference(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName, const std::string& objectName, bool inputScheme,
            VersionID version = 0):
        OperatorParamReference(PARAM_ARRAY_REF, parsingContext, arrayName, objectName, inputScheme),
        _version(version)
    {
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _version;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString(std::ostream &out, int indent = 0) const;

    VersionID getVersion() const;

private:
    VersionID _version;
};

class OperatorParamNamespaceReference: public OperatorParamReference
{
public:
    OperatorParamNamespaceReference()
        : OperatorParamReference()
        , _nsId(rbac::INVALID_NS_ID)
    {
        _paramType = PARAM_NS_REF;
    }

    OperatorParamNamespaceReference(const std::shared_ptr<ParsingContext>& parseCtx,
                                    NamespaceDesc const& nsDesc)
        : OperatorParamReference(PARAM_NS_REF,
                                 parseCtx,
                                 nsDesc.getName(), // The "array name", counterintuitively.
                                 "",
                                 nsDesc.isIdValid()) // Invalid id implies an output parameter,
                                                     // but valid can be either input or output.
        , _nsId(nsDesc.getId())
    { }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _nsId;
    }

    void toString(std::ostream &out, int indent = 0) const override;

    rbac::ID getNsId() const { return _nsId; }
    std::string getNsName() const { return getArrayName(); }
    NamespaceDesc getNamespace() const
    {
        return NamespaceDesc(getArrayName(), _nsId);
    }

private:
    rbac::ID _nsId;
};

class OperatorParamAttributeReference: public OperatorParamReference
{
public:
    OperatorParamAttributeReference() :
        OperatorParamReference(),
        _sortAscent(true)
    {
        _paramType = PARAM_ATTRIBUTE_REF;
    }

    OperatorParamAttributeReference(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName,
            const std::string& objectName,
            bool inputScheme):
        OperatorParamReference(PARAM_ATTRIBUTE_REF,
                               parsingContext,
                               arrayName,
                               objectName,
                               inputScheme),
        _sortAscent(true)
    {}

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
        ar & _sortAscent;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;

    bool getSortAscent() const
    {
        return _sortAscent;
    }

    void setSortAscent(bool sortAscent)
    {
        _sortAscent = sortAscent;
    }

private:
    //Sort quirk
    bool _sortAscent;
};

class OperatorParamDimensionReference: public OperatorParamReference
{
public:
    OperatorParamDimensionReference() : OperatorParamReference()
    {
        _paramType = PARAM_DIMENSION_REF;
    }

    OperatorParamDimensionReference(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& arrayName,
            const std::string& objectName,
            bool inputScheme):
        OperatorParamReference(PARAM_DIMENSION_REF, parsingContext, arrayName, objectName, inputScheme)
    {}

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParamReference>(*this);
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

class OperatorParamLogicalExpression: public OperatorParam
{
public:
    OperatorParamLogicalExpression() : OperatorParam()
    {
        _paramType = PARAM_LOGICAL_EXPRESSION;
    }

    OperatorParamLogicalExpression(
        const std::shared_ptr<ParsingContext>& parsingContext,
        const std::shared_ptr<LogicalExpression>& expression,  Type expectedType,
        bool constant = false):
            OperatorParam(PARAM_LOGICAL_EXPRESSION, parsingContext),
            _expression(expression),
            _expectedType(expectedType),
            _constant(constant)
    {

    }

    std::shared_ptr<LogicalExpression> getExpression() const
    {
        return _expression;
    }

    const  Type& getExpectedType() const
    {
        return _expectedType;
    }

    bool isConstant() const
    {
        return _constant;
    }

private:
    std::shared_ptr<LogicalExpression> _expression;
    Type _expectedType;
    bool _constant;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        assert(0);
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

class OperatorParamPhysicalExpression : public OperatorParam
{
public:
    OperatorParamPhysicalExpression() : OperatorParam()
    {
        _paramType = PARAM_PHYSICAL_EXPRESSION;
    }

    OperatorParamPhysicalExpression(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::shared_ptr<Expression>& expression,
            bool constant = false):
        OperatorParam(PARAM_PHYSICAL_EXPRESSION, parsingContext),
        _expression(expression),
        _constant(constant)
    {}

    std::shared_ptr<Expression> getExpression() const
    {
        return _expression;
    }

    bool isConstant() const
    {
        return _constant;
    }

private:
    std::shared_ptr<Expression> _expression;
    bool _constant;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);

        if (Archive::is_loading::value) {
            // TO-DO: If the de-serialized pointer may be equivalent to a previously-deserialized pointer,
            // the trick at PhysicalQueryPlanNode::serialize() may be used.
            Expression* e;
            ar & e;
            _expression = std::shared_ptr<Expression>(e);
        }
        else {
            Expression* e = _expression.get();
            ar & e;
        }

        ar & _constant;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};


class OperatorParamSchema: public OperatorParam
{
public:
    OperatorParamSchema() : OperatorParam()
    {
        _paramType = PARAM_SCHEMA;
    }

    OperatorParamSchema(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const ArrayDesc& schema):
        OperatorParam(PARAM_SCHEMA, parsingContext),
        _schema(schema)
    {}

    const ArrayDesc& getSchema() const
    {
        return _schema;
    }

private:
    ArrayDesc _schema;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _schema;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Call toString on interesting children. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

class OperatorParamAggregateCall: public OperatorParam
{
public:
    OperatorParamAggregateCall() : OperatorParam()
    {
        _paramType = PARAM_AGGREGATE_CALL;
    }

    OperatorParamAggregateCall(
            const std::shared_ptr<ParsingContext>& parsingContext,
            const std::string& aggregateName,
            std::shared_ptr <OperatorParam> const& inputAttribute,
            const std::string& alias):
        OperatorParam(PARAM_AGGREGATE_CALL, parsingContext),
        _aggregateName(aggregateName),
        _inputAttribute(inputAttribute),
        _alias(alias)
    {}

    std::string const& getAggregateName() const
    {
        return _aggregateName;
    }

    std::shared_ptr<OperatorParam> const& getInputAttribute() const
    {
        return _inputAttribute;
    }

    void setAlias(const std::string& alias)
    {
        _alias = alias;
    }

    std::string const& getAlias() const
    {
        return _alias;
    }

private:
    std::string _aggregateName;
    std::shared_ptr <OperatorParam> _inputAttribute;
    std::string _alias;

public:
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
        ar & _aggregateName;

        if (Archive::is_loading::value) {
            // TO-DO: If the de-serialized pointer may be equivalent to a previously-deserialized pointer,
            // the trick at PhysicalQueryPlanNode::serialize() may be used.
            OperatorParam* op;
            ar & op;
            _inputAttribute = std::shared_ptr<OperatorParam>(op);
        }
        else {
            OperatorParam* op = _inputAttribute.get();
            ar & op;
        }

        ar & _alias;
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Call toString on interesting children. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

/**
 * @brief Little addition to aggregate call parameter. Mostly for built-in COUNT(*).
 */
class OperatorParamAsterisk: public OperatorParam
{
public:
    OperatorParamAsterisk(): OperatorParam()
    {
        _paramType = PARAM_ASTERISK;
    }

    OperatorParamAsterisk(
            const std::shared_ptr<ParsingContext>& parsingContext
            ) :
        OperatorParam(PARAM_ASTERISK, parsingContext)
    {
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<OperatorParam>(*this);
    }

    /**
    * Retrieve a human-readable description.
    * Append a human-readable description of this onto str. Description takes up
    * one or more lines. Append indent spacer characters to the beginning of
    * each line. Call toString on interesting children. Terminate with newline.
    * @param[out] stream to write to
    * @param[in] indent number of spacer characters to start every line with.
    */
    virtual void toString(std::ostream &out, int indent = 0) const;
};

template<class Archive>
void registerLeafDerivedOperatorParams(Archive& ar)
{
    ar.register_type(static_cast<OperatorParamArrayReference*>(NULL));
    ar.register_type(static_cast<OperatorParamAttributeReference*>(NULL));
    ar.register_type(static_cast<OperatorParamDimensionReference*>(NULL));
    ar.register_type(static_cast<OperatorParamLogicalExpression*>(NULL));
    ar.register_type(static_cast<OperatorParamPhysicalExpression*>(NULL));
    ar.register_type(static_cast<OperatorParamSchema*>(NULL));
    ar.register_type(static_cast<OperatorParamAggregateCall*>(NULL));
    ar.register_type(static_cast<OperatorParamNamespaceReference*>(NULL));
    ar.register_type(static_cast<OperatorParamAsterisk*>(NULL));
}

typedef std::shared_ptr<OperatorParam> Parameter;
typedef std::vector<Parameter> Parameters;
typedef std::map<std::string, Parameter> KeywordParameters;

/**
 * Convert logical or physical parameter to string.
 *
 * @param p   parameter to convert, must be a constant logical or
 *            physical expression
 * @return string value extracted from parameter
 * @throw internal error unless p can be evaluated as a string.
 *
 * For good or ill the syntax for evaluating logical vs. physical
 * expressions is different.  We do this so often for strings that
 * it's convenient to have a function that can be called from both
 * PhysicalOperator and LogicalOperator subclasses.
 */
std::string paramToString(Parameter const& p);

} // namespace

#endif /* OPERATOR_PARAM_H_ */
