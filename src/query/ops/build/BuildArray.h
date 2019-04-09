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
 * @file BuildArray.h
 *
 * @brief The implementation of the array iterator for the build operator
 *
 */

#ifndef BUILD_ARRAY_H_
#define BUILD_ARRAY_H_

#include <string>
#include <vector>

#include <array/DelegateArray.h>
#include <array/Metadata.h>
#include <query/FunctionDescription.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>

using namespace std;

namespace scidb
{

class BuildArray;
class BuildArrayIterator;
class BuildChunkIterator;

class BuildChunk : public ConstChunk
{
  public:
    virtual const ArrayDesc& getArrayDesc() const;
    virtual const AttributeDesc& getAttributeDesc() const;
    virtual Coordinates const& getFirstPosition(bool withOverlap) const;
    virtual Coordinates const& getLastPosition(bool withOverlap) const;
    virtual std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    virtual CompressorType getCompressionMethod() const;
    virtual Array const& getArray() const;

    void setPosition(Coordinates const& pos);

    BuildChunk(BuildArray& array, AttributeID attrID);

  private:
    BuildArray& array;
    Coordinates firstPos;
    Coordinates lastPos;
    Coordinates firstPosWithOverlap;
    Coordinates lastPosWithOverlap;
    AttributeID attrID;
};

class BuildChunkIterator : public ConstChunkIterator
{
public:
    int getMode() const override;
    bool isEmpty() const override;
    Value const& getItem() override;
    void operator ++() override;
    bool end() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    ConstChunk const& getChunk() override;
    virtual std::shared_ptr<Query> getQuery() { return _query; }

    BuildChunkIterator(BuildArray& array, ConstChunk const* chunk, AttributeID attrID, int iterationMode);

  private:
    int iterationMode;
    BuildArray& array;
    Coordinates const& firstPos;
    Coordinates const& lastPos;
    Coordinates currPos;
    bool hasCurrent;
    AttributeID attrID;
    ConstChunk const* chunk;
    FunctionPointer _converter;
    Value _value;
    Value _trueValue;
    Expression _expression;
    ExpressionContext _params;
    bool _nullable;
    std::shared_ptr<Query> _query;
};

class BuildArrayIterator : public ConstArrayIterator
{
friend class BuildChunkIterator;

public:
    ConstChunk const& getChunk() override;
    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

    BuildArrayIterator(BuildArray& array, AttributeID id);

private:
    void nextChunk();

    BuildArray& array;
    bool hasCurrent;
    bool chunkInitialized;
    BuildChunk chunk;
    Dimensions const& dims;
    Coordinates currPos;
};

class BuildArray : public Array
{
friend class BuildArrayIterator;
friend class BuildChunkIterator;
friend class BuildChunk;

public:
        virtual ArrayDesc const& getArrayDesc() const;
        virtual std::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const;

        BuildArray(std::shared_ptr<Query>& query, ArrayDesc const& desc, std::shared_ptr<Expression> expression);

private:
    ArrayDesc _desc;
    std::shared_ptr<Expression> _expression;
    std::vector<BindInfo> _bindings;
     FunctionPointer _converter;
    size_t nInstances;
    size_t instanceID;
};

}

#endif
