#include "Exercise1.h"

namespace scidb
{
    Exercise1::Exercise1(scidb::ArrayDesc &array, scidb::Coordinates lowPos, scidb::Coordinates highPos,
                         std::shared_ptr<scidb::Array> &input, const std::shared_ptr<scidb::Query> &query): DelegateArray(array, input),
              Exercise1LowPos(lowPos),
              Exercise1HighPos(highPos),
              dims(desc.getDimensions()),
              inputDims(input->getArrayDesc().getDimension()) {
        _query = query;


    }


    DelegateArrayIterator *Exercise1::createArrayIterator(scidb::AttributeID attrID) const
    {
        return new Exercise1Iterator(*this, attrID);
    }

    Exercise1Iterator::Exercise1Iterator(scidb::Exercise1 const& Exercise1, scidb::AttributeID attrID, bool doRestart)
    : DelegateArrayIterator(Exercise1, attrID, Exercise1.inputArray->getConstIterator(attrID)),
    array(Exercise1),
    outPos(Exercise1.Exercise1LowPos.size()),
    inPos(outPos.size()),
    hasCurrent(false),
    outChunkPos(outPos.size())
    {
        if(doRestart)
        {
            restart();
        }
    }
    //
    void Exercise1Iterator::restart()
    {
        const Dimensions& dims =array.dims;
        size_t nDims = dims.size();
        for(size_t i = 0 ; i <nDims ; i++)
        {
            outPos[i] = 0;
        }
        chunkInitialized =false;
        outPos[nDims-1] -= dims[nDims-1].getChunkInterval();
        ++(*this);
    }

    void Exercise1Iterator::operator ++()
    {
        const Dimensions& dims = array.dims;
        size_t nDims = dims.size();
        chunkInitialized =false;
        while(true) {
            size_t i = nDims - 1;
            while((outPos[i] += dims[i].getChunkInterval()) > dims[i].getEndMax()) {
                if (i ==0) {
                    hasCurrent = false;
                    return;
                }
                outPos[i--] =0;
            }
            array.out2in(outPos, inPos);
            if (setInputPosition(0)) {
                hasCurrent = true;
                return;
            }
        }
    }

    bool Exercise1Iterator::setInputPosition(size_t i)
    {
        Dimensions const& dims = array.dims;
        chunkInitialized =false;

        if (i == dims.size()) {
            bool ret = inputIterator->setPosition(inPos);
            return ret;
        }
        if (setInputPosition(i+1)) {
            return true;
        }
        size_t interval = dims[i].getChunkInterval() - 1;
        inPos[i] += interval;
        bool rc = setInputPosition(i+1);
        inPos[i] -= interval;
        return rc;

    }
    bool Exercise1Iterator::setPosition(Coordinates const& pos)
    {
        if( !array.getArrayDesc().contains(pos) )
        {
            return hasCurrent = false;
        }
        outPos = pos;
        array.getArrayDesc().getChunkPositionFor(outPos);
        array.out2in(outPos, inPos);
        return hasCurrent = setInputPosition(0);
    }
    Cooriantes const& SubArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;

    }
    void Exercise1::out2in(Coordinates const& out, Coordinates& in) const
    {
        for (size_t i = 0, n = out.size(); i < n; i++) {
            in[i] = out[i] + subarrayLowPos[i];
        }
    }

    void Exercise1::in2out(Coordinates const& in, Coordinates& out) const
    {
        for (size_t i = 0, n = in.size(); i < n; i++) {
            out[i] = in[i] - subarrayLowPos[i];
        }
    }



}
