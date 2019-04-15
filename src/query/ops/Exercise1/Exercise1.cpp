#include "Exercise1.h"

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.Exercise1"));

    Exercise1::Exercise1(ArrayDesc &array, Coordinates lowPos, Coordinates highPos,
                         std::shared_ptr<scidb::Array> &input, const std::shared_ptr<scidb::Query> &query): DelegateArray(array, input),
                         Exercise1LowPos(lowPos),
                         Exercise1HighPos(highPos),
                         dims(desc.getDimensions()),
                         inputDims(input->getArrayDesc().getDimensions()) { _query = query;}



    Exercise1::Exercise1(ArrayDesc &array, AttributeDesc attrDesc, Coordinates lowPos, Coordinates highPos,
                         std::shared_ptr<scidb::Array> &input, const std::shared_ptr<scidb::Query> &query): DelegateArray(array, input),
                                                                                                            Exercise1AttributeDesc(attrDesc),
                                                                                                            Exercise1LowPos(lowPos),
                                                                                                            Exercise1HighPos(highPos),
                                                                                                            dims(desc.getDimensions()),
                                                                                                            inputDims(input->getArrayDesc().getDimensions()) { _query = query;}
    Exercise1::Exercise1(ArrayDesc &array, AttributeID attrID, Coordinates lowPos, Coordinates highPos,
                         std::shared_ptr<scidb::Array> &input, const std::shared_ptr<scidb::Query> &query): DelegateArray(array, input),
                                                                                                            Exercise1AttributeID(attrID),
                                                                                                            Exercise1LowPos(lowPos),
                                                                                                            Exercise1HighPos(highPos),
                                                                                                            dims(desc.getDimensions()),
                                                                                                            inputDims(input->getArrayDesc().getDimensions()) { _query = query;}
    DelegateArrayIterator *Exercise1::createArrayIterator(AttributeID attrID) const
    {
        return new Exercise1Iterator(*this, attrID);
    }


    Exercise1Iterator::Exercise1Iterator(Exercise1 const& Exercise1, AttributeID attrID, bool doRestart)
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

    void Exercise1Iterator::fillSparseChunk(size_t i)
    {
        LOG4CXX_TRACE(logger, "SubArrayIterator::fillSparseChunk i=" << i
                                                                     << " inpos=" << CoordsToStr(inPos));
        Dimensions const& dims = array.dims;
        if (i == dims.size()) {
            if (inputIterator->setPosition(inPos)) {
                ConstChunk const& inChunk = inputIterator->getChunk();
                std::shared_ptr<ConstChunkIterator> inIterator = inChunk.getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS|
                                                                                          ConstChunkIterator::IGNORE_EMPTY_CELLS);
                while (!inIterator->end()) {
                    Coordinates const& inChunkPos = inIterator->getPosition();
                    array.in2out(inChunkPos, outChunkPos);
                    if (outIterator->setPosition(outChunkPos)) {
                        LOG4CXX_TRACE(logger, "SubArrayIterator::fillSparseChunk"
                                << " writing item at "
                                << CoordsToStr(outChunkPos));
                        outIterator->writeItem(inIterator->getItem());
                    }
                    ++(*inIterator);
                }
            }
        } else {
            fillSparseChunk(i+1);

            size_t interval = dims[i].getChunkInterval() - 1;
            inPos[i] += interval;
            fillSparseChunk(i+1);
            inPos[i] -= interval;
        }
    }

    ConstChunk const& Exercise1Iterator::getChunk()
    {
        if (!chunkInitialized) {

            chunkInitialized = true;

            ArrayDesc const& desc = array.getArrayDesc();
            Address addr(attr, outPos);
            sparseChunk.initialize(&array, &desc, addr, CompressorType::NONE);

            int mode(0);
            AttributeDesc const* emptyAttr = desc.getEmptyBitmapAttribute();
            if (emptyAttr != NULL && emptyAttr->getId() != attr) {
                Address emptyAddr(emptyAttr->getId(), outPos);
                sparseBitmapChunk.initialize(&array, &desc, emptyAddr, CompressorType::NONE);
                sparseChunk.setBitmapChunk(&sparseBitmapChunk);
            }

            outIterator = sparseChunk.getIterator(Query::getValidQueryPtr(array._query), mode);
            fillSparseChunk(0);
            outIterator->flush();

            LOG4CXX_TRACE(logger, "SubArrayIterator::getChunk: "
                    <<" attr=" << attr
                    <<", outCoord=" << outPos
                    <<", chunk isEmpty="<<sparseChunk.isEmpty());
        }
        ASSERT_EXCEPTION(sparseChunk.isInitialized(), "SubArrayIterator::getChunk; ");
        return sparseChunk;
    }

    bool Exercise1Iterator::end()
    {
        return !hasCurrent;
    }

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
    //  setInputPosition(0) 0번째 dimension부터 n dimension까지 반복해서 inPos로 설정.
    bool Exercise1Iterator::setInputPosition(size_t i)
    {
        Dimensions const& dims = array.dims;
        chunkInitialized =false;
        // 마지막 dimension이면 iterator을 통해 setposition
        if (i == dims.size()) {
            //ConstArrayIterator
            bool ret = inputIterator->setPosition(inPos);
            return ret;
        }
        // 재귀
        if (setInputPosition(i+1)) {
            return true;
        }
        //
        size_t interval = dims[i].getChunkInterval() - 1;
        inPos[i] += interval;
        bool rc = setInputPosition(i+1);
        inPos[i] -= interval;
        return rc;

    }

    //output array의 position을 설정함
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

    Coordinates const& Exercise1Iterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }
    // output array의 position에서 input array의 position으로 변환
    void Exercise1::out2in(Coordinates const& out, Coordinates& in) const
    {
        for (size_t i = 0, n = out.size(); i < n; i++) {
            in[i] = out[i] + Exercise1LowPos[i];
        }
    }
    //input array의 position에서 Output array의 position으로 변환
    void Exercise1::in2out(Coordinates const& in, Coordinates& out) const
    {
        for (size_t i = 0, n = in.size(); i < n; i++) {
            out[i] = in[i] - Exercise1LowPos[i];
        }
    }
}
