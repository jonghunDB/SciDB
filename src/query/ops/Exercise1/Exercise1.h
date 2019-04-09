#include <array/DelegateArray.h>
#include <array/Metadata.h>
/**
 * @file Array.h
 *
 * Note: Array.h was split into
 *   8 main classes:
 *     ConstIterator.h ConstArrayIterator.h ArrayIterator.h ConstItemIterator.h
 *     ConstChunk.h Chunk.h
 *     ConstChunkIterator.h ChunkIterator.h
 *   and 5 ancillary classes:
 *     CompressedBuffer.h MemoryBuffer.h SharedBuffer.h
 *     PinBuffer.h UnPinner.h
 * Use "git blame -C" to show the code motion of the splitup.
 *
 */
namespace scidb
{
    using namespace scidb;

    class Exercise1;
    class Exercise1Iterator;

    //class Exercise1
    class Exercise1 : public DelegateArray
    {
    private:
        Coordinates Exercise1LowPos;
        Coordinates Exercise1HighPos;
        Dimensions const& dims;
        Dimensions const& inputDims;


    public:
        virtual ~Exercise1()
        {}
        Exercise1( ArrayDesc& d, Coordinates lowPos, Coordinates highPos, std::shared_ptr<Array>& input, std::shared_ptr<Query> const& query );

    };
    //class Exercise1Iterator
/*
    * Iterator의 계층적 구조
    * ConstIterator
    *   ConstArrayIterator
    *       DelegateArrayIterator
    *           ExerciseIterator
 */
/*
 * ConstIterator
 *
 * bool end() : chunk의 끝에 도달 했는지 확인
 * void ++() : 다음 원소로 현재의 Cursor을 이동
 * Coordinates const& getposition() : 청크에서 현재 위치의 원소의 Coordinates를 가져옴
 * bool setposition(Coordiantes const& pos) : iterator 현재위치를 pos값으로 설정
 * void restart() : iterator을 다시 시작함. chunk의 first position으로 커서를 옮김
*/


/*
 *
 * ConstArrayIterator
 *
 *
 * bool setposition(Coordinates const& pos) override;
 * void restart() override;
 * virtual ConstChunk const& getchunk() 현재의 청크를 가져옴
 *
 *
 * */



    class Exercise1Iterator : public DelegateArrayIterator
    {
    private:


    public:
        virtual ~Exercise1Iterator()
        {}

    };
}// namespace scidb