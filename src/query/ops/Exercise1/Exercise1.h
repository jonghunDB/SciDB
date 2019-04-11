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
/*
 * Array
 *  DelegateArray
 *      Exercise1
 *
 * */
/*
 * DelegateArray
 *
 * DelegateArray(ArrayDesc const& desc, std::shared_ptr<Array> input, bool isClone =false);
 *
 * */
    //class Exercise1
    class Exercise1 : public DelegateArray
    {
        friend class Exercise1Iterator;
    private:
        Coordinates Exercise1LowPos;
        Coordinates Exercise1HighPos;
        Dimensions const& dims;
        Dimensions const& inputDims;

//      std::set<Coordiantes, CoordinatesLess> _chunkSet;
//      void buildChunkSet();
//      void addChunksToSet(Coordinates outChunkCoords, size_t dim =0);
/*
 * const를 쓰는 이유.
 * 크게 2가지로 볼 수 있다.
 * 1. const로 설정함으로써 read-only version이라고 명시하는것. 즉 permanently하게 저장을 함
 * 2. optimization문제, 컴파일하는 과정에서 by-value로 값을 전달하기 때문에 optimization하는 수고를 덜게 됨.
 * not call by value but call by reference
 *
 * const 변수  :변수가 참조하는 포인터를 상수화
 * 변수 const  :변수가 참조하는 데이터를 상수화
 * void function() const : 함수를 상수화, 멤버변수의 리턴 불가 + 상수화되지 않은 함수 호출 불가
 *
 * *
 */
    public:
//      virtual ~Exercise1()
//      {}
        Exercise1( ArrayDesc& d, Coordinates lowPos, Coordinates highPos, std::shared_ptr<Array>& input, std::shared_ptr<Query> const& query );
        DelegateArrayIterator* createArrayIterator(AttributeID attrID) const;

        void out2in(Coordinates const& out, Coordinates& in) const;
        void in2out(Coordinates const& in, Coordinates& out) const;
    };
    //class Exercise1Iterator
/*
    * Iterator의 계층적 구조
    * ConstIterator                          :
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
 * local instance에서 array의 접근가능한 chunk들에 대해 iterator
 * iteration order은 구체화되지 않음
 *
 * bool setposition(Coordinates const& pos) override;
 * void restart() override;
 * virtual ConstChunk const& getchunk() 현재의 청크를 가져옴
 *
 *
 * */
/* DelegateArrayIterator
 *
 * array/DelegateArray.h에 구현되어있음
 * DelegateArrayIterator(DelegateArray const& delegate, AttributeID attrID, std::shared_ptr<ConstArrayIterator> inputIterator);
 *
 * bool end() override
 * void operator++() override;
 * Coordinates const& getPosition() override;
 * bool setPosition(Coordinates const& pos) override;
 * void restart() override;
 *
 * DelegateArray const& array;
 *
 * AttributeID attr; Metadata에서 AttributeID 를 가져옴
 *
 * */
    class Exercise1Iterator : public DelegateArrayIterator
    {
    protected:
        bool setInputPosition(size_t i);

        Exercise1 const& array;
        Coordinates outPos;
        Coordinates inPos;
        bool hasCurrent;

        Coordinates outChunkPos;
    public:
        Exercise1Iterator(Exercise1 const& exercise1, AttributeID attrID, bool doRestart = true);
        virtual ~Exercise1Iterator()
        {}

        ConstChunk const& getChunk() override;

        bool end() override;
        void operator ++() override;
        Coordinates const& getPosition() override;
        bool setPosition(Coordinates const& pos) override;
        void restart() override;

    };
}// namespace scidb