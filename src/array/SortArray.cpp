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

/*
 *  SortArray.cpp
 *
 *  Created on: Aug 14, 2013
 *  Based on implementation of operator sort
 */

#include <array/SortArray.h>

#include <vector>
#include <list>

#include <boost/scope_exit.hpp>

#include <array/MemArray.h>
#include <array/MergeSortArray.h>
#include <array/Metadata.h>
#include <array/TupleArray.h>
#include <query/Operator.h>
#include <system/Config.h>
#include <util/Timing.h>
#include <util/iqsort.h>

namespace scidb {

    using namespace std;
    using namespace boost;

    log4cxx::LoggerPtr SortArray::logger(log4cxx::Logger::getLogger("scidb.array.sortarray"));

    /**
     * Helper class SortIterators
     */

    /**
     * Constructor -- create iterators and position them at the correct
     * chunk
     */
    SortArray::SortIterators::SortIterators(std::shared_ptr<Array>const& input,
                                            size_t shift,
                                            size_t step) :
        _arrayIters(input->getArrayDesc().getAttributes().size()),
        _shift(shift),
        _step(step)
    {
        // Create the iterators
        for (AttributeID i = 0; i < _arrayIters.size(); i++)
        {
            _arrayIters[i] = input->getConstIterator(i);
        }

        // Position iterators at the correct chunk
        for (size_t j = _shift; j != 0 && !_arrayIters[0]->end(); --j)
        {
            for (size_t i = 0; i < _arrayIters.size(); i++)
            {
                ++(*_arrayIters[i]);
            }
        }
    }

    /**
     * Advance iterators by (step-1) chunks.
     * The goal is to enable SortJob::run() to read chunks: shift, shift+step, shift+2*step, ...
     * One may wonder: but to achieve the goal, shouldn't we advance (step) chunks instead of (step-1)?
     * The answer is: yes; but SortJob::run() calls TupleArray::append(), which advances 1 chunk.
     */
    void SortArray::SortIterators::advanceIterators()
    {
        for (size_t j = _step-1; j != 0 && !_arrayIters[0]->end(); --j)
        {
            for (size_t i = 0; i < _arrayIters.size(); i++)
            {
                ++(*_arrayIters[i]);
            }
        }
    }


    /**
     * Helper class SortJob
     */

    /**
     * The input array may not have an empty tag,
     * but the output array has an empty tag.
     */
    SortArray::SortJob::SortJob(SortArray* sorter,
                                std::shared_ptr<Query>const& query,
                                std::shared_ptr<PhysicalOperator>const& phyOp,
                                size_t id,
                                SortIterators* iters)
        : Job(query, std::string("SortArrayJob")),
          _sorter(*sorter),
          _sortIters(*iters),
          _complete(false),
          _id(id)
    {
    }

    /**
     * Here we try to partition part of the array into manageable sized chunks
     * and then sort them in-memory.  Each resulting sorted run is converted to
     * a MemArray and pushed onto the result list.  If we run out of input, or
     * we reach the limit on the size of the result list, we stop
     */
    void SortArray::SortJob::run()
    {
        try {
            arena::ScopedArenaTLS arenaTLS(_query->getArena());

            // At the end of run(), we must always mark ourselves on the stopped
            // job list and signal the main thread.
            BOOST_SCOPE_EXIT ( (&_sorter) (&_id) )
            {
                ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SA);

                _sorter._stoppedJobs[_id] = _sorter._runningJobs[_id];
                ++ _sorter._nStoppedJobs;
                _sorter._runningJobs[_id].reset();
                -- _sorter._nRunningJobs;
                _sorter._sortEvent.signal();
            } BOOST_SCOPE_EXIT_END;

            // TupleArray must handle the case that outputDesc.getAttributes().size()
            // is 1 larger than arrayIterators.size(), i.e. the case when the input array
            // does not have an empty tag (but the output array does).
            //
            size_t tupleArraySizeHint = _sorter._memLimit / _sorter._tupleSize;
            std::shared_ptr<TupleArray> buffer =
                make_shared<TupleArray>(_sorter._outputSchemaExpanded,
                                        _sortIters.getIterators(),
                                        _sorter.getInputArrayDesc(),
                                        0,
                                        tupleArraySizeHint,
                                        16*MiB,
                                        _sorter._arena,
                                        _sorter.preservePositions(),
                                        _sorter._skipper);

            // Append chunks to buffer until we run out of input or reach limit
            bool limitReached = false;
            while (!_sortIters.end() && !limitReached)
            {
                buffer->append(_sorter.getInputArrayDesc(), _sortIters.getIterators(), 1, _sorter._skipper);
                size_t currentSize = buffer->getSizeInBytes();
                LOG4CXX_TRACE(logger, "[SortArray] currentSize: " << currentSize);
                if (currentSize > _sorter._memLimit)
                {
                    std::shared_ptr<Array> baseBuffer =
                        static_pointer_cast<TupleArray, Array> (buffer);
                    buffer->sort(_sorter._tupleComp);
                    buffer->truncate();
                    {
                        std::shared_ptr<Array> ma(make_shared<MemArray>(baseBuffer->getArrayDesc(),
                                                                        getQuery()));
                        ma->appendHorizontal(baseBuffer);
                        ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SA);
                        _sorter._results.push_back(ma);
                        _sorter._runsProduced++;
                        LOG4CXX_DEBUG(logger, "[SortArray] At currentSize " << currentSize << " Produced sorted run # " << _sorter._runsProduced);
                        if (_sorter._results.size() > _sorter._pipelineLimit)
                        {
                            limitReached = true;
                        }
                    }
                    if (limitReached) {
                        buffer.reset();
                    } else {
                        buffer = std::make_shared<TupleArray>(_sorter._outputSchemaExpanded,
                                                    _sortIters.getIterators(),
                                                    _sorter._inputSchema,
                                                    0,
                                                    tupleArraySizeHint,
                                                    16*MiB,
                                                    _sorter._arena,
                                                    _sorter.preservePositions(),
                                                    _sorter._skipper);
                    }
                }
                _sortIters.advanceIterators();
            }

            // May have some left-overs --- only in the case where we are at the end
            if (_sortIters.end())
            {
                if (buffer && buffer->size())
                {
                    std::shared_ptr<Array> baseBuffer =
                        static_pointer_cast<TupleArray, Array> (buffer);
                    buffer->sort(_sorter._tupleComp);
                    buffer->truncate();
                    {
                        std::shared_ptr<Array> ma(make_shared<MemArray>(baseBuffer->getArrayDesc(),
                                                                        getQuery()));
                        ma->appendHorizontal(baseBuffer);
                        ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SA);
                        _sorter._results.push_back(ma);
                        _sorter._runsProduced++;
                        LOG4CXX_DEBUG(logger, "[SortArray] Produced sorted run # " << _sorter._runsProduced);
                    }
                }
                _complete = true;
            }
        } catch (std::bad_alloc const& e) {
            throw SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
        }
    }


    /**
     * Helper class MergeJob
     */

    /**
     * Constructor
     */
    SortArray::MergeJob::MergeJob(SortArray* sorter,
                                  std::shared_ptr<Query>const& query,
                                  std::shared_ptr<PhysicalOperator>const& phyOp,
                                  size_t id) :
        Job(query, std::string("SortArrayJob")),
        _sorter(*sorter),
        _id(id)
    {
    }

    /**
     * Remove a group of arrays from the list, merge them using a MergeSortArray,
     * then add the result back to the end of the list.
     */
    void SortArray::MergeJob::run()
    {
        try {
            arena::ScopedArenaTLS arenaTLS(_query->getArena());

            vector< std::shared_ptr<Array> > mergeStreams;
            std::shared_ptr<Array> merged;
            std::shared_ptr<Array> materialized;

            // At the end of run(), we must always put the result (if it exists) on the end of the
            // list, mark ourselves on the stopped job list, and signal the main thread
            BOOST_SCOPE_EXIT ( (&materialized) (&_sorter) (&_id) )
            {
                ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SA);

                if (materialized.get())
                {
                    _sorter._results.push_back(materialized);
                }
                _sorter._stoppedJobs[_id] = _sorter._runningJobs[_id];
                ++ _sorter._nStoppedJobs;
                _sorter._runningJobs[_id].reset();
                -- _sorter._nRunningJobs;
                _sorter._sortEvent.signal();
            } BOOST_SCOPE_EXIT_END;

            // remove the correct number of streams from the list
            {
                ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SA);

                size_t nSortedRuns = _sorter._results.size();
                size_t currentStreams =
                    nSortedRuns < _sorter._nStreams ? nSortedRuns : _sorter._nStreams;
                mergeStreams.resize(currentStreams);

                LOG4CXX_DEBUG(logger, "[SortArray] Found " << currentStreams << " runs to merge");

                for (size_t i = 0; i < currentStreams; i++)
                {
                    mergeStreams[i] = _sorter._results.front();
                    _sorter._results.pop_front();
                }
            }

            // merge the streams -- true means the array contains local data only
            size_t nStreams = mergeStreams.size();
            std::shared_ptr<vector<size_t> > streamSizes = make_shared< vector<size_t> > (nStreams);
            for (size_t i=0; i<nStreams; ++i) {
                (*streamSizes)[i] = -1;
            }
            merged = make_shared<MergeSortArray>(getQuery(),
                                            _sorter._outputSchemaExpanded,
                                            mergeStreams,
                                            _sorter._tupleComp,
                                            0,  // Do not add an offset to the cell's coordinates.
                                            streamSizes // Using -1 preserves the behavior of the original code here.
                                            );

            // false means perform a horizontal copy (copy all attributes for chunk 1,
            // all attributes for chunk 2,...)
            materialized = make_shared<MemArray>(merged->getArrayDesc(), getQuery());
            materialized->appendHorizontal(merged);
        } catch (std::bad_alloc const& e) {
            throw SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
        }
    }

    /**
     *
     */
    SortArray::SortArray(const ArrayDesc& inputSchema,arena::ArenaPtr const& arena,bool preservePositions,size_t chunkSize)
         : _inputSchema(inputSchema),
           _arena(arena),
           _sortEvent(),
           _nRunningJobs(0),
           _nStoppedJobs(0),
           _runsProduced(0),
           _partitionComplete(arena),
           _sortIterators(arena),
           _runningJobs(arena),
           _waitingJobs(arena),
           _stoppedJobs(arena),
           _preservePositions(preservePositions),
           _skipper(NULL)
     {
         calcOutputSchema(inputSchema, chunkSize);
     }

    /**
     * Output schema matches input attributes, but contains only one dimension, "n".
     * An emptytag attribute is added to the schema if it doesn't already exist.
     * ChunkSize is specified in the constructor and used here.
     */
    void SortArray::calcOutputSchema(const ArrayDesc& inputSchema, size_t chunkSize)
    {
        //Let's always call the output dimension "n". Because the input dimension no longer has any meaning!
        //It could've been "time", or "latitude", or "price" but after the sort it has no value.

        //Right now we always return an unbounded array. You can use subarray to bound it if you need to
        //(but you should not need to very often!!). TODO: if we return bounded arrays, some logic inside
        //MergeSortArray gives bad results. We should fix this some day.

        //If the user does not specify a chunk size, we'll use MIN( max_logical_size, 1 million).

        size_t inputSchemaSize = inputSchema.getSize();
        if (chunkSize == 0)
        {   //not set by user

            //1M is a good/recommended chunk size for most one-dimensional arrays -- unless you are using
            //large strings or UDTs
            chunkSize = 1000000;

            //If there's no way that the input has one million elements - reduce the chunk size further.
            //This is ONLY done for aesthetic purposes - don't want to see one million "()" on the screen.
            //In fact, sometimes it can become a liability...
            if(inputSchemaSize<chunkSize)
            {
                //make chunk size at least 1 to avoid errors
                chunkSize = std::max<size_t>(inputSchemaSize,1);
            }
        }

        Dimensions newDims(1,DimensionDesc("n", 0, 0,
                                           CoordinateBounds::getMax(),
                                           CoordinateBounds::getMax(),
                                           chunkSize, 0));

        const bool excludeEmptyBitmap = true;
        Attributes attributes = inputSchema.getAttributes(excludeEmptyBitmap);
        AttributeID nAttrsIn = safe_static_cast<AttributeID>(attributes.size());
        _outputSchemaNonExpanded = ArrayDesc(inputSchema.getName(),
                                          addEmptyTagAttribute(attributes),
                                          newDims,
                                          createDistribution(psUndefined),
                                          inputSchema.getResidency());

        // the expanded version.
        if (_preservePositions) {
            attributes.push_back(AttributeDesc(
                            nAttrsIn,
                            "chunk_pos",
                            TID_INT64,
                            0, // flags
                            CompressorType::NONE  // defaultCompressionMethod
                            ));
            attributes.push_back(AttributeDesc(
                            nAttrsIn+1,
                            "cell_pos",
                            TID_INT64,
                            0, // flags
                            CompressorType::NONE  // defaultCompressionMethod
                            ));
        }
        _outputSchemaExpanded = ArrayDesc(inputSchema.getName(),
                                               addEmptyTagAttribute(attributes),
                                               newDims,
                                               createDistribution(psUndefined),
                                               inputSchema.getResidency());
    }


    /***
     * Sort works by first transforming the input array into a series of sorted TupleArrays.
     * Then the TupleArrays are linked under a single MergeSortArray which encodes the merge
     * logic within its iterator classes.  To complete the sort, we materialize the merge
     * Array.
     */
    std::shared_ptr<MemArray> SortArray::getSortedArray(std::shared_ptr<Array> inputArray,
                                                   std::shared_ptr<Query> query,
                                                   const std::shared_ptr<PhysicalOperator>& phyOp,
                                                   std::shared_ptr<TupleComparator> tcomp,
                                                   TupleSkipper* skipper
                                                   )
    {
        assert(arena::Arena::getArenaTLS() == query->getArena());

        // Timing for Sort
        LOG4CXX_DEBUG(logger, "[SortArray] Sort for array " << _outputSchemaExpanded.getName() << " begins");
        ElapsedMilliSeconds timing;

        // Init config parameters
        size_t numJobs = inputArray->getSupportedAccess() == Array::RANDOM ?
	           Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) : 1;
        _memLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER)*MiB;
        _nStreams = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_NSTREAMS);
        _pipelineLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_PIPELINE_LIMIT);

        // Turn off threading, if numJobs is 1.
        if (numJobs == 1 && _arena->supports(arena::threading)) {
            // Note from Donghui Zhang: Looks to me that the arena API could be improved.
            // Here I have a seemingly simple need: turn off threading.
            // The arena API has addThreading() but not removeThreading().
            // It seems that calling newArena() with a Options object similar to that
            // of _arena but with threading=false is the only way.
            // However, there is no way to get _arena's Options.
            // So I'm calling _arena->supports() for each feature.
            // But the pagesize option value cannot be acquired. So I'm using 16MiB.
            _arena = arena::newArena(arena::Options("sort array arena", _arena).
                                     finalizing(_arena->supports(arena::finalizing)).
                                     recycling(_arena->supports(arena::recycling)).
                                     resetting(_arena->supports(arena::resetting)).
                                     debugging(_arena->supports(arena::debugging)).
                                     threading(false).
                                     pagesize(16*MiB));
        }

        // Validate config parameters
        if (_pipelineLimit <= 1)
        {
            _pipelineLimit = 2;
        }
        if (_nStreams <= 1)
        {
            _nStreams = 2;
        }
        if (_pipelineLimit < _nStreams)
        {
            _pipelineLimit = _nStreams;
        }

        // Init sorting state
        std::shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
        _input = inputArray;
        _tupleComp = tcomp;
        _tupleSize = TupleArray::getTupleFootprint(_outputSchemaExpanded.getAttributes());
        _nRunningJobs = 0;
        _nStoppedJobs = 0;
	      _runsProduced = 0;
        _partitionComplete.resize(numJobs);
        _waitingJobs.resize(numJobs);
        _runningJobs.resize(numJobs);
        _stoppedJobs.resize(numJobs);
        _sortIterators.resize(numJobs);
        _skipper = skipper;

        // Create the iterator groups and sort jobs
        for (size_t i = 0; i < numJobs; i++)
        {
            _sortIterators[i] =
                std::shared_ptr<SortIterators>(make_shared<SortIterators>(_input, i, numJobs));
            _waitingJobs[i] =
                std::shared_ptr<Job>(make_shared<SortJob>(this,
                                            query, phyOp,
                                            i,
                                            _sortIterators[i].get()));
            _partitionComplete[i] = false;
        }

        // Main loop
        while (true)
        {
            Event::ErrorChecker ec;
            ScopedMutexLock sm(_sortLock, PTW_SML_SA);

            // Try to spawn jobs
            for (size_t i = 0; i < numJobs; i++)
            {
                if (_waitingJobs[i])
                {
                    _runningJobs[i] = _waitingJobs[i];
                    _waitingJobs[i].reset();
                    _nRunningJobs++;
                    queue->pushJob(_runningJobs[i]);
                }
            }

            // If nothing is running, get out
            if (_nRunningJobs == 0)
            {
                if (_nStoppedJobs == 0) {
                    break;
                }
            } else {
                // Wait for a job to be done
                // The reason why it is in the else clause is that,
                // in case there is no running job but some job has failed,
                // there won't be any job that will signal _sortEvent any more.
                _sortEvent.wait(_sortLock, ec, PTW_EVENT_SORT_JOB);
            }

            // Reap the stopped jobs and re-schedule
            for (size_t i = 0; i < numJobs; i++)
            {
                if (_stoppedJobs[i])
                {
                    enum nextJobType { JobNone, JobMerge, JobSort };

                    nextJobType nextJob = JobNone;
                    bool jobSuccess = true;
                    std::shared_ptr<SortJob> sJob;

                    jobSuccess = _stoppedJobs[i]->wait();

                    if (!_failedJob && jobSuccess)
                    {
                        sJob = dynamic_pointer_cast<SortJob, Job>(_stoppedJobs[i]);
                        if (sJob.get() && sJob->complete())
                        {
                            _partitionComplete[i] = true;
                        }

                        if (_partitionComplete[i])
                        {
                            // partition is complete, schedule the merge if necessary
                            if (_results.size() > _nStreams)
                            {
                                nextJob = JobMerge;
                            }
                        }
                        else
                        {
                            // partition is not complete, schedule the sort if we can,
                            // or the merge if we must
                            if (_results.size() < _pipelineLimit)
                            {
                                nextJob = JobSort;
                            }
                            else
                            {
                                nextJob = JobMerge;
                            }
                        }

                        if (nextJob == JobSort)
                        {
                            _waitingJobs[i] = make_shared<SortJob>(this,
                                                              query, phyOp,
                                                              i,
                                                              _sortIterators[i].get());
                        }
                        else if (nextJob == JobMerge)
                        {
                            _waitingJobs[i] = make_shared<MergeJob>(this,query,phyOp,i);
                        }
                    }
                    else
                    {
                        // An error occurred.  Save this job so we can re-throw the exception
                        // when the rest of the jobs have stopped.
                        if (!jobSuccess)
                        {
                            _failedJob = _stoppedJobs[i];
                        }
                    }
                    _stoppedJobs[i].reset();
                    -- _nStoppedJobs;
                }
            }
        }

        // If there is a failed job, we need to just get out of here
        if (_failedJob)
        {
            _failedJob->rethrow();
        }

        // If there were no failed jobs, we still may need one last merge
        if (_results.size() > 1)
        {
            std::shared_ptr<Job> lastJob(make_shared<MergeJob>(this,query,phyOp,0));
            queue->pushJob(lastJob);
            lastJob->wait(true);
        }

        timing.logTiming(logger, "[SortArray] merge sorted chunks complete");

        // Return the result
        if (_results.empty())
        {
            _results.push_back(make_shared<MemArray>(_outputSchemaExpanded, query));
        }

        std::shared_ptr<MemArray> ret = dynamic_pointer_cast<MemArray, Array> (_results.front());
        _results.clear();
        return ret;
    }

}  // namespace scidb