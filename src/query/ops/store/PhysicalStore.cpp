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


#include <log4cxx/logger.h>

#include <array/Array.h>
#include <array/NewDBArray.h>
#include <array/DBArray.h>
#include <array/DelegateArray.h>
#include <array/Metadata.h>
#include <array/ParallelAccumulatorArray.h>
#include <array/TransientCache.h>
#include <query/PhysicalUpdate.h>
#include <query/QueryProcessor.h>
#include <query/TypeSystem.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physical_store"));

// main operator

class PhysicalStore: public PhysicalUpdate
{
public:
    /// @see PhysicalOperator::PhysicalOperator()
    PhysicalStore(const string& logicalName, const string& physicalName,
                  const Parameters& parameters, const ArrayDesc& schema);

    /// @see PhysicalOperator::getDistributionRquirement()
    DistributionRequirement getDistributionRequirement(const std::vector< ArrayDesc> & inputSchemas) const override;

    /// @see PhysicalOperator::getOutputBoundaries()
    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas) const override
    { return inputBoundaries.front(); }

    /// @see PhysicalOperator::acceptsPullSG()
    virtual bool acceptsPullSG(size_t input) const { return true; }

    /// @see PhysicalOperator::requiresRedimensionOrPartition()
    void requiresRedimensionOrRepartition(vector<ArrayDesc> const& inputSchemas,
                                          vector<ArrayDesc const*>& modifiedPtrs) const override
    { repartForStoreOrInsert(inputSchemas, modifiedPtrs); }

    /// @see PhysicalOperator::getOutputDistribution()
    RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                              std::vector<ArrayDesc> const& inputSchemas) const override;

    /// @see PhysicalOperator::isSingleThreaded()
    bool isSingleThreaded() const override { return false; }

    // preSingleExecute


    /// @see PhysicalOperator::execute()
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override;
private:

    // TODO: consider other names prefixed by execute
    static PhysicalBoundaries copyChunks(const std::shared_ptr<Array>& dstArray,
                                         const std::shared_ptr<Array>& srcArray,
                                         size_t nDims, size_t nAttrs,    // TODO: don't pass obtained from srcArray?
                                         std::shared_ptr<Query> query,
                                         std::set<Coordinates, CoordinatesLess>& createdChunksOut) ;
};

//
// begin implementation
//

// ctor helper
static string getArrayName(const Parameters& parameters) {
    SCIDB_ASSERT(!parameters.empty());
    return ((std::shared_ptr<OperatorParamReference>&)parameters[0])->getObjectName();
}

// ctor
PhysicalStore::PhysicalStore(const string& logicalName, const string& physicalName,
                             const Parameters& parameters, const ArrayDesc& schema)
:
    PhysicalUpdate(logicalName, physicalName, parameters, schema,
                   getArrayName(parameters))
{}

// getDistributionRequirement
DistributionRequirement
PhysicalStore::getDistributionRequirement(const std::vector< ArrayDesc> & inputSchemas) const
{
        ArrayDistPtr arrDist = _schema.getDistribution();
        SCIDB_ASSERT(arrDist);
        SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUninitialized);
        SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUndefined);

        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);
        SCIDB_ASSERT(_schema.getResidency());
        SCIDB_ASSERT(_schema.getResidency()->size() > 0);

        //XXX TODO: for now just check that all the instances in the residency are alive
        //XXX TODO: once we allow writes in a degraded mode, this call might have more semantics
        query->isDistributionDegradedForWrite(_schema);

        // make sure PhysicalStore informs the optimizer about the actual array residency
        ArrayResPtr arrRes = _schema.getResidency();
        SCIDB_ASSERT(arrRes);

        RedistributeContext distro(arrDist, arrRes);
        LOG4CXX_TRACE(logger, "store() input req distro: "<< distro);

        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder,
                                       vector<RedistributeContext>(1, distro));
}

// getPartitioningSchema not overridden

/// @see PhysicalOperator::getOutputDistribution()
RedistributeContext
PhysicalStore::getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                     std::vector<ArrayDesc> const& inputSchemas) const
{
        RedistributeContext distro(_schema.getDistribution(),
                                   _schema.getResidency());
        LOG4CXX_TRACE(logger, "store() output distro: "<< distro);
        return distro;
}

// getOutuputBoundaries not overridden


// requiresRedimensionOrRepartition not overridden

// preSingleExecute not overridden

// execute
std::shared_ptr<Array>
PhysicalStore::execute(vector< std::shared_ptr<Array> >& inputArrays,
                       std::shared_ptr<Query> query)
{
        SCIDB_ASSERT(inputArrays.size() == 1);
        executionPreamble(inputArrays[0], query);

        VersionID version = _schema.getVersionId();

        //
        // Store uses a struct here and insert does not
        // that makes the code more different than necessary
        // and harder to see if there is a bug in one vs the other
        //
        typedef struct {
            std::string arrayNameOrg;
            std::string namespaceName;
            std::string arrayName;
            std::string unvArrayName;
        } ARRAY_NAME_COMPONENTS;
        ARRAY_NAME_COMPONENTS schemaVars, paramVars;

        // why is public.A1@1 ?
        schemaVars.arrayNameOrg = _schema.getQualifiedArrayName();
        query->getNamespaceArrayNames(
            schemaVars.arrayNameOrg, schemaVars.namespaceName, schemaVars.arrayName);
        schemaVars.unvArrayName = ArrayDesc::makeUnversionedName(schemaVars.arrayName);

        paramVars.arrayNameOrg = getArrayName(_parameters);
        query->getNamespaceArrayNames(
            paramVars.arrayNameOrg, paramVars.namespaceName, paramVars.arrayName);
        paramVars.unvArrayName = ArrayDesc::makeUnversionedName(paramVars.arrayName);

        SCIDB_ASSERT(version == ArrayDesc::getVersionFromName(schemaVars.arrayName));
        SCIDB_ASSERT(paramVars.unvArrayName == schemaVars.unvArrayName);

        //
        // pre lock transient case
        //
        if (_schema.isTransient()) {
            // no pre-lock transient case for insert
        }

        //
        // acquire locks (nearly identical to insert)
        //
        if (!_lock) {
            SCIDB_ASSERT(!query->isCoordinator());
            const LockDesc::LockMode lockMode =
                _schema.isTransient() ? LockDesc::XCL : LockDesc::WR;

             std::string namespaceName = _schema.getNamespaceName();
            _lock = std::shared_ptr<LockDesc>(
                make_shared<LockDesc>(
                    schemaVars.namespaceName,
                    schemaVars.unvArrayName,
                    query->getQueryID(),
                    Cluster::getInstance()->getLocalInstanceId(),
                    LockDesc::WORKER,
                    lockMode));

            if (lockMode == LockDesc::WR) {
                SCIDB_ASSERT(!_schema.isTransient());
                _lock->setArrayVersion(version);
                std::shared_ptr<Query::ErrorHandler> ptr(make_shared<UpdateErrorHandler>(_lock));
                query->pushErrorHandler(ptr);
            }

           Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock,_lock,_1);
           query->pushFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker(bind(&Query::validate, query));
           if (!SystemCatalog::getInstance()->lockArray(_lock, errorChecker)) {
               throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK) << _lock->toString();
           }
           SCIDB_ASSERT(_lock->getLockMode() == lockMode);
           LOG4CXX_DEBUG(logger, __func__ << " output array lock acquired");
        }

        //
        // post lock transient case
        //

        if (_schema.isTransient()) {
            SCIDB_ASSERT(_lock->getLockMode() == LockDesc::XCL);

            MemArrayPtr        outArray(make_shared<MemArray>(_schema,query)); // materialized copy
            PhysicalBoundaries bounds(PhysicalBoundaries::createEmpty(_schema.getDimensions().size()));

            const size_t nAttrs = outArray->getArrayDesc().getAttributes().size();
            std::shared_ptr<Array> srcArray(inputArrays[0]);
            ArrayDesc const& srcArrayDesc(srcArray->getArrayDesc());
            // Cater to the case where the input array does not contain the coordinates
            // list. This happens as the result of an aggregate operator, for
            // example. See- Ticket SDB-5490. Use the same code from below (in the the
            // non-Transient Case) to add the coordinates list.
            // The "Right Thing To Do (tm)" is to eradicate "non-empty-able" arrays altogether.
            if (nAttrs > srcArrayDesc.getAttributes().size()) {
                SCIDB_ASSERT(nAttrs == srcArrayDesc.getAttributes().size() + 1);
                srcArray = std::shared_ptr<Array>(make_shared<NonEmptyableArray>(inputArrays[0]));
            }

            //
            // DELEGATE APPEND  -- transient case only
            //
            LOG4CXX_DEBUG(logger, __func__ << " transient case, using array->appendHorizontal()");
            outArray->appendHorizontal(srcArray);          // ...materialize it

            /* Run back over the chunks one more time to compute the physical bounds
            of the array...*/

            LOG4CXX_DEBUG(logger, __func__ << " transient case, using bounds.updateFromChunk() on outArray chunks");
            for (std::shared_ptr<ConstArrayIterator> i(outArray->getConstIterator(0)); !i->end(); ++(*i))
            {
                bounds.updateFromChunk(&i->getChunk());       // ...update bounds
            }

            LOG4CXX_DEBUG(logger, __func__ << " transient case, updating schema boundaries");
            updateSchemaBoundaries(_schema, bounds, query);
            query->pushFinalizer(boost::bind(&PhysicalUpdate::recordTransient, this, outArray,_1));
            getInjectedErrorListener().throwif(__LINE__, __FILE__);


            // EARLY RETURN
            return outArray;                             // ...return the copy
        } // end transient case

        //////////////////////////////////////////////////////////////////////
        //
        // NOT TRANSIENT CASE
        //
        //  seems like there is probably a lot of code duplication vs above
        //

        std::shared_ptr<Array>  srcArray    (inputArrays[0]);
        ArrayDesc const&   srcArrayDesc(srcArray->getArrayDesc());
        std::shared_ptr<Array>  dstArray    (NewDBArray::createDBArray(_schema, query));
        ArrayDesc const&   dstArrayDesc(dstArray->getArrayDesc());
        std::string descArrayName = dstArrayDesc.getName();
        std::string descNamespaceName = dstArrayDesc.getNamespaceName();
        SCIDB_ASSERT(dstArrayDesc == _schema);

        // TODO: needs commenting as to what this does and why here
        query->getReplicationContext()->enableInboundQueue(dstArrayDesc.getId(), dstArray);

        const size_t nAttrs = dstArrayDesc.getAttributes().size();

        if (nAttrs == 0) {
            return dstArray;        // EARLY RETURN
        }

        // See comment for similar code agove for transient case, SDB-5490.
        if (nAttrs > srcArrayDesc.getAttributes().size()) {
            assert(nAttrs == srcArrayDesc.getAttributes().size()+1);
            srcArray = std::shared_ptr<Array>(make_shared<NonEmptyableArray>(srcArray));
        }

        Dimensions const& dims = dstArrayDesc.getDimensions();
        set<Coordinates, CoordinatesLess> createdChunkCoords;   // return values

        LOG4CXX_DEBUG(logger, __func__ << " non-transient case, using copyChunks");
        PhysicalBoundaries bounds = copyChunks(dstArray, srcArray, dims.size(), nAttrs, query, createdChunkCoords);

        // Insert tombstone entries
        LOG4CXX_DEBUG(logger, __func__ << " non-transient case, converting dead chunks to tombstones");
        dstArray->removeDeadChunks(query, createdChunkCoords);

        // Update boundaries
        LOG4CXX_DEBUG(logger, __func__ << " non-transient case, updating schema boundaries");
        updateSchemaBoundaries(_schema, bounds, query);

        // Sync replication
        LOG4CXX_DEBUG(logger, __func__ << " non-transient case, waiting for replication");
        query->getReplicationContext()->replicationSync(dstArrayDesc.getId());
        query->getReplicationContext()->removeInboundQueue(dstArrayDesc.getId());

        // Flush data and metadata for the array
        dstArray->flush();
        getInjectedErrorListener().throwif(__LINE__, __FILE__);
        return dstArray;
}
//
// end execute
//


//
// was StoreJob::chunkHasValues in prior versions
//
// XXX TODO: until we have the single RLE? data format,
//           we need to filter out other depricated formats (e.g. dense/sparse/nonempyable)
static bool chunkHasValues(const std::shared_ptr<Array>& srcArray, ConstChunk const& srcChunk)
{
    bool isSrcEmptyable = (srcArray->getArrayDesc().getEmptyBitmapAttribute() != NULL);
    bool chunkHasVals = (!isSrcEmptyable) || (!srcChunk.isEmpty());
    return chunkHasVals;
}

//
// was StoreJob::run() in prior versions
//
PhysicalBoundaries
PhysicalStore::copyChunks(const std::shared_ptr<Array>& dstArray,
                          const std::shared_ptr<Array>& srcArray,
                          size_t nDims, size_t nAttrs,    // why are these passed, instead of obtained from srcArray?
                          std::shared_ptr<Query> query,
                          std::set<Coordinates, CoordinatesLess>& createdChunksOut)
{
    // from StoreJob::StoreJob in prior versions
    ASSERT_EXCEPTION(nAttrs <= std::numeric_limits<AttributeID>::max(),
                     "Attribute count exceeds max addressable iterator");

    PhysicalBoundaries bounds(PhysicalBoundaries::createEmpty(nDims));

    std::vector<std::shared_ptr<ArrayIterator> >      dstArrayIterators(nAttrs);
    std::vector<std::shared_ptr<ConstArrayIterator> > srcArrayIterators(nAttrs);
    for (size_t i = 0; i < nAttrs; i++) {
        dstArrayIterators[i] = dstArray->getIterator(static_cast<AttributeID>(i));
        srcArrayIterators[i] = srcArray->getConstIterator(static_cast<AttributeID>(i));
    }
    // end from StoreJob::StoreJob

    ArrayDesc const& dstArrayDesc = dstArray->getArrayDesc();

    //NOCHECKIN, see comment about why passed above ...
    // size_t nAttrs = dstArrayDesc.getAttributes().size();

    while (!srcArrayIterators[0]->end()) {
        bool chunkHasElems(true);
        for (size_t i = 0; i < nAttrs; i++) {
            ConstChunk const& srcChunk = srcArrayIterators[i]->getChunk();
            Coordinates srcPos         = srcArrayIterators[i]->getPosition();
            if (!dstArrayDesc.contains(srcPos)) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
                    << CoordsToStr(srcPos) << dstArrayDesc.getDimensions();
            }
            //
            //   Only insert the logical chunk boundaries into the list of
            //  created chunks once ...
            if (i==0) {
                chunkHasElems = chunkHasValues(srcArray, srcChunk);
                if (chunkHasElems) {
                    createdChunksOut.insert(srcPos);
                }
            } else {
                SCIDB_ASSERT(chunkHasElems == chunkHasValues(srcArray, srcChunk));
            }
            if (chunkHasElems) {
                Coordinates chunkStart(srcPos.size());
                Coordinates chunkEnd(srcPos.size());
                //
                //  If this is ...
                //  i. The last time we're going through a logical chunk's
                //     cells (that is, if we're dealing with the coordinates
                //     list / isEmpty attribute) and ...
                // ii. ... this logical chunk's boundaries are outside of the
                //     bounds we've gathered about what parts of the array have
                //     been seen so far ...
                //
                //  .. then track details about this chunk's MBR and use this
                // information to track the boundaries of the array being
                // stored, otherwise, just copy the chunk as best we can.
                bool isInside = bounds.isInsideBox(srcChunk.getFirstPosition(false)) &&
                                bounds.isInsideBox(srcChunk.getLastPosition(false));

                if (( i == nAttrs - 1 ) && (! isInside )) {
                    dstArrayIterators[i]->copyChunk( srcChunk, &chunkStart, &chunkEnd );
                    bounds = bounds.unionWith( PhysicalBoundaries ( chunkStart, chunkEnd ) );
                } else {
                    dstArrayIterators[i]->copyChunk(srcChunk);
                }
            }
            Query::validateQueryPtr(query);

            //
            // advance to next chunk
            //
            if (!srcArrayIterators[i]->end()) {
                ++(*srcArrayIterators[i]);
            }
        } // for
    } // while

    return bounds;
}


DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalStore, "store", "physicalStore")

}  // namespace ops
