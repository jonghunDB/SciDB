/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2018 SciDB, Inc.
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
#include <network/OrderedBcast.h>

#include <sys/types.h>
#include <signal.h>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <set>
#include <memory>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

#include <network/Connection.h>
#include <network/MessageHandleJob.h>
#include <network/MessageUtils.h>
#include <network/proto/scidb_msg.pb.h>
#include <system/Constants.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/Notification.h>
#include <util/Network.h>

using namespace std;

namespace scidb
{
// ASSERT_EXCEPTION is not safe to use in the code running on a dedicated queue.
// The exception is just swallowed in a release build by Job::run()
// Consider something like:
//   if (!cond) { assert(false); throw std::runtime_error("what"); }
// to cause an abort in a release build.

#define OBCASTMSG "OBCAST " << __FUNCTION__ << ':' << __LINE__<< ' '

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network.obcast"));

namespace messageUtils {

template<class Appendable>
bool parseLivenessVersions(Appendable& container,
                           const scidb_msg::LivenessVector_Versions& versions)
{
    namespace gpb = google::protobuf;
    const gpb::RepeatedPtrField<scidb_msg::LivenessVector_VersionEntry>& vEntries =
            versions.version_entry();
    for(gpb::RepeatedPtrField<scidb_msg::LivenessVector_VersionEntry>::const_iterator instanceIter =
                vEntries.begin();
        instanceIter != vEntries.end(); ++instanceIter) {

        const scidb_msg::LivenessVector_VersionEntry& entry = (*instanceIter);
        if(!entry.has_instance_id()) {
            SCIDB_ASSERT(false);
            return false;
        }

        if(!entry.has_version()) {
            SCIDB_ASSERT(false);
            return false;
        }

        container.insert(container.end(),
                         typename Appendable::value_type(entry.instance_id(), entry.version()));
    }
    return true;
}

template<typename Appendable>
bool parseLivenessVector(Appendable& container,
                         const scidb_msg::LivenessVector& vector)

{
    if (!vector.has_cluster_uuid() ||
        vector.cluster_uuid() != Cluster::getInstance()->getUuid()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "unable to parse liveness due to invalid cluster id");
        SCIDB_ASSERT(false);
        return false;
    }

    if (!vector.has_versions()) {
        SCIDB_ASSERT(false);
        return false;
    }

    return parseLivenessVersions(container, vector.versions());
}

template<typename Iterator>
bool serializeLivenessVector(const Iterator begin, const Iterator end,
                             scidb_msg::LivenessVector* vvMsg)
{
    SCIDB_ASSERT(vvMsg);

    vvMsg->set_cluster_uuid(Cluster::getInstance()->getUuid());

    scidb_msg::LivenessVector_Versions* verVector = vvMsg->mutable_versions();
    SCIDB_ASSERT(verVector);

    for (Iterator iter = begin ; iter != end; ++iter) {

        google::protobuf::uint64 iId = iter->first;
        google::protobuf::uint64 version = iter->second;
        scidb_msg::LivenessVector_VersionEntry* versionEntry = verVector->add_version_entry();
        SCIDB_ASSERT(versionEntry);
        versionEntry->set_instance_id(iId);
        versionEntry->set_version(version);
    }
    return true;
}

} //namespace

LivenessTracker::LivenessTracker()
: _selfId(Cluster::getInstance()->getLocalInstanceId()),
  _isInSync(false),
  _nm(NetworkManager::getInstance())
{
    SCIDB_ASSERT(isValidPhysicalInstance(_selfId));
}

MessagePtr LivenessTracker::createLiveness(MessageID msgId)
{
    SCIDB_ASSERT(msgId == mtLiveness);
    return scidb::MessagePtr(new scidb_msg::Liveness());
}

MessagePtr LivenessTracker::createLivenessAck(MessageID msgId)
{
    SCIDB_ASSERT(msgId == mtLivenessAck);
    return scidb::MessagePtr(new scidb_msg::LivenessAck());
}

/// New local or remote liveness handler
/// @return false if the liveness is ignored; true otherwise
bool LivenessTracker::newLiveness(InstanceID iId, const InstLivenessPtr& liveInfo)
{
    LOG4CXX_TRACE(logger, OBCASTMSG << "liveness from = "<< Iid(iId)
                  << " ver="<<liveInfo->getVersion() );

    LivenessVector::const_iterator selfIter = _liveVector.find(_selfId);

    if (iId == _selfId) {

        if (selfIter != _liveVector.end() &&
            selfIter->second->getVersion() >= liveInfo->getVersion()) {
            // Because we check for new liveness on every message, we may know
            // about the latest liveness long before the liveness notifications reach us.
            return false;
        }
        _isInSync = false;
        _versionVector.clear();
        _liveVector.clear();
        LOG4CXX_DEBUG(logger, OBCASTMSG << "NO SYNC" );
    }

    InstLivenessPtr& currentLiveness = _liveVector[iId]; //always insert

    if (currentLiveness == nullptr ||
        currentLiveness->getVersion() < liveInfo->getVersion()) {
        currentLiveness = liveInfo;
        checkInSync();
    } else if (liveInfo->getVersion() == 0) {
        // NOTE: initial liveness version is always 0
        // Locally, we must not get it AGAIN as a notification
        SCIDB_ASSERT(iId != _selfId);
        LOG4CXX_DEBUG(logger, OBCASTMSG << "dropping liveness from = "<< Iid(iId)
                      << " ver="<<liveInfo->getVersion() );
        return false;
    } else {
        SCIDB_ASSERT(iId != _selfId);
        // assuming instance-to-instance FIFO & increasing liveness version number
        if (currentLiveness->getVersion() != liveInfo->getVersion()) {
            LOG4CXX_ERROR(logger, OBCASTMSG << "dropping INVALID liveness from = "<< Iid(iId)
                         << " ver="<<liveInfo->getVersion()
                         << " current ver=" << currentLiveness->getVersion());
            SCIDB_ASSERT(false);
            return false;
        }
    }

    if (iId == _selfId)
    {
        std::shared_ptr<MessageDesc> msg = std::make_shared<Connection::ServerMessageDesc>();
        msg->initRecord(mtLiveness);
        std::shared_ptr<scidb_msg::Liveness> record = msg->getRecord<scidb_msg::Liveness>();
        bool res = serializeLiveness(liveInfo, record.get());
        SCIDB_ASSERT(res);
        const InstanceLiveness::LiveInstances& liveSet = liveInfo->getLiveInstances();
        try {
            _nm->multicastPhysical(liveSet.begin(), liveSet.end(), *this, msg);
        } catch (const scidb::Exception& e) {
            // XXX only the overflow & memory errors are expected to bubble up
            // XXX overflow is reported as query error by message dispatch
            // XXX memory errors are still an issue
            LOG4CXX_ERROR(logger, "FAILED to multicast mtLiveness because: " << e.what());
            SCIDB_ASSERT(e.getLongErrorCode() != scidb::SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
            throw std::runtime_error(e.what());
        }
    }
    else if (selfIter != _liveVector.end())
    {
        std::shared_ptr<scidb_msg::LivenessAck> record =
                dynamic_pointer_cast<scidb_msg::LivenessAck>(createLivenessAck(mtLivenessAck));
        record->set_request_version(liveInfo->getVersion());

        scidb_msg::Liveness* liveRec = record->mutable_liveness();
        SCIDB_ASSERT(liveRec);
        bool res = serializeLiveness(selfIter->second, liveRec);
        SCIDB_ASSERT(res);

        boost::asio::const_buffer binary(NULL,0);
        scidb::MessagePtr msgPtr(record);
        try {
            scidb::sendAsyncPhysical(iId, mtLivenessAck, msgPtr, binary);
            LOG4CXX_DEBUG(logger, "mtLivenessAck sent to instance: "<< Iid(iId) );
        } catch ( const scidb::Exception& e) {
            // XXX only the overflow & memory errors are expected to bubble up
            // XXX overflow is reported as query error by message dispatch
            // XXX memory errors are still an issue
            LOG4CXX_ERROR(logger, "FAILED to send mtLivenessAck sent to instance: "<< Iid(iId)
                          << " because: " << e.what());
            SCIDB_ASSERT(e.getLongErrorCode() != scidb::SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
            throw std::runtime_error(e.what());
        }
    } // else the local liveness will be broadcast when it is available
    return true;
}

/// reply to our liveness message
bool LivenessTracker::newLivenessAck(InstanceID remoteId,
                                     uint64_t myLivenessVer,
                                     const InstLivenessPtr& remoteLiveInfo)
{
    LOG4CXX_DEBUG(logger, OBCASTMSG
                  << "livenessACK from = "<< Iid(remoteId)
                  << " ver="<<remoteLiveInfo->getVersion()
                  << " to my_ver="<<myLivenessVer);
    SCIDB_ASSERT(remoteId != _selfId);

    LivenessVector::const_iterator selfIter = _liveVector.find(_selfId);
    SCIDB_ASSERT(selfIter != _liveVector.end());

    const InstLivenessPtr& myLiveness = selfIter->second;

    if (myLiveness->getVersion() > myLivenessVer) {
        LOG4CXX_DEBUG(logger, OBCASTMSG
                      << "dropping livenessACK from = "<< Iid(remoteId)
                      << " to my_ver="<<myLivenessVer
                      << " my_curr_ver="<<myLiveness->getVersion());
        return false;
    }

    if (myLiveness->getVersion() < myLivenessVer) {
        LOG4CXX_ERROR(logger, OBCASTMSG
                      << "dropping livenessACK from = "<< Iid(remoteId)
                      << " to my_ver="<<myLivenessVer
                      << " my_curr_ver="<<myLiveness->getVersion());
        SCIDB_ASSERT(false);
        return false;
    }

    SCIDB_ASSERT(myLiveness->getVersion() == myLivenessVer);

    InstLivenessPtr& remoteLiveness = _liveVector[remoteId] ;

    if (remoteLiveness == nullptr ||
        remoteLiveness->getVersion() < remoteLiveInfo->getVersion()) {
        remoteLiveness = remoteLiveInfo;
        checkInSync();
    } else {
        SCIDB_ASSERT(remoteLiveness->getVersion() == remoteLiveInfo->getVersion());
        return false;
    }
    return true;
}

/// Given a version vector (presumably of a message) it determines
/// whether the message can be immediately acted upon (i.e. delivered),
/// discarded (because it was sent in an older view), or deferred
/// (because this instance has not learned about the new view
/// to which the version vector corresponds to).
LivenessTracker::MsgDeliveryAction
LivenessTracker::getMsgDeliveryAction(const VersionVector& vv)
{
    bool skip = false;
    bool newer = false;
    bool older = false;

    // If we had a reliable (i.e. non-lossy) FIFO channel,
    // we would not need to check every message's vector,
    // but we dont (because a TCP connection can be broken).

    if (_versionVector.empty()) {
        SCIDB_ASSERT(!isInSync());
        // we have not formed the vector yet
        return DEFER;
    }

    bool hasDefaultVer = false;
    if (isDebug()) {
        VersionVector::const_iterator iv = vv.begin();
        for (; iv != vv.end() ; ++iv) {
            if (iv->second == DEFAULT_LIVENESS_VER) { break; }
        }
        hasDefaultVer = (iv != vv.end());
    }

    VersionVector::const_iterator viter = vv.begin();
    for (VersionVector::const_iterator iter = _versionVector.begin();
         iter != _versionVector.end() && viter != vv.end(); ) {

        const InstanceID localInst = iter->first;
        const InstanceID otherInst = viter->first;
        const uint64_t localVer = iter->second;
        const uint64_t otherVer = viter->second;

        if (localInst == otherInst) {
            if (localVer > otherVer) {
                older = true;
                SCIDB_ASSERT(!newer ||  otherVer == DEFAULT_LIVENESS_VER);
            } else if (localVer < otherVer) {
                newer = true;
                SCIDB_ASSERT(!older || hasDefaultVer);
            }
            ++iter;
            ++viter;
        } else if (localInst < otherInst) {
            skip = true;
            ++iter;
        } else { //if (localInst > otherInst)
            skip = true;
            ++viter;
        }
    }

    if (vv.size() != _versionVector.size()) {
        skip = true;
    }

    if (!skip && !newer && !older && isInSync()) {
        return DELIVER;
    }
    if (older) {
        return DISCARD;
    }
    return DEFER;
}

void LivenessTracker::checkInSync()
{
    _isInSync = false;
    _versionVector.clear();
    LOG4CXX_DEBUG(logger, OBCASTMSG << "NO SYNC");

    LivenessVector::const_iterator iter = _liveVector.find(_selfId);
    if (iter == _liveVector.end()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "local liveness is still unknown");
        return;
    }

    const InstLivenessPtr& myLiveness = iter->second;

    if (myLiveness->getNumLive() != _liveVector.size()) {
        return;
    }

    for (iter = _liveVector.begin();
         iter != _liveVector.end(); ++iter) {
        InstanceID nextId = iter->first;

        if (_selfId == nextId) { continue; }

        const InstLivenessPtr& nextLiveness = iter->second;
        SCIDB_ASSERT(nextLiveness != nullptr);
        SCIDB_ASSERT(nextLiveness->find(nextId) != NULL);
        SCIDB_ASSERT(!nextLiveness->isDead(nextId));
        // compare to myLiveness
        if (!myLiveness->isEqual(*nextLiveness)) {
            return;
        } else {
          //XXX TODO: mark this liveness as checked and dont compare it again
        }
    }

    for (iter = _liveVector.begin();
         iter != _liveVector.end(); ++iter) {
        const InstanceID nextId = iter->first;
        const InstLivenessPtr& nextLiveness = iter->second;
        // _versionVector needs to be ordered
        _versionVector.insert(VersionVector::value_type(nextId,
                                                        nextLiveness->getVersion()));
    }
    _isInSync = true;
    LOG4CXX_DEBUG(logger, OBCASTMSG << "IN SYNC vVec=" << _versionVector);
}

OrderedBcastManager::OrderedBcastManager()
    : _ts(0)
    , _lastBroadcastTs(0)
    , _nm(NetworkManager::getInstance())
    , _cluster(Cluster::getInstance())
    , _selfInstanceId(_cluster->getLocalInstanceId())
    , _livenessSubscriberID(0)
{
    SCIDB_ASSERT(_nm != NULL);
    SCIDB_ASSERT(_cluster != NULL);
    _wq = _nm->createWorkQueue("OrderedBcastWorkQueue");
    _wq->stop();
}

OrderedBcastManager::~OrderedBcastManager()
{
    if (_livenessSubscriberID) {
        Notification<InstanceLiveness>::unsubscribe(_livenessSubscriberID);
    }
}


void OrderedBcastManager::init(Timestamp ts)
{
    _ts = ts;
    _lastBroadcastTs = 0;
    _remoteTimestamps[_selfInstanceId] = _ts;

    handleLivenessNotification(_cluster->getInstanceLiveness());

    // Listen for liveness changes
    Notification<InstanceLiveness>::Subscriber listener =
            boost::bind(&OrderedBcastManager::handleLivenessNotification, this, _1);
    if (_livenessSubscriberID) {
        Notification<InstanceLiveness>::unsubscribe(_livenessSubscriberID);
    }
    _livenessSubscriberID = Notification<InstanceLiveness>::subscribe(listener); // stays until removed

    std::shared_ptr<scidb::NetworkMessageFactory> factory =
            scidb::getNetworkMessageFactory();

    // Register our message types
    NetworkMessageFactory::MessageCreator msgCreator =
            boost::bind(&OrderedBcastManager::createRequest, this,_1);
    NetworkMessageFactory::MessageHandler msgHandler =
            boost::bind(&OrderedBcastManager::handleRequest, this,_1);
    MessageID msgID = mtOBcastRequest;
    factory->addMessageType(msgID, msgCreator, msgHandler);

    msgCreator = boost::bind(&OrderedBcastManager::createReply, this,_1);
    msgHandler = boost::bind(&OrderedBcastManager::handleReply, this,_1);
    msgID = mtOBcastReply;
    factory->addMessageType(msgID, msgCreator, msgHandler);

    msgCreator = boost::bind(&LivenessTracker::createLiveness, &_livenessTracker,_1);
    msgHandler = boost::bind(&OrderedBcastManager::handleLiveness, this,_1);
    msgID = mtLiveness;
    factory->addMessageType(msgID, msgCreator, msgHandler);

    msgCreator = boost::bind(&LivenessTracker::createLivenessAck, &_livenessTracker,_1);
    msgHandler = boost::bind(&OrderedBcastManager::handleLivenessAck, this,_1);
    msgID = mtLivenessAck;
    factory->addMessageType(msgID, msgCreator, msgHandler);

    _wq->start(); // up to this point the callbacks just accumulate in the queue
}

void OrderedBcastManager::broadcast(const std::shared_ptr<MessageDesc>& messageDesc)
{
    WorkQueue::WorkItem work =
            boost::bind(&OrderedBcastManager::localRequest, this, messageDesc);
    enqueue(work);
}

scidb::MessagePtr OrderedBcastManager::createRequest(scidb::MessageID msgId)
{
    SCIDB_ASSERT(msgId == mtOBcastRequest);
    return scidb::MessagePtr(new scidb_msg::OrderedBcastRequest());
}

void OrderedBcastManager::handleRequest(const std::shared_ptr<MessageDescription>& obcastMsg)
{
    const InstanceID sourceInstanceId = obcastMsg->getSourceInstanceID();
    if (sourceInstanceId == _selfInstanceId ||
        !isValidPhysicalInstance(sourceInstanceId)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "selfInstanceID==sourceInstanceID");
        SCIDB_ASSERT(false);
        return;
    }

    std::shared_ptr<scidb_msg::OrderedBcastRequest> repPtr =
            dynamic_pointer_cast<scidb_msg::OrderedBcastRequest>(obcastMsg->getRecord());

    if (!repPtr ||
        !repPtr->IsInitialized()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    if (!repPtr->has_timestamp() ||
        !repPtr->has_payload_message_type() ||
        !repPtr->has_payload_message() ||
        !repPtr->has_vector()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));

        SCIDB_ASSERT(false);
        return;
    }

    const uint64_t payloadMsgType = repPtr->payload_message_type();
    if (payloadMsgType > std::numeric_limits<scidb::MessageID>::max()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId)
                     << ", payloadMsgType=" << payloadMsgType << " out of range");
        SCIDB_ASSERT(false);
        return;
    }

    const std::string& payloadMessage = repPtr->payload_message();

    // re-create the original message from the obcast message
    std::shared_ptr<SharedBuffer> binary = obcastMsg->getMutableBinary();
    std::shared_ptr<MessageDesc> messageDesc =
            std::make_shared<Connection::ServerMessageDesc>(binary);

    messageDesc->setSourceInstanceID(obcastMsg->getSourceInstanceID());
    messageDesc->setQueryID(obcastMsg->getQueryId());

    messageDesc->initRecord(static_cast<const scidb::MessageID>(payloadMsgType));
    SCIDB_ASSERT(uint64_t(messageDesc->getMessageType()) == payloadMsgType);

    if (!messageDesc->getRecord()->ParseFromString(payloadMessage)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId)
                     << " sent unparseable payload message");
        SCIDB_ASSERT(false);
        return;
    }

    LivenessTracker::VersionVector vVec;
    const scidb_msg::LivenessVector& vector = repPtr->vector();
    if (!messageUtils::parseLivenessVector(vVec, vector)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    if (vVec.count(sourceInstanceId) <= 0) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId)
                     << " not in request version vector");
        SCIDB_ASSERT(false);
        return;
    }

    const Timestamp reqTs = repPtr->timestamp();

    LOG4CXX_TRACE(logger, OBCASTMSG << "remote request reqTs="<< reqTs
                  << " from=" << Iid(sourceInstanceId));

    WorkQueue::WorkItem work = boost::bind(&OrderedBcastManager::remoteRequest, this,
                                           messageDesc, sourceInstanceId, reqTs, vVec); //XXXX copy
    enqueue(work);
}

scidb::MessagePtr OrderedBcastManager::createReply(scidb::MessageID msgId)
{
    SCIDB_ASSERT(msgId == mtOBcastReply);
    return scidb::MessagePtr(new scidb_msg::OrderedBcastReply());
}

void OrderedBcastManager::handleReply(const std::shared_ptr<MessageDescription>& obcastMsg)
{
    const InstanceID sourceInstanceId = obcastMsg->getSourceInstanceID();
    if (sourceInstanceId == _selfInstanceId ||
        !isValidPhysicalInstance(sourceInstanceId)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "selfInstanceID==sourceInstanceID");
        SCIDB_ASSERT(false);
        return;
    }

    std::shared_ptr<scidb_msg::OrderedBcastReply> repPtr =
            dynamic_pointer_cast<scidb_msg::OrderedBcastReply>(obcastMsg->getRecord());

    if (!repPtr ||
        !repPtr->IsInitialized()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    if (!repPtr->has_request_timestamp() ||
        !repPtr->has_timestamp() ||
        !repPtr->has_vector()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    const Timestamp requestTs = repPtr->request_timestamp();
    const InstanceID requestSrc = repPtr->request_instance();
    const Timestamp replyTs = repPtr->timestamp();

    LivenessTracker::VersionVector vVec;
    const scidb_msg::LivenessVector& vector = repPtr->vector();
    if (!messageUtils::parseLivenessVector(vVec, vector)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    if (vVec.count(sourceInstanceId) <= 0) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId)
                     << " not in reply version vector");
        SCIDB_ASSERT(false);
        return;
    }

    if (vVec.count(requestSrc) <= 0) {
        LOG4CXX_WARN(logger, OBCASTMSG << "request instanceID=" << Iid(requestSrc)
                     << " not in reply version vector");
        SCIDB_ASSERT(false);
        return;
    }

    WorkQueue::WorkItem work = boost::bind(&OrderedBcastManager::remoteReply, this,
                                           sourceInstanceId, replyTs,
                                           requestSrc, requestTs,  vVec); //XXXX copy vVec
    enqueue(work);
}

void OrderedBcastManager::handleLivenessNotification(const InstLivenessPtr& liveInfo)
{
    SCIDB_ASSERT(liveInfo != nullptr);
    SCIDB_ASSERT(liveInfo->find(_selfInstanceId) != NULL);
    SCIDB_ASSERT(!liveInfo->isDead(_selfInstanceId));

    // make sure local requests on hold are active/broadcasted
    // discard old requests
    WorkQueue::WorkItem work = boost::bind(&OrderedBcastManager::livenessChange, this,
                                                   _selfInstanceId, liveInfo);
    enqueue(work);
}


void OrderedBcastManager::handleLiveness(const std::shared_ptr<MessageDescription>& liveMsg)
{
    // discard old requests
    const InstanceID sourceInstanceId = liveMsg->getSourceInstanceID();
    if (sourceInstanceId == _selfInstanceId ||
        !isValidPhysicalInstance(sourceInstanceId)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "selfInstanceID==sourceInstanceID");
        SCIDB_ASSERT(false);
        return;
    }

    std::shared_ptr<scidb_msg::Liveness> repPtr =
            dynamic_pointer_cast<scidb_msg::Liveness>(liveMsg->getRecord());

    if (!repPtr ||
        !repPtr->IsInitialized()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    InstLivenessPtr liveness = parseLiveness(*repPtr);
    if (!liveness ||
        liveness->find(sourceInstanceId) == NULL ||
        liveness->isDead(sourceInstanceId)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    WorkQueue::WorkItem work = boost::bind(&OrderedBcastManager::livenessChange, this,
                                           sourceInstanceId, liveness);
    enqueue(work);
}

void OrderedBcastManager::livenessChange(InstanceID iId, const InstLivenessPtr& liveInfo)
{
    if (_livenessTracker.newLiveness(iId, liveInfo)) {
        processQueueOnLivenessChange();
    }
}


void OrderedBcastManager::handleLivenessAck(const std::shared_ptr<MessageDescription>& liveAckMsg)
{
    const InstanceID sourceInstanceId = liveAckMsg->getSourceInstanceID();
    if (sourceInstanceId == _selfInstanceId ||
        !isValidPhysicalInstance(sourceInstanceId)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "selfInstanceID==sourceInstanceID");
        SCIDB_ASSERT(false);
        return;
    }

    std::shared_ptr<scidb_msg::LivenessAck> repPtr =
            dynamic_pointer_cast<scidb_msg::LivenessAck>(liveAckMsg->getRecord());

    if (!repPtr ||
        !repPtr->IsInitialized()) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    if (!repPtr->has_request_version() ||
        !repPtr->has_liveness() ) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    uint64_t myLivenessVer = repPtr->request_version();

    const scidb_msg::Liveness& liveMsg = repPtr->liveness();

    InstLivenessPtr liveness = parseLiveness(liveMsg);
    if (!liveness ||
        liveness->find(sourceInstanceId) == NULL ||
        liveness->isDead(sourceInstanceId)) {
        LOG4CXX_WARN(logger, OBCASTMSG << "instanceID=" << Iid(sourceInstanceId));
        SCIDB_ASSERT(false);
        return;
    }

    WorkQueue::WorkItem work = boost::bind(&OrderedBcastManager::livenessAck, this,
                                           sourceInstanceId, myLivenessVer, liveness);
    enqueue(work);
}

void OrderedBcastManager::livenessAck(InstanceID remoteId,
                                      uint64_t myLivenessVer,
                                      const InstLivenessPtr& remoteLiveInfo)
{
    if (_livenessTracker.newLivenessAck(remoteId, myLivenessVer, remoteLiveInfo)) {
        processQueueOnLivenessChange();
    }
}


void OrderedBcastManager::enqueue(WorkQueue::WorkItem& work)
{
    SCIDB_ASSERT(work);
    try
    {
        getFIFO()->enqueue(work);
    }
    catch (const WorkQueue::OverflowException& e)
    {
        LOG4CXX_ERROR(logger, "Overflow exception from the OBCAST message queue " << e.what());
        throw;
    }
}

void OrderedBcastManager::localRequest(const std::shared_ptr<MessageDesc>& messageDesc)
{
    // Make sure our liveness is up-to-date, i.e. our liveness is no older than
    // the liveness referred to by the query owning this message (if any).
    livenessChange(_selfInstanceId,
                   _cluster->getInstanceLiveness());

    LivenessTracker::VersionVector vVec = _livenessTracker.getVersionVector();
    LivenessTracker::MsgDeliveryAction msgState = _livenessTracker.getMsgDeliveryAction(vVec);

    SCIDB_ASSERT(msgState != LivenessTracker::DISCARD);

    messageDesc->setSourceInstanceID(_selfInstanceId);

    std::shared_ptr<RequestEntry> re =
            std::make_shared<RequestEntry>(messageDesc, vVec);
    SCIDB_ASSERT(vVec.empty());

    if (msgState == LivenessTracker::DELIVER) {
        verifyNoDeferredInDebug(_selfInstanceId);
        _ts = _ts + 1;
        LogicalTimestamp lts(_ts,_selfInstanceId);
        deliverLocalRequest(lts, re);
    } else {
        SCIDB_ASSERT(msgState == LivenessTracker::DEFER);
        SCIDB_ASSERT(!_livenessTracker.isInSync());
        SCIDB_ASSERT(re->getVersionVector().empty());
        LogicalTimestamp lts(0,_selfInstanceId);
        _deferredQueue.push_back(DeferredQueue::value_type(lts,re));
    }
}

void OrderedBcastManager::deliverLocalRequest(const LogicalTimestamp& lts,
                                              const std::shared_ptr<RequestEntry>& re)
{
    SCIDB_ASSERT(_livenessTracker.isInSync());
    SCIDB_ASSERT(!_livenessTracker.getVersionVector().empty());
    SCIDB_ASSERT(_livenessTracker.getVersionVector()==re->getVersionVector());
    SCIDB_ASSERT(re->getVersionVector().count(lts.getInstanceId()) > 0);

    std::pair<RequestQueue::iterator, bool> res = _queue.insert(RequestQueue::value_type(lts,re));
    SCIDB_ASSERT(res.second);
    SCIDB_ASSERT(res.first != _queue.end());
    SCIDB_ASSERT(++res.first == _queue.end());

    if (re->getVersionVector().size() == 1) {
        SCIDB_ASSERT( (*re->getVersionVector().begin()).first == _selfInstanceId);
        release();
    } else {
        SCIDB_ASSERT(re->getVersionVector().size()>1);
        LOG4CXX_TRACE(logger, OBCASTMSG << "deliver (broadcast) local request lts="
                      << lts << " _ts=" << _ts);
        broadcastRequest(lts, re->getMessage(), re->getVersionVector());
    }
}

void
OrderedBcastManager::remoteRequest(const std::shared_ptr<MessageDesc>& messageDesc,
                                   const InstanceID reqSrc,
                                   const Timestamp reqTs,
                                   LivenessTracker::VersionVector& vVec)
{
    SCIDB_ASSERT(reqSrc != _selfInstanceId);
    SCIDB_ASSERT(isValidPhysicalInstance(messageDesc->getSourceInstanceID()));
    SCIDB_ASSERT(messageDesc->getSourceInstanceID() != _selfInstanceId);

    LogicalTimestamp lts(reqTs,reqSrc);

    LivenessTracker::MsgDeliveryAction msgState = _livenessTracker.getMsgDeliveryAction(vVec);
    if (msgState == LivenessTracker::DISCARD) {
        // the view of this message is older than ours
        LOG4CXX_DEBUG(logger, OBCASTMSG << "discard remote request lts = "<< lts);
        return;
    }

    SCIDB_ASSERT(!vVec.empty());
    SCIDB_ASSERT(vVec.count(reqSrc) > 0);
    std::shared_ptr<RequestEntry> re = std::make_shared<RequestEntry>(messageDesc, vVec);
    SCIDB_ASSERT(vVec.empty());

    if (msgState == LivenessTracker::DEFER) {
        // the view of this message is NOT older than ours but not the same
        _deferredQueue.push_back(DeferredQueue::value_type(lts,re));
        LOG4CXX_DEBUG(logger, OBCASTMSG << "defer remote request lts = "<< lts);
        return;
    }

    verifyNoDeferredInDebug(reqSrc);

    _ts = std::max(_ts,reqTs) + 1;

    SCIDB_ASSERT(msgState == LivenessTracker::DELIVER);
    deliverRemoteRequest(lts, re);
}

bool
OrderedBcastManager::deliverRemoteRequest(const LogicalTimestamp& lts,
                                          const std::shared_ptr<RequestEntry>& re,
                                          const bool isReply)
{
    // The request view of the liveness should be the same as ours
    // (i.e. the version vectors are identical).
    // We can proceed with this message ...
    SCIDB_ASSERT(_livenessTracker.isInSync());
    SCIDB_ASSERT(!_livenessTracker.getVersionVector().empty());
    SCIDB_ASSERT(_livenessTracker.getVersionVector()==re->getVersionVector());
    SCIDB_ASSERT(re->getVersionVector().count(lts.getInstanceId()) > 0);

    LOG4CXX_TRACE(logger, OBCASTMSG << "deliver remote request lts = "<< lts << " _ts="<<_ts);

    if (_remoteTimestamps.find(lts.getInstanceId()) != _remoteTimestamps.end() &&
        _remoteTimestamps[lts.getInstanceId()] >= lts.getTimestamp()) {
        LOG4CXX_ERROR(logger, OBCASTMSG << "dropping remote request with a STALE timestamp lts = "
                      << lts << " _ts = "<<_ts
                      << " current known ts = "<< _remoteTimestamps[lts.getInstanceId()]);
        SCIDB_ASSERT(false);
        return false;
    }

    std::pair<RequestQueue::iterator, bool> res = _queue.insert(RequestQueue::value_type(lts,re));
    if (!res.second) {
        LOG4CXX_ERROR(logger, OBCASTMSG << "dropping duplicate remote request lts = "
                     << lts << " !!!");
        SCIDB_ASSERT(false);
        return false;
    }

    _remoteTimestamps[lts.getInstanceId()] = lts.getTimestamp();

    if (isReply && _lastBroadcastTs < lts.getTimestamp()) {
        LOG4CXX_TRACE(logger, OBCASTMSG << "reply to remote request lts = "<< lts);
        broadcastReply(lts.getInstanceId(), lts.getTimestamp(), re->getVersionVector());
    }
    release();
    return true;
}

void OrderedBcastManager::remoteReply(const InstanceID repSrc,
                                      const Timestamp repTs,
                                      const InstanceID reqSrc,
                                      const Timestamp reqTs,
                                      LivenessTracker::VersionVector& vVec)
{
    SCIDB_ASSERT(repSrc != _selfInstanceId);

    LivenessTracker::MsgDeliveryAction msgState = _livenessTracker.getMsgDeliveryAction(vVec);
    if (msgState == LivenessTracker::DISCARD) {
        // the view of this message is older than ours
        LOG4CXX_DEBUG(logger, OBCASTMSG << "discard remote reply repSrc= "<< repSrc
                      << " repTs=" << repTs << " reqTs=" << reqTs);
        return;
    }

    SCIDB_ASSERT(!vVec.empty());
    SCIDB_ASSERT(vVec.count(repSrc) > 0);
    SCIDB_ASSERT(vVec.count(reqSrc) > 0);

    if (msgState == LivenessTracker::DEFER) {
        // the view of this message is NOT older than ours but not the same
        LogicalTimestamp lts(repTs,repSrc);
        std::shared_ptr<Entry> entry = std::make_shared<ReplyEntry>(reqTs, reqSrc, vVec);
        _deferredQueue.push_back(DeferredQueue::value_type(lts, entry));
        SCIDB_ASSERT(vVec.empty());
        LOG4CXX_DEBUG(logger, OBCASTMSG << "defer remote reply repSrc= "<< Iid(repSrc)
                      << " repTs=" << repTs << " reqSrc=" << Iid(reqSrc) << " reqTs=" << reqTs);
        return;
    }

    verifyNoDeferredInDebug(repSrc);

    _ts = std::max(_ts,repTs) + 1;

    SCIDB_ASSERT(msgState == LivenessTracker::DELIVER);
    deliverRemoteReply(repSrc, repTs, reqSrc, reqTs, vVec);
}

void OrderedBcastManager::deliverRemoteReply(const InstanceID repSrc,
                                             const Timestamp repTs,
                                             const InstanceID reqSrc,
                                             const Timestamp reqTs,
                                             const LivenessTracker::VersionVector& vVec)
{
    SCIDB_ASSERT(_livenessTracker.isInSync());
    SCIDB_ASSERT(!_livenessTracker.getVersionVector().empty());
    SCIDB_ASSERT(_livenessTracker.getVersionVector()==vVec);
    SCIDB_ASSERT(vVec.count(repSrc) > 0);
    SCIDB_ASSERT(vVec.count(reqSrc) > 0);

    LOG4CXX_TRACE(logger, OBCASTMSG << "deliver remote reply repSrc= "<< Iid(repSrc)
                  << " repTs=" << repTs << " reqSrc=" << Iid(reqSrc) << " reqTs=" << reqTs);

    if (_remoteTimestamps.find(repSrc) != _remoteTimestamps.end() &&
        _remoteTimestamps[repSrc] >= repTs) {
        LOG4CXX_ERROR(logger, OBCASTMSG
                      << "dropping remote reply with a STALE timestamp repSrc= "<< Iid(repSrc)
                      << " repTs=" << repTs << " reqSrc=" << Iid(reqSrc) << " reqTs=" << reqTs
                      << " _ts = "<<_ts
                      << " current known ts = "<< _remoteTimestamps[repSrc]);
        SCIDB_ASSERT(false);
        return;
    }
    _remoteTimestamps[repSrc] = repTs;

    release();
}

void OrderedBcastManager::verifyNoDeferredInDebug(InstanceID iId)
{
    if (!isDebug()) { return; }
    for (DeferredQueue::iterator iter = _deferredQueue.begin();
         iter != _deferredQueue.end(); ++iter)
    {
        const LogicalTimestamp& lts = iter->first ;
        SCIDB_ASSERT(lts.getInstanceId() != iId);
    }
}

void OrderedBcastManager::clearQueueInDebug()
{
    if (!isDebug()) { return; }

    // process the active queue
    for (RequestQueue::iterator iter = _queue.begin(); iter != _queue.end();) {

        const std::shared_ptr<RequestEntry>& entry = (*iter).second ;

        LivenessTracker::MsgDeliveryAction msgState =
                _livenessTracker.getMsgDeliveryAction(entry->getVersionVector());

        if (msgState == LivenessTracker::DISCARD) {
            // remove from queue
            _queue.erase(iter++);
            continue;
        }

        SCIDB_ASSERT(msgState == LivenessTracker::DELIVER);
        // All messages in the active queue must become undeliverable upon a liveness change
        // because a new liveness must be different from the previously agreed upon liveness.
        SCIDB_ASSERT(false);
        ++iter;
    }
    SCIDB_ASSERT(_queue.empty());
}

/// React to a changed liveness. If the liveness is in-sync with other instances,
/// do the following:
/// 1. Discard old messages from the active _queue.
/// 2. Discard old messages from the _deferredQueue
/// 3. Transfer (in order) the deferred requests and replies with the current version vector
/// from _deferredQueue to _queue.
/// 4. Process _queue normally
void OrderedBcastManager::processQueueOnLivenessChange()
{
    if(!_livenessTracker.isInSync()) {
        return;
    }
    _remoteTimestamps.clear(); //allow for remote clocks to be reset
    _lastBroadcastTs = 0; // some instances may have never seen our clock
    SCIDB_ASSERT(!_livenessTracker.getVersionVector().empty());
    _remoteTimestamps[_selfInstanceId] = _ts;

    clearQueueInDebug();
    _queue.clear();

    // for debug checking
    std::set<InstanceID> deferredInstances;

    Timestamp lastRemoteReqTs = 0;
    InstanceID lastRemoteReqInstance = INVALID_INSTANCE;

    // process deferred request messages
    for (DeferredQueue::iterator iter = _deferredQueue.begin(); iter != _deferredQueue.end(); )
    {
        const LogicalTimestamp& lts = iter->first ;
        const std::shared_ptr<Entry>& entry = iter->second ;

        if (entry->getVersionVector().empty()) {
            // local deferred request is not versioned
            SCIDB_ASSERT(lts.getInstanceId() == _selfInstanceId);
            LivenessTracker::VersionVector vVec = _livenessTracker.getVersionVector();
            entry->setVersionVector(vVec);
            SCIDB_ASSERT(!entry->getVersionVector().empty());
            SCIDB_ASSERT(vVec.empty());
        }

        LivenessTracker::MsgDeliveryAction msgState =
                _livenessTracker.getMsgDeliveryAction(entry->getVersionVector());

        if (msgState == LivenessTracker::DISCARD) {
            // remove from queue
            iter = _deferredQueue.erase(iter);
            continue;
        }

        if (msgState == LivenessTracker::DELIVER) {

            std::shared_ptr<OrderedBcastManager::RequestEntry> reqEntry =
                    dynamic_pointer_cast<OrderedBcastManager::RequestEntry>(entry);

            const InstanceID srcId = lts.getInstanceId();

            // Because we expect FIFO instance-to-instance ordering of messages and of liveness
            // we must not find a *deliverable* message after we have already deferred a message
            // from a given instance.
            SCIDB_ASSERT(deferredInstances.count(srcId) == 0);

            if (srcId == _selfInstanceId) {
                SCIDB_ASSERT(reqEntry);
                // local deferred request is not timestamped
                SCIDB_ASSERT(lts.getTimestamp() == 0);

                _ts = _ts + 1;

                deliverLocalRequest(LogicalTimestamp(_ts,_selfInstanceId), reqEntry);
            }
            else
            {
                _ts = std::max(_ts,lts.getTimestamp()) + 1;
                const bool dontReply = false;

                SCIDB_ASSERT(lts.getTimestamp() > 0);
                if (reqEntry == nullptr) {
                    std::shared_ptr<OrderedBcastManager::ReplyEntry> repEntry =
                            dynamic_pointer_cast<OrderedBcastManager::ReplyEntry>(entry);
                    SCIDB_ASSERT(repEntry);

                    deliverRemoteReply(srcId, //repSrc
                                       lts.getTimestamp(), //repTs
                                       repEntry->getRequestInstanceId(), //reqSrc
                                       repEntry->getRequestTimestamp(), // reqTs
                                       repEntry->getVersionVector());
                } else if (deliverRemoteRequest(lts, reqEntry, dontReply ) &&
                        lastRemoteReqTs < lts.getTimestamp()) {
                    // try to collapse all acks into one
                    lastRemoteReqInstance = lts.getInstanceId();
                    lastRemoteReqTs = lts.getTimestamp();
                }
            }
            iter = _deferredQueue.erase(iter);
            continue;
        }
        if (isDebug()) {
            deferredInstances.insert(lts.getInstanceId());
        }
        ++iter;
    }

    if (_lastBroadcastTs < lastRemoteReqTs) {
        SCIDB_ASSERT(lastRemoteReqTs>0);
        SCIDB_ASSERT(isValidPhysicalInstance(lastRemoteReqInstance));
        LOG4CXX_TRACE(logger, OBCASTMSG << "reply to remote request lts = "
                      << LogicalTimestamp(lastRemoteReqInstance, lastRemoteReqTs));
        broadcastReply(lastRemoteReqInstance, lastRemoteReqTs,
                       _livenessTracker.getVersionVector());
    }

    LOG4CXX_DEBUG(logger, OBCASTMSG << "_queue size="<< _queue.size());
    LOG4CXX_TRACE(logger, OBCASTMSG << "_queue: "<< _queue);
    LOG4CXX_DEBUG(logger, OBCASTMSG << "_deferredQueue size="<< _deferredQueue.size()) ;
    LOG4CXX_TRACE(logger, OBCASTMSG << "_deferredQueue: "<< _deferredQueue) ;
}

void OrderedBcastManager::release()
{
    if (!_livenessTracker.isInSync()) {
        return;
    }
    _remoteTimestamps[_selfInstanceId] = _ts;

    LOG4CXX_TRACE(logger, OBCASTMSG << "trying to release based on remote state="
                  << _remoteTimestamps);

    RequestQueue::const_iterator iter = _queue.begin();
    for ( ; iter != _queue.end(); ++iter) {

        const std::shared_ptr<RequestEntry>& entry = iter->second ;

        if (isDebug()) {
            LivenessTracker::MsgDeliveryAction msgState =
                    _livenessTracker.getMsgDeliveryAction(entry->getVersionVector());
            SCIDB_ASSERT(msgState == LivenessTracker::DELIVER);
        }

        if (isOkToRelease(iter->first) ) {
            LOG4CXX_TRACE(logger, OBCASTMSG << "release request lts = "<< iter->first << " " <<*entry);
            try {
                _nm->sendLocal(entry->getMessage());
            } catch (const scidb::Exception& e) {
                // XXX only the overflow & memory errors are expected to bubble up
                // XXX overflow is reported as query error by message dispatch
                // XXX memory errors are still an issue
                LOG4CXX_ERROR(logger, "Exception in message handler: " << e.what());
                LOG4CXX_ERROR(logger, "Exception in message handler: messageType = "
                              << entry->getMessage()->getMessageType());
                LOG4CXX_ERROR(logger, "Exception in message handler: source instance ID = "
                              << entry->getMessage()->getSourceInstanceID());
            }
        } else {
            break;
        }
    }
    _queue.erase(_queue.begin(), iter);
    LOG4CXX_TRACE(logger, OBCASTMSG << "_queue: "<< _queue);
    LOG4CXX_TRACE(logger, OBCASTMSG << "_deferredQueue: "<< _deferredQueue) ;
}

bool OrderedBcastManager::isOkToRelease(const LogicalTimestamp& lts)
{
    SCIDB_ASSERT(_livenessTracker.isInSync());
    const LivenessTracker::VersionVector& vVec = _livenessTracker.getVersionVector();
    SCIDB_ASSERT(vVec.size() > 0);

    if (vVec.size() > _remoteTimestamps.size())  {
        return false;
    }
    SCIDB_ASSERT(_remoteTimestamps.size() > 0);
    SCIDB_ASSERT(vVec.size() == _remoteTimestamps.size());

    const Timestamp ts = lts.getTimestamp();
    for (InstanceClocks::const_iterator rIter = _remoteTimestamps.begin();
         rIter != _remoteTimestamps.end(); ++rIter) {
        const InstanceID iId = rIter->first;
        SCIDB_ASSERT(vVec.find(iId) != vVec.end());
        if (rIter->second < ts) { return false; }
    }
    return true;
}

void
OrderedBcastManager::broadcastReply(InstanceID requestId,
                                    uint64_t requestTs,
                                    const LivenessTracker::VersionVector& vVec)
{
    SCIDB_ASSERT(requestId != _selfInstanceId);
    SCIDB_ASSERT(_livenessTracker.isInSync());
    SCIDB_ASSERT(_livenessTracker.getVersionVector() == vVec);

    LOG4CXX_TRACE(logger, OBCASTMSG << "reply to requestId= "<< requestId
                  << " requestTs=" << requestTs << " local _ts="<<_ts);

    std::shared_ptr<MessageDesc> replyMessageDesc =
            std::make_shared<Connection::ServerMessageDesc>();

    replyMessageDesc->initRecord(mtOBcastReply);
    replyMessageDesc->setSourceInstanceID(_selfInstanceId);

    std::shared_ptr<scidb_msg::OrderedBcastReply> record =
            replyMessageDesc->getRecord<scidb_msg::OrderedBcastReply>();

    record->set_request_timestamp(requestTs);
    record->set_request_instance(requestId);
    SCIDB_ASSERT(LogicalTimestamp(_ts, _selfInstanceId) > LogicalTimestamp(requestTs,requestId));
    record->set_timestamp(_ts); // _ts must be incremented appropriately at this point

    scidb_msg::LivenessVector* vvMsg = record->mutable_vector();
    SCIDB_ASSERT(vvMsg);

    bool rc = messageUtils::serializeLivenessVector(vVec.begin(),
                                                    vVec.end(),
                                                    vvMsg);
    SCIDB_ASSERT(rc);

    SCIDB_ASSERT(replyMessageDesc->getSourceInstanceID() == _selfInstanceId);
    SCIDB_ASSERT(_lastBroadcastTs < _ts);

    //XXX TODO: the reply broadcast should be throttled back (based on time ?) to avoid N^2 messaging
    try {
        _nm->multicastPhysical(vVec.begin(), vVec.end(), _livenessTracker, replyMessageDesc);
    } catch (const scidb::Exception& e) {
        // XXX only the overflow & memory errors are expected to bubble up
        // XXX overflow is reported as query error by message dispatch
        // XXX memory errors are still an issue
        LOG4CXX_ERROR(logger, "FAILED to multicast mtOBcastReply because: " << e.what());
        SCIDB_ASSERT(e.getLongErrorCode() != scidb::SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
        throw std::runtime_error(e.what());
    }
    _lastBroadcastTs = _ts;
}

void OrderedBcastManager::broadcastRequest(const LogicalTimestamp& lts,
                                           const std::shared_ptr<MessageDesc>& messageDesc,
                                           const LivenessTracker::VersionVector& vVec)
{
    SCIDB_ASSERT(_livenessTracker.isInSync());
    SCIDB_ASSERT(_livenessTracker.getVersionVector() == vVec);
    SCIDB_ASSERT(messageDesc->getSourceInstanceID() == _selfInstanceId);

    std::shared_ptr<SharedBuffer> binary = messageDesc->getBinary();
    std::shared_ptr<MessageDesc> requestMessageDesc =
            std::make_shared<Connection::ServerMessageDesc>(binary);

    requestMessageDesc->initRecord(mtOBcastRequest);
    requestMessageDesc->setQueryID(messageDesc->getQueryID());
    requestMessageDesc->setSourceInstanceID(messageDesc->getSourceInstanceID());

    std::shared_ptr<scidb_msg::OrderedBcastRequest> obcastRecord =
            requestMessageDesc->getRecord<scidb_msg::OrderedBcastRequest>();

    SCIDB_ASSERT(lts.getInstanceId() == _selfInstanceId);
    SCIDB_ASSERT(lts.getTimestamp() <= _ts);

    obcastRecord->set_timestamp(lts.getTimestamp());
    obcastRecord->set_payload_message_type(messageDesc->getMessageType());
    std::string message;
    messageDesc->getRecord()->SerializeToString(&message);
    obcastRecord->set_payload_message(message);

    LOG4CXX_TRACE(logger, "mtOBcastRequest: ts="<< obcastRecord->timestamp()
                  << " my instanceID=" << Iid(requestMessageDesc->getSourceInstanceID()));

    scidb_msg::LivenessVector* vvMsg = obcastRecord->mutable_vector();
    SCIDB_ASSERT(vvMsg);

    bool rc = messageUtils::serializeLivenessVector(vVec.begin(),
                                                    vVec.end(),
                                                    vvMsg);
    SCIDB_ASSERT(rc);

    SCIDB_ASSERT(requestMessageDesc->getSourceInstanceID() == _selfInstanceId);
    SCIDB_ASSERT(_lastBroadcastTs < lts.getTimestamp());
    try {
        _nm->multicastPhysical(vVec.begin(), vVec.end(), _livenessTracker, requestMessageDesc);
    } catch (const scidb::Exception& e) {
        // XXX only the overflow & memory errors are expected to bubble up
        // XXX overflow is reported as query error by message dispatch
        // XXX memory errors are still an issue
        LOG4CXX_ERROR(logger, "FAILED to multicast mtOBcastRequest because: " << e.what());
        SCIDB_ASSERT(e.getLongErrorCode() != scidb::SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
        throw std::runtime_error(e.what());
    }
    _lastBroadcastTs = lts.getTimestamp();

}

std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::RequestEntry& re)
{
    os << "ReqEntry [ ";
    os << "msgId=" << re.getMessage()->getMessageType() << " "
    <<" vVec="<< re.getVersionVector() ;
    os << " ]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::LogicalTimestamp& lts)
{
    os << lts.getTimestamp() << "," << lts.getInstanceId() ;
    return os;
}

std::ostream& operator<<(std::ostream& os, const std::map<InstanceID, uint64_t>& m)
{
    os << "OrdMap size=" << m.size() <<" [ ";
    for (std::map<InstanceID, uint64_t>::const_iterator iter = m.begin();
         iter != m.end(); ++iter) {
        os << " (" << iter->first << "," << iter->second <<")" ;
    }
    os << " ]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::ReplyEntry& re)
{
   os << "RepEntry [ ";
   os << "reqTs=" << re.getRequestTimestamp() ;
   os << " reqSrc=" << re.getRequestInstanceId() ;
   os << " vVec=" << re.getVersionVector() ;
   os << " ]";
   return os;
}

std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::RequestQueue& rq)
{
    os << "ReqQ size=" << rq.size() <<" [ ";
    for (OrderedBcastManager::RequestQueue::const_iterator iter = rq.begin();
         iter != rq.end(); ++iter) {
        os << " (" << iter->first << ", " << *(iter->second) <<")";
    }
    os << " ]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::DeferredQueue& rq)
{
    os << "DefQ size=" << rq.size() <<" [ ";
    for (OrderedBcastManager::DeferredQueue::const_iterator iter = rq.begin();
         iter != rq.end(); ++iter) {
        os << " (" << iter->first << ", ";
        OrderedBcastManager::Entry* e = iter->second.get();
        if (dynamic_cast<OrderedBcastManager::RequestEntry*>(e)) {
            os << *static_cast<OrderedBcastManager::RequestEntry*>(e);
        } else if (dynamic_cast<OrderedBcastManager::ReplyEntry*>(e)) {
            os << *static_cast<OrderedBcastManager::ReplyEntry*>(e);
        } else {
            assert(false);
        }
        os <<")";
    }
    os << " ]";
    return os;
}

} //namespace scidb


