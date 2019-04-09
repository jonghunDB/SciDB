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
 * ReplicationManager.cpp
 *
 * Description: Poor man's replication manager that blocks the replicating thread if the network is congested
 */

#include "ReplicationManager.h"

#include <network/MessageDesc.h>
#include <util/InjectedErrorCodes.h>
#include <system/Config.h>

using namespace std;
namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.replication"));

ReplicationManager::ReplicationManager()
:
    _repEvent(),
    _subscribeId(0),
    _listenerInjectedErrorReplicaWait(InjectErrCode::REPLICA_WAIT),
    _listenerInjectedErrorReplicaSend(InjectErrCode::REPLICA_SEND)
{}

void ReplicationManager::start(const std::shared_ptr<JobQueue>& jobQueue)
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);
    assert(!_subscribeId);
    Notification<NetworkManager::ConnectionStatus>::Subscriber pListener =
        boost::bind(&ReplicationManager::handleConnectionStatus, this, _1); // bare pointer because RM should never go away
    _subscribeId = Notification<NetworkManager::ConnectionStatus>::subscribe(pListener);
    assert(_subscribeId);

    // initialize inbound replication queue
    int size = Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE);
    assert(size>0);
    size = (size<1) ? 4 : size+4; // allow some minimal extra space to tolerate mild overflows
    // this queue is single-threaded because the order of replicas is important (per source)
    // and CachedStorage serializes everything anyway via THE mutex.
    _inboundReplicationQ = std::make_shared<WorkQueue>(jobQueue, "ReplicationManagerWorkQueue", 1, static_cast<uint64_t>(size));
    _listenerInjectedErrorReplicaWait.start();
    _listenerInjectedErrorReplicaSend.start();
}

void ReplicationManager::stop()
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);
    Notification<NetworkManager::ConnectionStatus>::unsubscribe(_subscribeId);
    clear();
    _listenerInjectedErrorReplicaWait.stop();
    _listenerInjectedErrorReplicaSend.stop();
    _repEvent.signal();
}

void ReplicationManager::send(const std::shared_ptr<Item>& item)
{
    assert(item);
    assert(!item->isDone());
    assert(_subscribeId);

    ScopedMutexLock cs(_repMutex, PTW_SML_REP);
    std::shared_ptr<RepItems>& ri = _repQueue[item->getInstanceId()];
    if (!ri) {
        ri = std::shared_ptr<RepItems>(new RepItems);
    }
    ri->push_back(item);
    if (ri->size() == 1) {
        assert(ri->front() == item);
        sendItem(*ri);
    }
}

void ReplicationManager::wait(const std::shared_ptr<Item>& item)
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);

    assert(_subscribeId);

    if (item->isDone()) {
        item->validate();
        return;
    }

    std::shared_ptr<RepItems>& ri = _repQueue[item->getInstanceId()];
    assert(ri);

    Event::ErrorChecker ec = boost::bind(&ReplicationManager::checkItemState, item);

    while (true) {

        LOG4CXX_TRACE(logger, "ReplicationManager::wait: about to wait for instance=" << item->getInstanceId()
                      << ", size=" << item->getChunkMsg()->getMessageSize()
                      << ", query (" << item->getChunkMsg()->getQueryID()<<")"
                      << ", queue size="<< ri->size());
        assert(!ri->empty());

        bool mine = (ri->front() == item);
        bool res = sendItem(*ri);
        if (mine && res) {
            assert(item->isDone());
            item->validate();
            return;
        }
        if (!res) {
            try {
                _listenerInjectedErrorReplicaWait.throwif(__LINE__, __FILE__);
                _repEvent.wait(_repMutex, ec, PTW_EVENT_REP);
            } catch (Exception& e) {
                item->setDone(e.copy());
                throw;
            }
        }
        if (item->isDone()) {
            item->validate();
            return;
        }
    }
}

void ReplicationManager::handleConnectionStatus(Notification<NetworkManager::ConnectionStatus>::MessageTypePtr connStatus)
{
    assert(connStatus->getPhysicalInstanceId() != INVALID_INSTANCE);

    LOG4CXX_TRACE(logger, "ReplicationManager::handleConnectionStatus: notification for instance="
                  << connStatus->getPhysicalInstanceId()
                  << ", remote receive queue size="
                  << connStatus->getAvailableQueueSize());

    if (connStatus->getQueueType() != NetworkManager::mqtReplication)
    {
        return;
    }
    if (connStatus->getAvailableQueueSize() <= 0)
    {
        return;
    }
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);

    RepQueue::iterator iter = _repQueue.find(connStatus->getPhysicalInstanceId());
    if (iter == _repQueue.end()) {
        return;
    }

    LOG4CXX_TRACE(logger, "ReplicationManager::handleConnectionStatus: notification for instance="
                  << connStatus->getPhysicalInstanceId()
                  << ", local replication queue size="<< iter->second->size()
                  << ", remote receive queue size="<< connStatus->getAvailableQueueSize());
    _repEvent.signal();
}

bool ReplicationManager::sendItem(RepItems& ri)
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);

    const std::shared_ptr<Item>& item = ri.front();

    if (item->isDone()) {
        ri.pop_front();
        return true;
    }
    try {
        std::shared_ptr<Query> q(Query::getValidQueryPtr(item->getQuery()));

        std::shared_ptr<MessageDesc> chunkMsg(item->getChunkMsg());
        NetworkManager::getInstance()->sendPhysical(item->getInstanceId(), chunkMsg,
                                                   NetworkManager::mqtReplication);
        LOG4CXX_TRACE(logger, "ReplicationManager::sendItem: successful replica chunk send to instance="
                      << item->getInstanceId()
                      << ", size=" << item->getChunkMsg()->getMessageSize()
                      << ", query (" << q->getQueryID()<<")"
                      << ", queue size="<< ri.size());
        _listenerInjectedErrorReplicaSend.throwif(__LINE__, __FILE__);
        item->setDone();
    } catch (NetworkManager::OverflowException& e) {
        assert(e.getQueueType() == NetworkManager::mqtReplication);
        return false;
    } catch (Exception& e) {
        item->setDone(e.copy());
    }
    ri.pop_front();
    return true;
}

void ReplicationManager::clear()
{
    // mutex must be locked
    for (RepQueue::iterator iter = _repQueue.begin(); iter != _repQueue.end(); ++iter) {
        std::shared_ptr<RepItems>& ri = iter->second;
        assert(ri);
        for (RepItems::iterator i=ri->begin(); i != ri->end(); ++i) {
            (*i)->setDone(SYSTEM_EXCEPTION_SPTR(SCIDB_SE_REPLICATION, SCIDB_LE_UNKNOWN_ERROR));
        }
    }
    _repQueue.clear();
    _repEvent.signal();
}

}

