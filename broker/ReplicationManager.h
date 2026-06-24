// broker/ReplicationManager.h
#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <chrono>
#include <iostream>
#include <algorithm>
#include "BrokerInfo.h"

#ifdef _WIN32
    #ifndef _WIN32_WINNT
        #define _WIN32_WINNT 0x0600
    #endif
    #include <windows.h>
    typedef CRITICAL_SECTION PlatformMutex;
#else
    #include <mutex>
    typedef std::mutex PlatformMutex;
#endif

// Custom lock guard for PlatformMutex
class ReplicationLockGuard {
    PlatformMutex* mtx;
public:
    ReplicationLockGuard(PlatformMutex* m) : mtx(m) {
#ifdef _WIN32
        EnterCriticalSection(mtx);
#else
        mtx->lock();
#endif
    }
    ~ReplicationLockGuard() {
#ifdef _WIN32
        LeaveCriticalSection(mtx);
#else
        mtx->unlock();
#endif
    }
};

struct PartitionReplicaInfo {
    int leaderBrokerId;
    std::vector<int> followerBrokerIds;
    std::vector<int> isr;  // In-Sync Replicas
    long lastReplicatedOffset;
    
    PartitionReplicaInfo() : leaderBrokerId(-1), lastReplicatedOffset(0) {}
};

class ReplicationManager {
private:
    int currentBrokerId;
    std::unordered_map<int, BrokerInfo> knownBrokers;
    std::unordered_map<std::string, PartitionReplicaInfo> partitionInfo;
    PlatformMutex replicationMutex;
    
    std::string getPartitionKey(const std::string& topic, int partition) const {
        return topic + "-" + std::to_string(partition);
    }
    
public:
    ReplicationManager(int brokerId) : currentBrokerId(brokerId) {
#ifdef _WIN32
        InitializeCriticalSection(&replicationMutex);
#endif
    }
    
    ~ReplicationManager() {
#ifdef _WIN32
        DeleteCriticalSection(&replicationMutex);
#endif
    }
    
    void registerBroker(const BrokerInfo& broker) {
        ReplicationLockGuard lock(&replicationMutex);
        knownBrokers[broker.brokerId] = broker;
        std::cout << "[REPLICATION] Registered " << broker.toString() << std::endl;
    }
    
    void unregisterBroker(int brokerId) {
        ReplicationLockGuard lock(&replicationMutex);
        knownBrokers.erase(brokerId);
        std::cout << "[REPLICATION] Unregistered Broker " << brokerId << std::endl;
    }
    
    bool isBrokerAlive(int brokerId) {
        ReplicationLockGuard lock(&replicationMutex);
        auto it = knownBrokers.find(brokerId);
        if (it != knownBrokers.end()) {
            return it->second.isAlive();
        }
        return false;
    }
    
    void setPartitionLeader(const std::string& topic, int partition, int leaderBrokerId) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it == partitionInfo.end()) {
            PartitionReplicaInfo info;
            info.leaderBrokerId = leaderBrokerId;
            partitionInfo[key] = info;
        } else {
            it->second.leaderBrokerId = leaderBrokerId;
        }
        
        std::cout << "[REPLICATION] Partition " << topic << "-" << partition 
                  << " leader set to Broker " << leaderBrokerId << std::endl;
    }
    
    int getPartitionLeader(const std::string& topic, int partition) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it != partitionInfo.end()) {
            return it->second.leaderBrokerId;
        }
        return -1;
    }
    
    void addFollower(const std::string& topic, int partition, int followerBrokerId) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it == partitionInfo.end()) {
            PartitionReplicaInfo info;
            info.leaderBrokerId = currentBrokerId;
            info.followerBrokerIds.push_back(followerBrokerId);
            info.isr.push_back(followerBrokerId);
            partitionInfo[key] = info;
        } else {
            auto& followers = it->second.followerBrokerIds;
            if (std::find(followers.begin(), followers.end(), followerBrokerId) == followers.end()) {
                followers.push_back(followerBrokerId);
                it->second.isr.push_back(followerBrokerId);
            }
        }
        
        std::cout << "[REPLICATION] Broker " << followerBrokerId 
                  << " added as follower for " << topic << "-" << partition << std::endl;
    }
    
    std::vector<int> getFollowers(const std::string& topic, int partition) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it != partitionInfo.end()) {
            return it->second.followerBrokerIds;
        }
        return {};
    }
    
    std::vector<int> getISR(const std::string& topic, int partition) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it != partitionInfo.end()) {
            return it->second.isr;
        }
        return {};
    }
    
    bool isLeader(const std::string& topic, int partition) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it != partitionInfo.end()) {
            return it->second.leaderBrokerId == currentBrokerId;
        }
        return false;
    }
    
    void replicateToFollowers(const std::string& topic, int partition, 
                             long offset, const std::string& message) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it != partitionInfo.end()) {
            it->second.lastReplicatedOffset = offset;
            
            std::cout << "[REPLICATION] Replicating message to " 
                      << it->second.followerBrokerIds.size() 
                      << " followers for " << topic << "-" << partition 
                      << " offset " << offset << std::endl;
        }
    }
    
    void updateFollowerOffset(const std::string& topic, int partition, 
                             int followerId, long offset) {
        ReplicationLockGuard lock(&replicationMutex);
        // In real implementation, track per-follower offsets
    }
    
    int electNewLeader(const std::string& topic, int partition) {
        ReplicationLockGuard lock(&replicationMutex);
        std::string key = getPartitionKey(topic, partition);
        
        auto it = partitionInfo.find(key);
        if (it != partitionInfo.end()) {
            for (int brokerId : it->second.isr) {
                if (brokerId != currentBrokerId && isBrokerAlive(brokerId)) {
                    it->second.leaderBrokerId = brokerId;
                    std::cout << "[REPLICATION] New leader elected: Broker " 
                              << brokerId << " for " << topic << "-" << partition << std::endl;
                    return brokerId;
                }
            }
        }
        return -1;
    }
    
    std::string toString() const {
        std::string result = "Replication Manager (Broker " + std::to_string(currentBrokerId) + ")\n";
        result += "Known Brokers: " + std::to_string(knownBrokers.size()) + "\n";
        result += "Partitions: " + std::to_string(partitionInfo.size()) + "\n";
        
        for (const auto& pair : partitionInfo) {
            result += "  " + pair.first + ": Leader=" + std::to_string(pair.second.leaderBrokerId);
            result += ", Followers=" + std::to_string(pair.second.followerBrokerIds.size());
            result += ", ISR=" + std::to_string(pair.second.isr.size()) + "\n";
        }
        return result;
    }
};