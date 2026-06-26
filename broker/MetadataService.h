// broker/MetadataService.h
#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>
#include "BrokerInfo.h"
#include "Metadata.h"

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

class MetadataLockGuard {
    PlatformMutex* mtx;
public:
    MetadataLockGuard(PlatformMutex* m) : mtx(m) {
#ifdef _WIN32
        EnterCriticalSection(mtx);
#else
        mtx->lock();
#endif
    }
    ~MetadataLockGuard() {
#ifdef _WIN32
        LeaveCriticalSection(mtx);
#else
        mtx->unlock();
#endif
    }
};

struct ClusterMetadata {
    std::unordered_map<int, BrokerInfo> brokers;
    std::unordered_map<std::string, TopicMetadata> topics;
    std::chrono::steady_clock::time_point lastUpdate;
    int controllerBrokerId;
    
    ClusterMetadata() : controllerBrokerId(-1) {
        lastUpdate = std::chrono::steady_clock::now();
    }
};

class MetadataService {
private:
    ClusterMetadata clusterMetadata;
    PlatformMutex metadataMutex;
    std::string metadataFilePath;
    
    void saveToDisk();
    void loadFromDisk();
    
public:
    MetadataService(int brokerId = 1);
    ~MetadataService();
    
    // Broker management
    void registerBroker(const BrokerInfo& broker);
    void unregisterBroker(int brokerId);
    void updateBrokerHeartbeat(int brokerId);
    std::vector<int> getActiveBrokers();
    BrokerInfo getBrokerInfo(int brokerId);
    
    // Topic management
    bool createTopic(const std::string& topicName, 
                     int partitionCount, 
                     int replicationFactor = 1);
    bool deleteTopic(const std::string& topicName);
    bool topicExists(const std::string& topicName);
    TopicMetadata getTopicMetadata(const std::string& topicName);
    std::vector<std::string> listTopics();
    
    // Partition management
    bool addPartition(const std::string& topicName, 
                      int partitionId, 
                      int leaderBrokerId,
                      const std::vector<int>& replicas);
    bool updatePartitionLeader(const std::string& topicName, 
                               int partitionId, 
                               int newLeaderId);
    bool updateISR(const std::string& topicName, 
                   int partitionId, 
                   const std::vector<int>& isr);
    PartitionMetadataInfo getPartitionMetadata(const std::string& topicName, 
                                               int partitionId);
    std::vector<int> getPartitionReplicas(const std::string& topicName, 
                                          int partitionId);
    void updatePartitionCount(const std::string& topicName, 
                              int partitionId, 
                              long count);
    
    // Controller management
    void setController(int brokerId);
    int getController();
    bool isController(int brokerId);
    
    // Cluster health
    bool isClusterHealthy();
    std::vector<std::string> getUnhealthyBrokers();
    
    // Stats and monitoring
    int getTotalMessageCount();
    int getTotalPartitionCount();
    std::string getClusterSummary();
    std::string getFullMetadata();
    
    // Save/Load
    void persistMetadata();
    void recoverMetadata();
};