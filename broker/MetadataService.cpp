#include "MetadataService.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <vector>
#include <unordered_map>

// REMOVED: Duplicate MetadataLockGuard definition (already in MetadataService.h)
// REMOVED: Duplicate BrokerInfo definition (already in BrokerInfo.h)

MetadataService::MetadataService(int brokerId) {
#ifdef _WIN32
    InitializeCriticalSection(&metadataMutex);
#endif
    
    metadataFilePath = "data/metadata/cluster_metadata.json";
    clusterMetadata.controllerBrokerId = brokerId;
    
    BrokerInfo self(brokerId, "localhost", 9092 + brokerId - 1);
    registerBroker(self);
    
    recoverMetadata();
    
    std::cout << "[METADATA] Metadata Service initialized for Broker " << brokerId << std::endl;
}

MetadataService::~MetadataService() {
    persistMetadata();
#ifdef _WIN32
    DeleteCriticalSection(&metadataMutex);
#endif
}

void MetadataService::registerBroker(const BrokerInfo& broker) {
    MetadataLockGuard lock(&metadataMutex);
    clusterMetadata.brokers[broker.brokerId] = broker;
    clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
    std::cout << "[METADATA] Registered " << broker.toString() << std::endl;
}

void MetadataService::unregisterBroker(int brokerId) {
    MetadataLockGuard lock(&metadataMutex);
    clusterMetadata.brokers.erase(brokerId);
    clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
    std::cout << "[METADATA] Unregistered Broker " << brokerId << std::endl;
    triggerLeaderElection(brokerId);
}

void MetadataService::triggerLeaderElection(int failedBrokerId) {
    std::cout << "[ELECTION] Triggering leader election due to failure of Broker " << failedBrokerId << std::endl;
    
    std::vector<int> active;
    for (const auto& pair : clusterMetadata.brokers) {
        if (pair.second.isAlive()) {
            active.push_back(pair.first);
        }
    }
    
    for (auto& topicPair : clusterMetadata.topics) {
        for (auto& partition : topicPair.second.partitions) {
            if (partition->leaderBrokerId == failedBrokerId) {
                std::cout << "[ELECTION] Leader of " << topicPair.first << "-" << partition->partitionId 
                          << " was failed Broker " << failedBrokerId << ". Electing new leader..." << std::endl;
                
                int newLeader = -1;
                
                // 1. Try to elect from ISR
                for (int replicaId : partition->isr) {
                    if (replicaId != failedBrokerId && std::find(active.begin(), active.end(), replicaId) != active.end()) {
                        newLeader = replicaId;
                        break;
                    }
                }
                
                // 2. If not found in ISR, try any active replica
                if (newLeader == -1) {
                    for (int replicaId : partition->replicas) {
                        if (replicaId != failedBrokerId && std::find(active.begin(), active.end(), replicaId) != active.end()) {
                            newLeader = replicaId;
                            break;
                        }
                    }
                }
                
                // 3. Fallback: pick any active broker in the cluster (unclean election)
                if (newLeader == -1 && !active.empty()) {
                    newLeader = active[0];
                }
                
                partition->leaderBrokerId = newLeader;
                std::cout << "[ELECTION] Partition " << topicPair.first << "-" << partition->partitionId 
                          << " new leader elected: Broker " << newLeader << std::endl;
            }
            
            auto& isr = partition->isr;
            isr.erase(std::remove(isr.begin(), isr.end(), failedBrokerId), isr.end());
        }
    }
    
    saveToDisk();
}

void MetadataService::updateBrokerHeartbeat(int brokerId) {
    MetadataLockGuard lock(&metadataMutex);
    auto it = clusterMetadata.brokers.find(brokerId);
    if (it != clusterMetadata.brokers.end()) {
        it->second.updateHeartbeat();
        clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
    }
}

std::vector<int> MetadataService::getActiveBrokers() {
    MetadataLockGuard lock(&metadataMutex);
    std::vector<int> active;
    for (const auto& pair : clusterMetadata.brokers) {
        if (pair.second.isAlive()) {
            active.push_back(pair.first);
        }
    }
    return active;
}

BrokerInfo MetadataService::getBrokerInfo(int brokerId) {
    MetadataLockGuard lock(&metadataMutex);
    auto it = clusterMetadata.brokers.find(brokerId);
    if (it != clusterMetadata.brokers.end()) {
        return it->second;
    }
    return BrokerInfo();
}

bool MetadataService::createTopic(const std::string& topicName, 
                                  int partitionCount, 
                                  int replicationFactor) {
    MetadataLockGuard lock(&metadataMutex);
    
    if (clusterMetadata.topics.find(topicName) != clusterMetadata.topics.end()) {
        std::cout << "[METADATA] Topic already exists: " << topicName << std::endl;
        return false;
    }
    
    TopicMetadata topicMetadata(topicName, partitionCount, replicationFactor);
    
    // Create partition metadata
    for (int i = 0; i < partitionCount; i++) {
        auto partition = std::make_shared<PartitionMetadata>();
        partition->partitionId = i;
        partition->leaderBrokerId = -1;
        partition->messageCount = 0;
        partition->startOffset = 0;
        partition->endOffset = 0;
        topicMetadata.partitions.push_back(partition);
    }
    
    clusterMetadata.topics[topicName] = topicMetadata;
    clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
    
    persistMetadata();
    std::cout << "[METADATA] Created topic: " << topicName 
              << " (Partitions: " << partitionCount 
              << ", Replication: " << replicationFactor << ")" << std::endl;
    return true;
}

bool MetadataService::deleteTopic(const std::string& topicName) {
    MetadataLockGuard lock(&metadataMutex);
    auto it = clusterMetadata.topics.find(topicName);
    if (it == clusterMetadata.topics.end()) {
        return false;
    }
    clusterMetadata.topics.erase(it);
    clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
    persistMetadata();
    return true;
}

bool MetadataService::topicExists(const std::string& topicName) {
    MetadataLockGuard lock(&metadataMutex);
    return clusterMetadata.topics.find(topicName) != clusterMetadata.topics.end();
}

TopicMetadata MetadataService::getTopicMetadata(const std::string& topicName) {
    MetadataLockGuard lock(&metadataMutex);
    auto it = clusterMetadata.topics.find(topicName);
    if (it != clusterMetadata.topics.end()) {
        return it->second;
    }
    return TopicMetadata();
}

std::vector<std::string> MetadataService::listTopics() {
    MetadataLockGuard lock(&metadataMutex);
    std::vector<std::string> topics;
    for (const auto& pair : clusterMetadata.topics) {
        topics.push_back(pair.first);
    }
    return topics;
}

bool MetadataService::addPartition(const std::string& topicName, 
                                   int partitionId, 
                                   int leaderBrokerId,
                                   const std::vector<int>& replicas) {
    MetadataLockGuard lock(&metadataMutex);
    auto it = clusterMetadata.topics.find(topicName);
    if (it == clusterMetadata.topics.end()) {
        return false;
    }
    
    auto partition = std::make_shared<PartitionMetadata>();
    partition->partitionId = partitionId;
    partition->leaderBrokerId = leaderBrokerId;
    partition->replicas = replicas;
    partition->isr = replicas;
    partition->messageCount = 0;
    
    it->second.partitions.push_back(partition);
    it->second.partitionCount = it->second.partitions.size();
    clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
    persistMetadata();
    return true;
}

bool MetadataService::updatePartitionLeader(const std::string& topicName, 
                                            int partitionId, 
                                            int newLeaderId) {
    MetadataLockGuard lock(&metadataMutex);
    auto topicIt = clusterMetadata.topics.find(topicName);
    if (topicIt == clusterMetadata.topics.end()) {
        return false;
    }
    
    for (auto& partition : topicIt->second.partitions) {
        if (partition->partitionId == partitionId) {
            partition->leaderBrokerId = newLeaderId;
            clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
            persistMetadata();
            return true;
        }
    }
    return false;
}

bool MetadataService::updateISR(const std::string& topicName, 
                                int partitionId, 
                                const std::vector<int>& isr) {
    MetadataLockGuard lock(&metadataMutex);
    auto topicIt = clusterMetadata.topics.find(topicName);
    if (topicIt == clusterMetadata.topics.end()) {
        return false;
    }
    
    for (auto& partition : topicIt->second.partitions) {
        if (partition->partitionId == partitionId) {
            partition->isr = isr;
            clusterMetadata.lastUpdate = std::chrono::steady_clock::now();
            persistMetadata();
            return true;
        }
    }
    return false;
}

PartitionMetadataInfo MetadataService::getPartitionMetadata(const std::string& topicName, 
                                                            int partitionId) {
    MetadataLockGuard lock(&metadataMutex);
    auto topicIt = clusterMetadata.topics.find(topicName);
    if (topicIt == clusterMetadata.topics.end()) {
        return PartitionMetadataInfo();
    }
    
    for (const auto& partition : topicIt->second.partitions) {
        if (partition->partitionId == partitionId) {
            return PartitionMetadataInfo(*partition);
        }
    }
    return PartitionMetadataInfo();
}

std::vector<int> MetadataService::getPartitionReplicas(const std::string& topicName, 
                                                       int partitionId) {
    MetadataLockGuard lock(&metadataMutex);
    auto topicIt = clusterMetadata.topics.find(topicName);
    if (topicIt == clusterMetadata.topics.end()) {
        return {};
    }
    
    for (const auto& partition : topicIt->second.partitions) {
        if (partition->partitionId == partitionId) {
            return partition->replicas;
        }
    }
    return {};
}

void MetadataService::updatePartitionCount(const std::string& topicName, 
                                           int partitionId, 
                                           long count) {
    MetadataLockGuard lock(&metadataMutex);
    auto topicIt = clusterMetadata.topics.find(topicName);
    if (topicIt == clusterMetadata.topics.end()) {
        return;
    }
    
    for (auto& partition : topicIt->second.partitions) {
        if (partition->partitionId == partitionId) {
            partition->messageCount = count;
            break;
        }
    }
}

void MetadataService::setController(int brokerId) {
    MetadataLockGuard lock(&metadataMutex);
    clusterMetadata.controllerBrokerId = brokerId;
    std::cout << "[METADATA] Controller set to Broker " << brokerId << std::endl;
}

int MetadataService::getController() {
    MetadataLockGuard lock(&metadataMutex);
    return clusterMetadata.controllerBrokerId;
}

bool MetadataService::isController(int brokerId) {
    MetadataLockGuard lock(&metadataMutex);
    return clusterMetadata.controllerBrokerId == brokerId;
}

bool MetadataService::isClusterHealthy() {
    MetadataLockGuard lock(&metadataMutex);
    for (const auto& pair : clusterMetadata.brokers) {
        if (!pair.second.isAlive()) {
            return false;
        }
    }
    return !clusterMetadata.brokers.empty();
}

std::vector<std::string> MetadataService::getUnhealthyBrokers() {
    MetadataLockGuard lock(&metadataMutex);
    std::vector<std::string> unhealthy;
    for (const auto& pair : clusterMetadata.brokers) {
        if (!pair.second.isAlive()) {
            unhealthy.push_back("Broker " + std::to_string(pair.first));
        }
    }
    return unhealthy;
}

int MetadataService::getTotalMessageCount() {
    MetadataLockGuard lock(&metadataMutex);
    int total = 0;
    for (auto& topicPair : clusterMetadata.topics) {
        for (auto& partition : topicPair.second.partitions) {
            total += partition->messageCount;
        }
    }
    return total;
}

int MetadataService::getTotalPartitionCount() {
    MetadataLockGuard lock(&metadataMutex);
    int total = 0;
    for (const auto& pair : clusterMetadata.topics) {
        total += pair.second.partitionCount;
    }
    return total;
}

std::string MetadataService::getClusterSummary() {
    MetadataLockGuard lock(&metadataMutex);
    std::stringstream ss;
    ss << "=== Cluster Summary ===" << std::endl;
    ss << "Brokers: " << clusterMetadata.brokers.size() << " (";
    ss << getActiveBrokers().size() << " active)" << std::endl;
    ss << "Topics: " << clusterMetadata.topics.size() << std::endl;
    ss << "Partitions: " << getTotalPartitionCount() << std::endl;
    ss << "Messages: " << getTotalMessageCount() << std::endl;
    ss << "Controller: Broker " << clusterMetadata.controllerBrokerId << std::endl;
    return ss.str();
}

std::string MetadataService::getFullMetadata() {
    MetadataLockGuard lock(&metadataMutex);
    std::stringstream ss;
    ss << "=== Full Cluster Metadata ===" << std::endl;
    ss << "Last Update: " << std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - clusterMetadata.lastUpdate).count() 
       << "s ago" << std::endl;
    
    ss << "\n--- Brokers ---" << std::endl;
    for (const auto& pair : clusterMetadata.brokers) {
        ss << "  " << pair.second.toString();
        if (!pair.second.isAlive()) ss << " (DEAD)";
        ss << std::endl;
    }
    
    ss << "\n--- Topics ---" << std::endl;
    for (const auto& pair : clusterMetadata.topics) {
        ss << "  " << pair.second.toString() << std::endl;
    }
    
    return ss.str();
}

void MetadataService::saveToDisk() {
    std::ofstream file(metadataFilePath);
    if (!file.is_open()) return;
    
    file << "=== Cluster Metadata ===" << std::endl;
    file << "Controller: " << clusterMetadata.controllerBrokerId << std::endl;
    file << "Brokers: " << clusterMetadata.brokers.size() << std::endl;
    file << "Topics: " << clusterMetadata.topics.size() << std::endl;
    file.close();
}

void MetadataService::loadFromDisk() {
    std::ifstream file(metadataFilePath);
    if (!file.is_open()) return;
    file.close();
}

void MetadataService::persistMetadata() {
    saveToDisk();
}

void MetadataService::recoverMetadata() {
    loadFromDisk();
}