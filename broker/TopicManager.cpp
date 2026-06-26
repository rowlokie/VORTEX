// TopicManager.cpp
#include "TopicManager.h"
#include "MetadataService.h"
#include "Metadata.h"
#include <fstream>
#include <iostream>
#include <ctime>
#include <algorithm>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <atomic>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#define make_dir(dir) _mkdir(dir)
#else
#include <sys/types.h>
#include <dirent.h>
#define make_dir(dir) mkdir(dir, 0777)
#endif

class LogLockGuard {
    PlatformMutex* mtx;
public:
    LogLockGuard(PlatformMutex* m) : mtx(m) {
#ifdef _WIN32
        EnterCriticalSection(mtx);
#else
        mtx->lock();
#endif
    }
    ~LogLockGuard() {
#ifdef _WIN32
        LeaveCriticalSection(mtx);
#else
        mtx->unlock();
#endif
    }
};

// ==================== Helper Function ====================

std::vector<std::string> getLogFiles(const std::string& dir) {
    std::vector<std::string> files;
    
#ifdef _WIN32
    std::string searchPath = dir + "*.log";
    WIN32_FIND_DATA findData;
    HANDLE hFind = FindFirstFile(searchPath.c_str(), &findData);
    if (hFind != INVALID_HANDLE_VALUE) {
        do {
            if (!(findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
                files.push_back(dir + findData.cFileName);
            }
        } while (FindNextFile(hFind, &findData));
        FindClose(hFind);
    }
#else
    DIR* dp = opendir(dir.c_str());
    if (dp != nullptr) {
        struct dirent* entry;
        while ((entry = readdir(dp)) != nullptr) {
            std::string filename(entry->d_name);
            if (filename.length() > 4 && filename.substr(filename.length() - 4) == ".log") {
                files.push_back(dir + filename);
            }
        }
        closedir(dp);
    }
#endif
    
    return files;
}

// ==================== Constructor / Destructor ====================

TopicManager::TopicManager(int brokerid) 
    : replicationManager(brokerid), metadataService(brokerid) {
#ifdef _WIN32
    InitializeCriticalSection(&topicMutex);
#endif
    make_dir("data");
    make_dir("data/offsets");
    make_dir("data/meta");

    std::cout << "[TopicManager] Constructor: About to call recover()" << std::endl;
    recover();
    std::cout << "[TopicManager] Constructor: recover() completed" << std::endl;
    std::cout << "[TopicManager] Constructor: topicsMetadata size = " << topicsMetadata.size() << std::endl;
}

TopicManager::~TopicManager() {
#ifdef _WIN32
    DeleteCriticalSection(&topicMutex);
#endif
}

// ==================== Private Methods ====================

int TopicManager::getNextOffset(const std::string& topic) {
    auto it = nextOffsets.find(topic);
    if (it != nextOffsets.end()) {
        return it->second;
    }

    int count = 0;
    std::string path = "data/" + topic + ".log";
    std::ifstream file(path);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            count++;
        }
        file.close();
    }
    int nextVal = count + 1;
    nextOffsets[topic] = nextVal;
    return nextVal;
}

int TopicManager::getPartition(const std::string& topic, const std::string& key) {
    auto it = topicsMetadata.find(topic);
    if (it == topicsMetadata.end())
        return 0;

    int pCount = it->second.partitionCount;
    if (pCount <= 0)
        return 0;

    // fallback for empty key
    if (key.empty()) {
        static std::atomic<int> rr{0};
        return rr++ % pCount;
    }

    return std::hash<std::string>{}(key) % pCount;
}

// ==================== Topic Management ====================

bool TopicManager::createTopic(const std::string& topic, int partitionCount) {
    if (topic.empty() || partitionCount <= 0) {
        std::cerr << "[CREATE_TOPIC] Invalid parameters: topic=" << topic 
                  << ", partitions=" << partitionCount << std::endl;
        return false;
    }

    LogLockGuard lock(&topicMutex);

    // Check if topic already exists in memory
    if (topicsMetadata.find(topic) != topicsMetadata.end()) {
        std::cerr << "[CREATE_TOPIC] Topic already exists: " << topic << std::endl;
        return false;
    }

    // Check if topic already exists in metadata service
    if (metadataService.topicExists(topic)) {
        std::cerr << "[CREATE_TOPIC] Topic already exists in metadata: " << topic << std::endl;
        return false;
    }

    // Get active brokers for partition assignment
    std::vector<int> activeBrokers = metadataService.getActiveBrokers();
    if (activeBrokers.empty()) {
        int currentBrokerId = 1;
        std::cout << "[CREATE_TOPIC] No active brokers found. Registering Broker " 
                  << currentBrokerId << std::endl;
        metadataService.registerBroker(BrokerInfo(currentBrokerId, "localhost", 9092));
        activeBrokers.push_back(currentBrokerId);
    }

    // 1. Create TopicMetadata
    TopicMetadata meta;
    meta.topicName = topic;
    meta.partitionCount = partitionCount;

    // 2. Create partition files and metadata
    std::cout << "[CREATE_TOPIC] Creating " << partitionCount 
              << " partitions for topic: " << topic << std::endl;

    for (int i = 0; i < partitionCount; i++) {
        std::string path = "data/" + topic + "-" + std::to_string(i) + ".log";
        
        std::ofstream logFile(path);
        if (!logFile.is_open()) {
            std::cerr << "[CREATE_TOPIC] Failed to create partition file: " << path << std::endl;
            return false;
        }
        logFile.close();

        auto partition = std::make_shared<PartitionMetadata>();
        partition->partitionId = i;
        partition->filePath = path;
        partition->nextOffset = 1;

        meta.partitions.push_back(partition);
        
        std::cout << "[CREATE_TOPIC] Created partition " << i 
                  << " at " << path << std::endl;
    }

    // 3. Store in memory
    topicsMetadata[topic] = std::move(meta);

    // 4. Register with Metadata Service
    if (!metadataService.createTopic(topic, partitionCount, 1)) {
        std::cerr << "[CREATE_TOPIC] Metadata service failed to create topic: " << topic << std::endl;
        topicsMetadata.erase(topic);
        return false;
    }

    // 5. Update partition metadata in Metadata Service
    for (int i = 0; i < partitionCount; i++) {
        int leaderId = activeBrokers[i % activeBrokers.size()];
        std::vector<int> replicas = {leaderId};
        
        metadataService.addPartition(topic, i, leaderId, replicas);
        metadataService.updateISR(topic, i, replicas);
        
        std::cout << "[CREATE_TOPIC] Partition " << i 
                  << " assigned to leader Broker " << leaderId << std::endl;
    }

    // 6. Save metadata to disk
    std::string metaPath = "data/meta/" + topic + ".meta";
    std::ofstream metaFile(metaPath, std::ios::trunc);
    
    if (metaFile.is_open()) {
        metaFile << "topic=" << topic << "\n";
        metaFile << "partitionCount=" << partitionCount << "\n";
        metaFile << "replicationFactor=1\n";
        metaFile << "createdAt=" << std::time(nullptr) << "\n";
        metaFile << "brokers=";
        for (size_t i = 0; i < activeBrokers.size(); i++) {
            if (i > 0) metaFile << ",";
            metaFile << activeBrokers[i];
        }
        metaFile << "\n";
        metaFile.close();
        std::cout << "[CREATE_TOPIC] Saved metadata to: " << metaPath << std::endl;
    } else {
        std::cerr << "[CREATE_TOPIC] Warning: Failed to save metadata to: " << metaPath << std::endl;
    }

    // 7. Persist metadata service to disk
    metadataService.persistMetadata();

    std::cout << "[CREATE_TOPIC] ✅ Topic '" << topic 
              << "' created successfully with " << partitionCount 
              << " partitions" << std::endl;
    
    return true;
}

// ==================== Message Operations ====================

long TopicManager::appendMessage(const std::string& topic,
                                 const std::string& key,
                                 const std::string& message,
                                 int& partitionId) {
    LogLockGuard lock(&topicMutex);

    auto topicIt = topicsMetadata.find(topic);
    if (topicIt == topicsMetadata.end()) {
        return -1;
    }

    partitionId = getPartition(topic, key);

    auto partition = topicIt->second.partitions[partitionId];

    long offset = partition->nextOffset++;

    std::ofstream file(partition->filePath, std::ios::app);
    if (!file.is_open()) {
        return -1;
    }

    std::time_t ts = std::time(nullptr);
    file << offset << "|" << ts << "|" << message << "\n";
    file.close();

    // Replication: forward to followers if this broker is leader
    if (isPartitionLeader(topic, partitionId)) {
        replicateMessage(topic, partitionId, offset, message);
    }

    return offset;
}

std::string TopicManager::getMessages(const std::string& topic,
                                      int partitionId,
                                      int afterOffset) {
    if (topic.empty() ||
        topic.find("..") != std::string::npos ||
        topic.find('/') != std::string::npos ||
        topic.find('\\') != std::string::npos) {
        return "";
    }

    LogLockGuard lock(&topicMutex);

    auto topicIt = topicsMetadata.find(topic);
    if (topicIt == topicsMetadata.end())
        return "";

    auto& partitions = topicIt->second.partitions;

    if (partitionId < 0 || partitionId >= static_cast<int>(partitions.size())) {
        return "";
    }

    auto partition = partitions[partitionId];

    std::ifstream file(partition->filePath);
    if (!file.is_open())
        return "";

    std::string result;
    std::string line;

    while (std::getline(file, line)) {
        size_t firstPipe = line.find('|');
        if (firstPipe == std::string::npos)
            continue;

        try {
            int offset = std::stoi(line.substr(0, firstPipe));
            if (offset > afterOffset) {
                result += line;
                result += "\n";
            }
        } catch (...) {
        }
    }

    return result;
}

// ==================== Offset Management ====================

bool TopicManager::commitOffset(const std::string& topic, 
                                const std::string& consumerId, 
                                int partition, 
                                long offset) {
    if (topic.empty() || consumerId.empty() || 
        topic.find("..") != std::string::npos || 
        consumerId.find("..") != std::string::npos) {
        return false;
    }

    LogLockGuard lock(&topicMutex);
    std::string path = "data/offsets/" + topic + "_" + consumerId + 
                       "_part" + std::to_string(partition) + ".offset";
    std::ofstream file(path, std::ios::trunc);
    if (!file.is_open()) {
        return false;
    }
    file << offset << "\n";
    return file.good();
}

long TopicManager::getOffset(const std::string& topic, 
                             const std::string& consumerId, 
                             int partition) {
    if (topic.empty() || consumerId.empty() || 
        topic.find("..") != std::string::npos || 
        consumerId.find("..") != std::string::npos) {
        return 0;
    }

    LogLockGuard lock(&topicMutex);
    std::string path = "data/offsets/" + topic + "_" + consumerId + 
                       "_part" + std::to_string(partition) + ".offset";
    std::ifstream file(path);
    if (!file.is_open()) {
        return 0;
    }
    long offset = 0;
    if (file >> offset) {
        file.close();
        return offset;
    }
    file.close();
    return 0;
}

// ==================== Consumer Group Methods ====================

bool TopicManager::joinGroup(const std::string& groupId, 
                             const std::string& topic, 
                             const std::string& consumerId) {
    auto it = topicsMetadata.find(topic);
    if (it == topicsMetadata.end()) {
        std::cerr << "Topic not found: " << topic << std::endl;
        return false;
    }
    
    bool result = groupManager.joinGroup(groupId, topic, consumerId);
    
    if (result) {
        int partitionCount = it->second.partitionCount;
        groupManager.assignPartitions(groupId, partitionCount);
    }
    
    return result;
}

bool TopicManager::leaveGroup(const std::string& groupId, const std::string& consumerId) {
    return groupManager.leaveGroup(groupId, consumerId);
}

bool TopicManager::heartbeat(const std::string& groupId, const std::string& consumerId) {
    return groupManager.heartbeat(groupId, consumerId);
}

std::vector<int> TopicManager::getConsumerPartitions(const std::string& groupId, 
                                                     const std::string& consumerId) {
    return groupManager.getConsumerPartitions(groupId, consumerId);
}

std::string TopicManager::getMessagesForConsumer(const std::string& groupId, 
                                                 const std::string& consumerId) {
    std::vector<int> partitions = getConsumerPartitions(groupId, consumerId);
    
    if (partitions.empty()) {
        return "{\"error\":\"No partitions assigned to consumer " + consumerId + "\"}";
    }
    
    std::string topic = groupManager.getGroupTopic(groupId);
    if (topic.empty()) {
        return "{\"error\":\"No topic found for group " + groupId + "\"}";
    }
    
    std::stringstream response;
    response << "{\"consumer\":\"" << consumerId 
             << "\",\"group\":\"" << groupId 
             << "\",\"topic\":\"" << topic
             << "\",\"messages\":[";
    
    bool first = true;
    bool hasMessages = false;
    
    for (int partition : partitions) {
        std::string consumerKey = groupId + ":" + consumerId;
        long offset = getOffset(topic, consumerKey, partition);
        
        std::string messages = getMessages(topic, partition, offset);
        
        if (!messages.empty() && messages.find("No messages") == std::string::npos) {
            if (!first) {
                response << ",";
            }
            response << "{\"partition\":" << partition 
                     << ",\"offset\":" << offset 
                     << ",\"messages\":" << messages << "}";
            first = false;
            hasMessages = true;
        }
    }
    
    response << "]}";
    
    if (!hasMessages) {
        return "{\"consumer\":\"" + consumerId + 
               "\",\"group\":\"" + groupId + 
               "\",\"topic\":\"" + topic +
               "\",\"messages\":[],\"message\":\"No new messages\"}";
    }
    
    return response.str();
}

// ==================== Replication Methods ====================

void TopicManager::registerBroker(const BrokerInfo& broker) {
    replicationManager.registerBroker(broker);
}

void TopicManager::unregisterBroker(int brokerId) {
    replicationManager.unregisterBroker(brokerId);
}

int TopicManager::getPartitionLeader(const std::string& topic, int partition) {
    return replicationManager.getPartitionLeader(topic, partition);
}

void TopicManager::setPartitionLeader(const std::string& topic, int partition, int leaderBrokerId) {
    replicationManager.setPartitionLeader(topic, partition, leaderBrokerId);
}

void TopicManager::addFollower(const std::string& topic, int partition, int followerBrokerId) {
    replicationManager.addFollower(topic, partition, followerBrokerId);
}

bool TopicManager::isPartitionLeader(const std::string& topic, int partition) {
    return replicationManager.isLeader(topic, partition);
}

std::vector<int> TopicManager::getPartitionReplicas(const std::string& topic, int partition) {
    return replicationManager.getFollowers(topic, partition);
}

void TopicManager::replicateMessage(const std::string& topic, int partition, 
                                   long offset, const std::string& message) {
    replicationManager.replicateToFollowers(topic, partition, offset, message);
}

bool TopicManager::receiveReplication(const std::string& topic, int partition, 
                                     long offset, const std::string& message) {
    LogLockGuard lock(&topicMutex);
    
    auto it = topicsMetadata.find(topic);
    if (it == topicsMetadata.end()) {
        return false;
    }
    
    auto& partitions = it->second.partitions;
    if (partition < 0 || partition >= static_cast<int>(partitions.size())) {
        return false;
    }
    
    auto partitionPtr = partitions[partition];
    
    std::ofstream file(partitionPtr->filePath, std::ios::app);
    if (!file.is_open()) {
        return false;
    }
    
    std::time_t ts = std::time(nullptr);
    file << offset << "|" << ts << "|" << message << "\n";
    file.close();
    
    std::cout << "[REPLICATION] Follower received message for " << topic 
              << "-" << partition << " offset " << offset << std::endl;
    
    return true;
}

// ==================== Metadata Methods ====================

std::string TopicManager::getTopicMetadata(const std::string& topicName) {
    LogLockGuard lock(&topicMutex);
    
    std::cout << "[METADATA] ========== START ==========" << std::endl;
    std::cout << "[METADATA] Looking for: '" << topicName << "'" << std::endl;
    std::cout << "[METADATA] topicsMetadata size: " << topicsMetadata.size() << std::endl;
    
    // Print ALL keys in topicsMetadata
    std::cout << "[METADATA] All topics in topicsMetadata:" << std::endl;
    for (const auto& pair : topicsMetadata) {
        std::cout << "  - '" << pair.first << "' (partitions: " << pair.second.partitionCount << ")" << std::endl;
    }
    
    auto it = topicsMetadata.find(topicName);
    if (it == topicsMetadata.end()) {
        std::cout << "[METADATA] ❌ Topic NOT found!" << std::endl;
        std::cout << "[METADATA] ========== END (ERROR) ==========" << std::endl;
        return "{\"status\":\"ERROR\",\"message\":\"Topic not found: " + topicName + "\"}";
    }
    
    std::cout << "[METADATA] ✅ Topic found!" << std::endl;
    
    const TopicMetadata& meta = it->second;
    
    std::string response = "{";
    response += "\"status\":\"OK\",";
    response += "\"topic\":\"" + meta.topicName + "\",";
    response += "\"partitionCount\":" + std::to_string(meta.partitionCount) + ",";
    response += "\"partitions\":[";
    
    for (size_t i = 0; i < meta.partitions.size(); i++) {
        if (i > 0) response += ",";
        response += "{";
        response += "\"id\":" + std::to_string(meta.partitions[i]->partitionId) + ",";
        response += "\"nextOffset\":" + std::to_string(meta.partitions[i]->nextOffset.load());
        response += "}";
    }
    
    response += "]}";
    
    std::cout << "[METADATA] Response: " << response << std::endl;
    std::cout << "[METADATA] ========== END (OK) ==========" << std::endl;
    return response;
}

std::string TopicManager::getAllTopicsMetadata() {
    LogLockGuard lock(&topicMutex);
    
    if (topicsMetadata.empty()) {
        return "{\"status\":\"OK\",\"topics\":[]}";
    }
    
    std::string response = "{\"status\":\"OK\",\"topics\":[";
    
    bool first = true;
    for (const auto& pair : topicsMetadata) {
        if (!first) response += ",";
        first = false;
        
        const TopicMetadata& meta = pair.second;
        response += "{";
        response += "\"topic\":\"" + meta.topicName + "\",";
        response += "\"partitionCount\":" + std::to_string(meta.partitionCount);
        response += "}";
    }
    
    response += "]}";
    return response;
}

std::string TopicManager::getClusterStatus() {
    return metadataService.getClusterSummary();
}

std::string TopicManager::getBrokerList() {
    auto activeBrokers = metadataService.getActiveBrokers();
    
    std::stringstream response;
    response << "ACTIVE BROKERS [" << activeBrokers.size() << "]\n";
    
    for (int brokerId : activeBrokers) {
        auto info = metadataService.getBrokerInfo(brokerId);
        response << "  " << info.toString() << "\n";
    }
    
    return response.str();
}

// ==================== Recovery ====================

void TopicManager::recover() {
    LogLockGuard lock(&topicMutex);
    
    std::cout << "[TopicManager::recover] Starting recovery..." << std::endl;
    
    // Scan the data directory to find all log files
    std::map<std::string, std::set<int>> topicPartitions;
    std::string dataDir = "data/";
    std::vector<std::string> logFiles = getLogFiles(dataDir);
    
    std::cout << "[TopicManager::recover] Found " << logFiles.size() << " log files" << std::endl;
    
    for (const auto& file : logFiles) {
        std::string filename = file.substr(dataDir.length());
        size_t dashPos = filename.find('-');
        if (dashPos == std::string::npos) continue;
        
        std::string topicName = filename.substr(0, dashPos);
        size_t dotPos = filename.find('.', dashPos);
        if (dotPos == std::string::npos) continue;
        
        std::string partitionStr = filename.substr(dashPos + 1, dotPos - dashPos - 1);
        try {
            int partitionId = std::stoi(partitionStr);
            topicPartitions[topicName].insert(partitionId);
            std::cout << "[TopicManager::recover] Found: " << topicName << "-" << partitionId << std::endl;
        } catch (...) {
            continue;
        }
    }
    
    std::cout << "[TopicManager::recover] Found " << topicPartitions.size() << " topics" << std::endl;
    
    // Recover each topic
    for (const auto& topicEntry : topicPartitions) {
        const std::string& topicName = topicEntry.first;
        const std::set<int>& partitionIds = topicEntry.second;
        
        std::cout << "[TopicManager::recover] Recovering topic: " << topicName 
                  << " with " << partitionIds.size() << " partitions" << std::endl;
        
        TopicMetadata meta;
        meta.topicName = topicName;
        meta.partitionCount = partitionIds.size();
        
        for (int partitionId : partitionIds) {
            auto part = std::make_shared<PartitionMetadata>();
            part->partitionId = partitionId;
            part->filePath = "data/" + topicName + "-" + std::to_string(partitionId) + ".log";
            
            long maxOffset = -1;
            std::ifstream file(part->filePath);
            if (file.is_open()) {
                std::string line;
                while (std::getline(file, line)) {
                    size_t pos = line.find('|');
                    if (pos != std::string::npos) {
                        try {
                            long offset = std::stol(line.substr(0, pos));
                            maxOffset = std::max(maxOffset, offset);
                        } catch (...) {}
                    }
                }
                file.close();
            }
            
            part->nextOffset = maxOffset + 1;
            meta.partitions.push_back(part);
            std::cout << "[TopicManager::recover] Partition " << partitionId 
                      << " nextOffset = " << part->nextOffset << std::endl;
        }
        
        topicsMetadata[topicName] = meta;
        std::cout << "[TopicManager::recover] Added topic to topicsMetadata: " << topicName << std::endl;
    }
    
    std::cout << "[TopicManager::recover] Recovery complete. topicsMetadata size = " << topicsMetadata.size() << std::endl;
    
    // Print all topics in topicsMetadata
    std::cout << "[TopicManager::recover] Topics in metadata:" << std::endl;
    for (const auto& pair : topicsMetadata) {
        std::cout << "  - " << pair.first << " (partitions: " << pair.second.partitionCount << ")" << std::endl;
    }
}