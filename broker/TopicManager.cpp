#include "TopicManager.h"
#include <fstream>
#include <iostream>
#include <ctime>
#include <algorithm>
#include <sstream>
#include<vector>
#include <map>
#ifdef _WIN32
#include <direct.h>
#define make_dir(dir) _mkdir(dir)
#else
#include <sys/stat.h>
#include <sys/types.h>
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

TopicManager::TopicManager(int brokerid) 
    :replicationManager(brokerid) {
#ifdef _WIN32
    InitializeCriticalSection(&topicMutex);
#endif
    make_dir("data");
    make_dir("data/offsets");
    make_dir("data/meta");
    recover();
}

TopicManager::~TopicManager() {
#ifdef _WIN32
    DeleteCriticalSection(&topicMutex);
#endif
}

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

bool TopicManager::createTopic(
    const std::string& topic,
    int partitionCount
)
{
    LogLockGuard lock(&topicMutex);

    TopicMetadata meta;
    meta.topicName = topic;
    meta.partitionCount = partitionCount;

    // 1. Create partition files
    for (int i = 0; i < partitionCount; i++)
    {
        std::string path =
            "data/" +
            topic +
            "-" +
            std::to_string(i) +
            ".log";

        std::ofstream(path).close();

        auto partition =
            std::make_shared<PartitionMetadata>();

        partition->partitionId = i;
        partition->filePath = path;
        partition->nextOffset = 1;

        meta.partitions.push_back(partition);
    }

    // 2. Save metadata to disk (NEW PART)
    std::string metaPath = "data/meta/" + topic + ".meta";
    std::ofstream metaFile(metaPath, std::ios::trunc);

    if (metaFile.is_open())
    {
        metaFile << "partitionCount=" << partitionCount << "\n";
        metaFile.close();
    }

    // 3. Store in memory
    topicsMetadata[topic] = std::move(meta);

    return true;
}


std::string TopicManager::getMessages(
    const std::string& topic,
    int partitionId,
    int afterOffset
)
{
    if (topic.empty() ||
        topic.find("..") != std::string::npos ||
        topic.find('/') != std::string::npos ||
        topic.find('\\') != std::string::npos)
    {
        return "";
    }

    LogLockGuard lock(&topicMutex);

    auto topicIt = topicsMetadata.find(topic);
    if (topicIt == topicsMetadata.end())
        return "";

    auto& partitions = topicIt->second.partitions;

    if (partitionId < 0 ||
        partitionId >= static_cast<int>(partitions.size()))
    {
        return "";
    }

    auto partition = partitions[partitionId];

    std::ifstream file(partition->filePath);
    if (!file.is_open())
        return "";

    std::string result;
    std::string line;

    while (std::getline(file, line))
    {
        size_t firstPipe = line.find('|');
        if (firstPipe == std::string::npos)
            continue;

        try
        {
            int offset =
                std::stoi(line.substr(0, firstPipe));

            if (offset > afterOffset)
            {
                result += line;
                result += "\n";
            }
        }
        catch (...)
        {
        }
    }

    return result;
}

bool TopicManager::commitOffset(const std::string& topic, const std::string& consumerId, int partition, long offset) {
    if (topic.empty() || consumerId.empty() || topic.find("..") != std::string::npos || consumerId.find("..") != std::string::npos) {
        return false;
    }

    LogLockGuard lock(&topicMutex);
    // Include partition in the filename
    std::string path = "data/offsets/" + topic + "_" + consumerId + "_part" + std::to_string(partition) + ".offset";
    std::ofstream file(path, std::ios::trunc);
    if (!file.is_open()) {
        return false;
    }
    file << offset << "\n";
    return file.good();
}

long TopicManager::getOffset(const std::string& topic, const std::string& consumerId, int partition) {
    if (topic.empty() || consumerId.empty() || topic.find("..") != std::string::npos || consumerId.find("..") != std::string::npos) {
        return 0;
    }

    LogLockGuard lock(&topicMutex);
    // Include partition in the filename
    std::string path = "data/offsets/" + topic + "_" + consumerId + "_part" + std::to_string(partition) + ".offset";
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

int TopicManager::getPartition(
    const std::string& topic,
    const std::string& key
)
{
    auto it = topicsMetadata.find(topic);

    if (it == topicsMetadata.end())
        return 0;

    int pCount = it->second.partitionCount;

    if (pCount <= 0)
        return 0;

    // fallback for empty key
    if (key.empty())
    {
        static std::atomic<int> rr{0};
        return rr++ % pCount;
    }

    return std::hash<std::string>{}(key) % pCount;
}

long TopicManager::appendMessage(
    const std::string& topic,
    const std::string& key,
    const std::string& message,
    int& partitionId
)
{
LogLockGuard lock(&topicMutex);


    auto topicIt =
        topicsMetadata.find(topic);

    if(topicIt==topicsMetadata.end())
    {
        return -1;
    }

    partitionId =
        getPartition(
            topic,
            key
        );

    auto partition =
        topicIt
        ->second
        .partitions[partitionId];

    long offset =
        partition->nextOffset++;

    std::ofstream file(
        partition->filePath,
        std::ios::app
    );

    if(!file.is_open())
    {
        return -1;
    }

    std::time_t ts =
        std::time(nullptr);

    file
        << offset
        << "|"
        << ts
        << "|"
        << message
        << "\n";

    file.close();

    //Replication : forward to followers if this broker is leader

    if(isPartitionLeader(topic,partitionId)){
        replicateMessage(topic,partitionId,offset,message);
    }


    return offset;
}

void TopicManager::recover()
{
    std::string base = "orders"; 
    std::string path0 = "data/" + base + "-0.log";

    std::ifstream test(path0);
    if (!test.is_open()) return;

    TopicMetadata meta;
    meta.topicName = base;
    meta.partitionCount = 3;

    for (int p = 0; p < meta.partitionCount; p++)
    {
        auto part = std::make_shared<PartitionMetadata>();

        part->partitionId = p;
        part->filePath = "data/" + base + "-" + std::to_string(p) + ".log";

        long maxOffset = 0;

        std::ifstream file(part->filePath);
        std::string line;

        while (std::getline(file, line))
        {
            size_t pos = line.find('|');
            if (pos == std::string::npos) continue;

            try {
                long offset = std::stol(line.substr(0, pos));
                maxOffset = std::max(maxOffset, offset);
            }
            catch (...) {}
        }

        part->nextOffset = maxOffset + 1;

        meta.partitions.push_back(part);
    }

    topicsMetadata[base] = std::move(meta);
}


// Add these methods at the end of TopicManager.cpp

bool TopicManager::joinGroup(const std::string& groupId, 
                             const std::string& topic, 
                             const std::string& consumerId) {
    // Check if topic exists
    auto it = topicsMetadata.find(topic);
    if (it == topicsMetadata.end()) {
        std::cerr << "Topic not found: " << topic << std::endl;
        return false;
    }
    
    bool result = groupManager.joinGroup(groupId, topic, consumerId);
    
    if (result) {
        // Assign partitions for this group
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
    // Get partitions assigned to this consumer
    std::vector<int> partitions = getConsumerPartitions(groupId, consumerId);
    
    if (partitions.empty()) {
        return "{\"error\":\"No partitions assigned to consumer " + consumerId + "\"}";
    }
    
    // Get the topic for this group
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
        // Get last committed offset for this consumer in this partition
        // Use groupId:consumerId as the consumer identifier for offsets
        std::string consumerKey = groupId + ":" + consumerId;
        long offset = getOffset(topic, consumerKey, partition);
        
        // Get messages from this partition starting from the offset
        std::string messages = getMessages(topic, partition, offset);
        
        // Check if we got actual messages
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


// broker/TopicManager.cpp - Add these at the end

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
    // Follower receives replicated message - write to local storage
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
