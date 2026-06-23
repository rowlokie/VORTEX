#include "TopicManager.h"
#include <fstream>
#include <iostream>
#include <ctime>
#include <algorithm>
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

TopicManager::TopicManager() {
#ifdef _WIN32
    InitializeCriticalSection(&topicMutex);
#endif
    make_dir("data");
    make_dir("data/offsets");
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