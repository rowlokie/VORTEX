// broker/Metadata.h
#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <chrono>

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

struct PartitionMetadata {
    int partitionId;
    std::string filePath;
    std::atomic<long> nextOffset;
    PlatformMutex partitionMutex;
    
    // Metadata service fields
    int leaderBrokerId;
    std::vector<int> replicas;
    std::vector<int> isr;
    long startOffset;
    long endOffset;
    int messageCount;

    PartitionMetadata()
        : partitionId(0), nextOffset(1), leaderBrokerId(-1), 
          startOffset(0), endOffset(0), messageCount(0) {
#ifdef _WIN32
        InitializeCriticalSection(&partitionMutex);
#endif
    }

    ~PartitionMetadata() {
#ifdef _WIN32
        DeleteCriticalSection(&partitionMutex);
#endif
    }

    // Not copyable (mutex prevents copying)
    PartitionMetadata(const PartitionMetadata&) = delete;
    PartitionMetadata& operator=(const PartitionMetadata&) = delete;
    
    std::string toString() const {
        std::string result = "Partition " + std::to_string(partitionId) + 
                            " [Leader: " + std::to_string(leaderBrokerId) + "]";
        result += " ISR: [";
        for (size_t i = 0; i < isr.size(); i++) {
            if (i > 0) result += ", ";
            result += std::to_string(isr[i]);
        }
        result += "]";
        result += " Messages: " + std::to_string(messageCount);
        return result;
    }
};

// Copyable version for metadata queries (no mutex)
struct PartitionMetadataInfo {
    int partitionId;
    int leaderBrokerId;
    std::vector<int> replicas;
    std::vector<int> isr;
    long startOffset;
    long endOffset;
    int messageCount;
    
    PartitionMetadataInfo() 
        : partitionId(-1), leaderBrokerId(-1), 
          startOffset(0), endOffset(0), messageCount(0) {}
    
    // Convert from PartitionMetadata
    PartitionMetadataInfo(const PartitionMetadata& other) 
        : partitionId(other.partitionId),
          leaderBrokerId(other.leaderBrokerId),
          replicas(other.replicas),
          isr(other.isr),
          startOffset(other.startOffset),
          endOffset(other.endOffset),
          messageCount(other.messageCount) {}
    
    std::string toString() const {
        std::string result = "Partition " + std::to_string(partitionId) + 
                            " [Leader: " + std::to_string(leaderBrokerId) + "]";
        result += " ISR: [";
        for (size_t i = 0; i < isr.size(); i++) {
            if (i > 0) result += ", ";
            result += std::to_string(isr[i]);
        }
        result += "]";
        result += " Messages: " + std::to_string(messageCount);
        return result;
    }
};

struct TopicMetadata {
    std::string topicName;
    int partitionCount;
    int replicationFactor;
    std::vector<std::shared_ptr<PartitionMetadata>> partitions;
    std::chrono::steady_clock::time_point createdAt;
    
    TopicMetadata() : partitionCount(0), replicationFactor(1) {
        createdAt = std::chrono::steady_clock::now();
    }
    
    TopicMetadata(const std::string& name, int count, int factor) 
        : topicName(name), partitionCount(count), replicationFactor(factor) {
        createdAt = std::chrono::steady_clock::now();
    }
    
    std::string toString() const {
        std::string result = "Topic: " + topicName + 
                            " (Partitions: " + std::to_string(partitionCount) + 
                            ", Replication: " + std::to_string(replicationFactor) + ")";
        for (const auto& partition : partitions) {
            result += "\n  " + partition->toString();
        }
        return result;
    }
};