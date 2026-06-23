#pragma once

#include <string>
#include <memory>
#include <vector>

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

// std::atomic<long> is safe with MinGW win32 threads — it uses intrinsics, not pthreads
#include <atomic>

struct PartitionMetadata {
    int partitionId;
    std::string filePath;
    std::atomic<long> nextOffset;
    PlatformMutex partitionMutex; // cross-platform mutex

    PartitionMetadata()
        : partitionId(0), nextOffset(1)
    {
#ifdef _WIN32
        InitializeCriticalSection(&partitionMutex);
#endif
    }

    ~PartitionMetadata() {
#ifdef _WIN32
        DeleteCriticalSection(&partitionMutex);
#endif
    }

    // Not copyable (mutex + atomic are not copyable)
    PartitionMetadata(const PartitionMetadata&) = delete;
    PartitionMetadata& operator=(const PartitionMetadata&) = delete;
};

struct TopicMetadata {
    std::string topicName;
    int partitionCount;
    std::vector<std::shared_ptr<PartitionMetadata>> partitions;
};