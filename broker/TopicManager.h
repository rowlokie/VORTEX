#ifndef TOPIC_MANAGER_H
#define TOPIC_MANAGER_H

#include <string>
#include <unordered_map>

// Platform guard must come BEFORE Metadata.h so PlatformMutex is defined
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

#include "Metadata.h"

class TopicManager {
private:
    PlatformMutex topicMutex;
    std::unordered_map<std::string, int> nextOffsets;
    std::unordered_map<std::string, TopicMetadata> topicsMetadata;

int getPartition(
    const std::string& topic,
    const std::string& key
);

    int getNextOffset(const std::string& topic); // private helper

public:
    TopicManager();
    void recover();
    ~TopicManager();
   
    bool createTopic(
    const std::string& topic,
    int partitionCount
);

long appendMessage(
    const std::string& topic,
    const std::string& key,
    const std::string& message,
    int& partitionId
);// Returns assigned offset, or -1 on error
    std::string getMessages(const std::string& topic, int partition, int afterOffset); // Returns records after the given offset for a specific partition

bool commitOffset(const std::string& topic, const std::string& consumerId, int partition, long offset); // Persist per-partition consumer offset
long getOffset(const std::string& topic, const std::string& consumerId, int partition); // Retrieve per-partition consumer offset
};
#endif // TOPIC_MANAGER_H
