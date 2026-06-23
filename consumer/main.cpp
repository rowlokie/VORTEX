#include "Consumer.h"
#include <iostream>
#include <vector>

int main() {
    std::cout << "Starting Consumer client...\n";
    Consumer consumer("localhost", 9092);

    std::string topic = "orders";
    std::string consumerId = "consumerA";

    // 1. Get committed offset
    long committedOffset = consumer.getCommittedOffset(topic, consumerId);
    std::cout << "[SDK] Last committed offset for " << consumerId << ": " << committedOffset << "\n";

    // 2. Poll messages starting after that offset
    std::cout << "Polling messages starting after offset " << committedOffset << "...\n\n";
    std::map<int, int> offsets;
offsets[0] = committedOffset;  // Use partition 0
std::vector<Record> messages = consumer.poll(topic, offsets);

    if (messages.empty()) {
        std::cout << "No new messages found.\n";
    } else {
        long lastOffset = committedOffset;
        for (const auto& record : messages) {
            std::cout << "Offset=" << record.offset << "\n";
            std::cout << "Timestamp=" << record.timestamp << "\n";
            std::cout << "Message=" << record.message << "\n\n";
            lastOffset = record.offset;
        }

        // 3. Commit the last offset we successfully processed
        std::cout << "Committing offset " << lastOffset << " for " << consumerId << "...\n";
        if (consumer.commitOffset(topic, consumerId, lastOffset)) {
            std::cout << "Offset committed successfully!\n";
        } else {
            std::cerr << "Failed to commit offset!\n";
        }
    }

    std::cout << "\nConsumer client finished.\n";
    return 0;
}
