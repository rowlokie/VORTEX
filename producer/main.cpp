#include "Producer.h"
#include <iostream>
#include <vector>
#include <string>

int main() {
    std::cout << "Starting Producer client...\n";
    Producer producer("localhost", 9092);

    // 1. Create a topic
    std::cout << "\nCreating topic 'orders'...\n";
    if (producer.createTopic("orders", 3)) {
        std::cout << "Topic 'orders' created successfully!\n";
    } else {
        std::cerr << "Failed to create topic 'orders'\n";
    }

    // 2. Send single messages
    std::cout << "\nSending individual messages...\n";
    int offset1 = producer.send("orders", "user123", "order_1");
    if (offset1 != -1) {
        std::cout << "Successfully sent 'order_1'. Assigned Offset: " << offset1 << "\n";
    } else {
        std::cerr << "Failed to send 'order_1'\n";
    }

    int offset2 =
    producer.send(
        "orders",
        "user123",
        "order_2"
    );
    if (offset2 != -1) {
        std::cout << "Successfully sent 'order_2'. Assigned Offset: " << offset2 << "\n";
    } else {
        std::cerr << "Failed to send 'order_2'\n";
    }

    // 3. Send a batch of messages
    std::cout << "\nSending a batch of messages...\n";
    std::vector<std::string> batch = {
        "order_3_batch",
        "order_4_batch",
        "order_5_batch"
    };

    std::vector<int> offsets = producer.sendBatch("orders","user123", batch);
    for (size_t i = 0; i < batch.size(); ++i) {
        if (offsets[i] != -1) {
            std::cout << "Batch message [" << batch[i] << "] -> Assigned Offset: " << offsets[i] << "\n";
        } else {
            std::cerr << "Batch message [" << batch[i] << "] -> Failed to send\n";
        }
    }

    std::cout << "\nProducer client finished.\n";
    return 0;
}
