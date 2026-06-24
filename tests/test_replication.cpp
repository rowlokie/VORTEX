// tests/test_replication.cpp
#include "../broker/TopicManager.h"
#include <iostream>
#include <thread>
#include <chrono>

void testReplication() {
    std::cout << "=== Testing Replication ===" << std::endl;
    
    // Simulate multiple brokers
    int brokerId = 1;
    TopicManager tm(brokerId);
    
    // Register brokers in cluster
    std::cout << "\n1. Setting up cluster..." << std::endl;
    
    BrokerInfo broker1(1, "localhost", 9092);
    BrokerInfo broker2(2, "localhost", 9093);
    BrokerInfo broker3(3, "localhost", 9094);
    
    tm.registerBroker(broker1);
    tm.registerBroker(broker2);
    tm.registerBroker(broker3);
    
    // Create topic with replication
    std::cout << "\n2. Creating topic 'orders' with 3 partitions..." << std::endl;
    tm.createTopic("orders", 3);
    
    // Set partition leaders (simulate election)
    std::cout << "\n3. Assigning partition leaders..." << std::endl;
    tm.setPartitionLeader("orders", 0, 1);  // Broker1 is leader
    tm.setPartitionLeader("orders", 1, 1);  // Broker1 is leader
    tm.setPartitionLeader("orders", 2, 1);  // Broker1 is leader
    
    // Add followers
    tm.addFollower("orders", 0, 2);
    tm.addFollower("orders", 0, 3);
    tm.addFollower("orders", 1, 2);
    tm.addFollower("orders", 1, 3);
    tm.addFollower("orders", 2, 2);
    tm.addFollower("orders", 2, 3);
    
    // Check leadership
    std::cout << "\n4. Checking leadership..." << std::endl;
    bool isLeader = tm.isPartitionLeader("orders", 0);
    std::cout << "Broker 1 is leader for partition 0: " << (isLeader ? "Yes" : "No") << std::endl;
    
    std::vector<int> replicas = tm.getPartitionReplicas("orders", 0);
    std::cout << "Partition 0 replicas: ";
    for (int id : replicas) std::cout << id << " ";
    std::cout << std::endl;
    
    // Produce messages (leader handles replication)
    std::cout << "\n5. Producing messages..." << std::endl;
    for (int i = 1; i <= 6; i++) {
        int partitionId;
        long offset = tm.appendMessage("orders", "", "Message " + std::to_string(i), partitionId);
        std::cout << "Message " << i << " -> partition " << partitionId 
                  << ", offset " << offset << std::endl;
    }
    
    // Read messages
    std::cout << "\n6. Reading messages from partitions..." << std::endl;
    for (int p = 0; p < 3; p++) {
        std::cout << "Partition " << p << ":" << std::endl;
        std::string msgs = tm.getMessages("orders", p, 0);
        std::cout << msgs << std::endl;
    }
    
    std::cout << "\n=== Replication Test Complete ===" << std::endl;
}

int main() {
    system("mkdir -p data");
    system("mkdir -p data/meta");
    system("mkdir -p data/offsets");
    
    testReplication();
    return 0;
}