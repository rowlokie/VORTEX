#include <iostream>
#include "broker/MetadataService.h"
#include "broker/BrokerInfo.h"
int main() {
    MetadataService svc(1);
    // Register 3 brokers
    BrokerInfo b1; b1.brokerId = 1; b1.host = "localhost"; b1.port = 9092;
    BrokerInfo b2; b2.brokerId = 2; b2.host = "localhost"; b2.port = 9093;
    BrokerInfo b3; b3.brokerId = 3; b3.host = "localhost"; b3.port = 9094;
    svc.registerBroker(b1);
    svc.registerBroker(b2);
    svc.registerBroker(b3);
    // Create a topic with broker 2 as leader of partition 0
    svc.createTopic("payments", 2);
    svc.addPartition("payments", 0, 2, {2, 3});
    svc.addPartition("payments", 1, 2, {2, 3});
    std::cout << "payments-0 leader: Broker " 
              << svc.getPartitionMetadata("payments", 0).leaderBrokerId << std::endl;
    std::cout << "payments-1 leader: Broker " 
              << svc.getPartitionMetadata("payments", 1).leaderBrokerId << std::endl;
    std::cout << ">>> Simulating Broker 2 going DOWN <<<" << std::endl;
    // Simulate broker 2 failing
    svc.unregisterBroker(2);
    std::cout << "payments-0 leader: Broker " 
              << svc.getPartitionMetadata("payments", 0).leaderBrokerId << std::endl;
    std::cout << "payments-1 leader: Broker " 
              << svc.getPartitionMetadata("payments", 1).leaderBrokerId << std::endl;
    return 0;
}
