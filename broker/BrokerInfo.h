// broker/BrokerInfo.h
#pragma once

#include <string>
#include <chrono>

struct BrokerInfo {
    int brokerId;
    std::string host;
    int port;
    bool isActive;
    std::chrono::steady_clock::time_point lastHeartbeat;
    
    BrokerInfo() : brokerId(-1), host("localhost"), port(9092), isActive(true) {
        lastHeartbeat = std::chrono::steady_clock::now();
    }
    
    BrokerInfo(int id, const std::string& h, int p) 
        : brokerId(id), host(h), port(p), isActive(true) {
        lastHeartbeat = std::chrono::steady_clock::now();
    }
    
    void updateHeartbeat() {
        lastHeartbeat = std::chrono::steady_clock::now();
        isActive = true;
    }
    
    bool isAlive() const {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            now - lastHeartbeat
        );
        return elapsed.count() < 30; // 30 second timeout
    }
    
    std::string toString() const {
        return "Broker[" + std::to_string(brokerId) + "](" + host + ":" + std::to_string(port) + ")";
    }
};