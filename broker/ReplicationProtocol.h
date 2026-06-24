// broker/ReplicationProtocol.h
#pragma once

#include <string>
#include <vector>

// Replication message types
enum class ReplicationMessageType {
    REPLICATE_REQUEST,
    REPLICATE_RESPONSE,
    FETCH_REQUEST,
    FETCH_RESPONSE,
    LEADER_ELECTION,
    HEARTBEAT
};

// Request from leader to follower
struct ReplicateRequest {
    std::string topic;
    int partition;
    long offset;
    std::string message;
    int leaderBrokerId;
    
    std::string serialize() const {
        return "REPLICATE|" + topic + "|" + std::to_string(partition) + "|" + 
               std::to_string(offset) + "|" + std::to_string(leaderBrokerId) + "|" + message;
    }
    
    static ReplicateRequest deserialize(const std::string& data) {
        ReplicateRequest req;
        // Parse format: REPLICATE|topic|partition|offset|leaderId|message
        size_t pos = 0;
        size_t end = data.find('|', pos);
        // Skip "REPLICATE"
        pos = end + 1;
        
        end = data.find('|', pos);
        req.topic = data.substr(pos, end - pos);
        pos = end + 1;
        
        end = data.find('|', pos);
        req.partition = std::stoi(data.substr(pos, end - pos));
        pos = end + 1;
        
        end = data.find('|', pos);
        req.offset = std::stol(data.substr(pos, end - pos));
        pos = end + 1;
        
        end = data.find('|', pos);
        req.leaderBrokerId = std::stoi(data.substr(pos, end - pos));
        pos = end + 1;
        
        req.message = data.substr(pos);
        return req;
    }
};

// Response from follower
struct ReplicateResponse {
    bool success;
    long offset;
    std::string error;
    
    std::string serialize() const {
        return "REPLICATE_RESPONSE|" + std::to_string(success) + "|" + 
               std::to_string(offset) + "|" + error;
    }
};

// Heartbeat between brokers
struct BrokerHeartbeat {
    int brokerId;
    std::string host;
    int port;
    long timestamp;
    
    std::string serialize() const {
        return "HEARTBEAT|" + std::to_string(brokerId) + "|" + 
               host + "|" + std::to_string(port) + "|" + std::to_string(timestamp);
    }
};