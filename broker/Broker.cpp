#include "Broker.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <cstring>
#include <unordered_map>
#ifdef _WIN32
#include "mingw.thread.h"
#else
#include <thread>
#endif


class StatsLockGuard {
    PlatformMutex* mtx;
public:
    StatsLockGuard(PlatformMutex* m) : mtx(m) {
#ifdef _WIN32
        EnterCriticalSection(mtx);
#else
        mtx->lock();
#endif
    }
    ~StatsLockGuard() {
#ifdef _WIN32
        LeaveCriticalSection(mtx);
#else
        mtx->unlock();
#endif
    }
};

Broker::Broker(int port)
    : port(port), server_fd(INVALID_SOCKET_VAL), running(false), messagesProduced(0), messagesConsumed(0) {
#ifdef _WIN32
    InitializeCriticalSection(&statsMutex);
#endif
}

Broker::~Broker() {
    stop();
#ifdef _WIN32
    DeleteCriticalSection(&statsMutex);
#endif
}

bool Broker::start() {
#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed.\n";
        return false;
    }
#endif

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == INVALID_SOCKET_VAL) {
        std::cerr << "Failed to create socket.\n";
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    int opt = 1;
#ifdef _WIN32
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#else
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

    sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) == SOCKET_ERROR_VAL) {
        std::cerr << "Bind failed.\n";
        CLOSE_SOCKET(server_fd);
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    if (listen(server_fd, 10) == SOCKET_ERROR_VAL) {
        std::cerr << "Listen failed.\n";
        CLOSE_SOCKET(server_fd);
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    running = true;
    
    // ✅ CHANGE THIS LINE - Show "BROKER IS ON FIRE!" instead
    std::cout << "🔥🔥🔥 BROKER IS ON FIRE! 🔥🔥🔥" << std::endl;
    std::cout << "🔥 Listening on port " << port << " 🔥" << std::endl;
    std::cout.flush();

    while (running) {
        sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);
        SOCKET_TYPE clientSocket = accept(server_fd, (struct sockaddr*)&clientAddr, &clientLen);
        
        if (clientSocket == INVALID_SOCKET_VAL) {
            if (!running) break;
            std::cerr << "Accept failed.\n";
            continue;
        }

        std::cout << "🔥🔥🔥 NEW CLIENT ACCEPTED! 🔥🔥🔥" << std::endl;
        std::cout.flush();

        // Use std::thread on all platforms
        std::thread clientThread([this, clientSocket]() {
            std::cout << "🔥 CLIENT THREAD STARTED!" << std::endl;
            std::cout.flush();
            this->handleClient(clientSocket);
        });
        clientThread.detach();
    }

    return true;
}

void Broker::stop() {
    if (running) {
        running = false;
        if (server_fd != INVALID_SOCKET_VAL) {
            CLOSE_SOCKET(server_fd);
            server_fd = INVALID_SOCKET_VAL;
        }
#ifdef _WIN32
        WSACleanup();
#endif
    }
}

void Broker::handleClient(SOCKET_TYPE clientSocket) {
    std::cout << "🔥🔥🔥 handleClient() EXECUTING! 🔥🔥🔥" << std::endl;
    std::cout.flush();
    
    char buffer[4096];
    while (true) {
        std::memset(buffer, 0, sizeof(buffer));
        int bytesReceived = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (bytesReceived <= 0) {
            break;
        }
        
        std::string request(buffer);

        std::cout << "[BROKER] ========================================" << std::endl;
        std::cout << "[BROKER] RAW REQUEST: '" << request << "'" << std::endl;
        std::cout << "[BROKER] REQUEST LENGTH: " << request.length() << std::endl;
        std::cout << "[BROKER] REQUEST HEX: ";

        for (char c : request) {
            std::cout << std::hex << (int)(unsigned char)c << " ";
        }
        std::cout << std::dec << std::endl;
        std::cout << "[BROKER] ========================================" << std::endl;

        // Trim whitespace and newlines
        while (!request.empty() && (request.back() == '\r' || request.back() == '\n' || request.back() == ' ')) {
            request.pop_back();
        }

        if (request.empty()) {
            continue;
        }

        std::cout << "[DEBUG] Received: " << request << std::endl;

        std::stringstream ss(request);
        std::string action;
        ss >> action;

        std::cout << "[BROKER] PARSED ACTION: '" << action << "'" << std::endl;
        std::cout << "[BROKER] ACTION LENGTH: " << action.length() << std::endl;

        std::string response;

        if (action == "CREATE_TOPIC") {
            std::string topic;
            int partitions = 1;
            ss >> topic >> partitions;

            std::cout << "[DEBUG] CREATE_TOPIC: topic=" << topic << ", partitions=" << partitions << std::endl;

            if (!topic.empty() && topicManager.createTopic(topic, partitions)) {
                std::cout << "[CREATE_TOPIC] topic=" << topic << " partitions=" << partitions << std::endl;
                response = "OK\n";
            } else {
                response = "ERROR: Failed to create topic\n";
            }
        } 
        else if (action == "METADATA") {
            std::cout << "[BROKER] ✅✅✅ METADATA DETECTED! ✅✅✅" << std::endl;
            std::string topic;
            ss >> topic;
            std::cout << "Topic received in metadata: " << topic << std::endl;
            
            std::string metadataResponse;
            if (topic.empty()) {
                std::cout << "Topic is empty - getting all metadata" << std::endl;
                metadataResponse = topicManager.getAllTopicsMetadata();
            } else {
                std::cout << "Getting metadata for topic: " << topic << std::endl;
                metadataResponse = topicManager.getTopicMetadata(topic);
            }
            
            // Make sure response ends with newline
            response = metadataResponse;
            if (response.empty() || response.back() != '\n') {
                response += "\n";
            }
        }
        else if (action == "PRODUCE") {
            std::string topic;
            std::string key;
            ss >> topic >> key;

            std::string message;
            std::getline(ss, message);

            if (!message.empty() && message[0] == ' ') {
                message = message.substr(1);
            }

            std::cout << "[DEBUG] PRODUCE: topic=" << topic << ", key=" << key << ", message=" << message << std::endl;

            int partitionId;
            long offset = topicManager.appendMessage(topic, key, message, partitionId);

            if (offset != -1) {
                response = "{\"status\":\"OK\",\"partition\":" + std::to_string(partitionId) + ",\"offset\":" + std::to_string(offset) + "}\n";
                
                {
                    StatsLockGuard statsLock(&statsMutex);
                    messagesProduced++;
                }
            } else {
                response = "ERROR: Failed to produce message\n";
            }
        } 
        else if (action == "CONSUME") {
            std::string topic;
            int partition;
            int offset;

            if (ss >> topic >> partition >> offset) {
                std::cout << "[CONSUME] topic=" << topic << " partition=" << partition << " offset=" << offset << std::endl;

                std::string messagesData = topicManager.getMessages(topic, partition, offset);
                response = messagesData + "\n";

                long count = 0;
                std::stringstream temp(messagesData);
                std::string line;
                while (std::getline(temp, line)) {
                    if (!line.empty()) count++;
                }

                {
                    StatsLockGuard statsLock(&statsMutex);
                    messagesConsumed += count;
                    std::cout << "[STATS] Produced=" << messagesProduced << " Consumed=" << messagesConsumed << std::endl;
                }
            } else {
                response = "ERROR: Invalid CONSUME command\n";
            }
        } 
        else if (action == "JOIN_GROUP") {
            std::string topic, groupId, consumerId;
            if (ss >> topic >> groupId >> consumerId) {
                std::cout << "[JOIN_GROUP] topic=" << topic << ", group=" << groupId << ", consumer=" << consumerId << std::endl;
                if (topicManager.joinGroup(groupId, topic, consumerId)) {
                    response = "{\"status\":\"OK\",\"message\":\"Joined group\"}\n";
                } else {
                    response = "{\"status\":\"ERROR\",\"message\":\"Failed to join group\"}\n";
                }
            } else {
                response = "ERROR: Invalid JOIN_GROUP command\n";
            }
        }
        else if (action == "LEAVE_GROUP") {
            std::string groupId, consumerId;
            if (ss >> groupId >> consumerId) {
                if (topicManager.leaveGroup(groupId, consumerId)) {
                    response = "{\"status\":\"OK\",\"message\":\"Left group\"}\n";
                } else {
                    response = "{\"status\":\"ERROR\",\"message\":\"Failed to leave group\"}\n";
                }
            } else {
                response = "ERROR: Invalid LEAVE_GROUP command\n";
            }
        }
        else if (action == "POLL") {
            std::string groupId, consumerId;
            if (ss >> groupId >> consumerId) {
                std::cout << "[POLL] group=" << groupId << ", consumer=" << consumerId << std::endl;
                response = topicManager.getMessagesForConsumer(groupId, consumerId) + "\n";
            } else {
                response = "ERROR: Invalid POLL command\n";
            }
        }
        else if (action == "ASSIGNMENTS") {
            std::string groupId, consumerId;
            if (ss >> groupId >> consumerId) {
                auto partitions = topicManager.getConsumerPartitions(groupId, consumerId);
                
                std::stringstream assignmentResp;
                assignmentResp << "{\"consumer\":\"" << consumerId 
                              << "\",\"group\":\"" << groupId 
                              << "\",\"partitions\":[";
                
                for (size_t i = 0; i < partitions.size(); i++) {
                    if (i > 0) assignmentResp << ",";
                    assignmentResp << partitions[i];
                }
                assignmentResp << "]}";
                
                response = assignmentResp.str() + "\n";
            } else {
                response = "ERROR: Invalid ASSIGNMENTS command\n";
            }
        }
        else if (action == "COMMIT") {
            std::string topic;
            std::string consumerId;
            int partition;
            long offset;
            if (ss >> topic >> consumerId >> partition >> offset) {
                if (topicManager.commitOffset(topic, consumerId, partition, offset)) {
                    response = "{\"status\":\"OK\"}\n";
                } else {
                    response = "{\"status\":\"ERROR\",\"message\":\"Failed to commit offset\"}\n";
                }
            } else {
                response = "ERROR: Invalid COMMIT command\n";
            }
        } 
        else if (action == "GET_OFFSET") {
            std::string topic;
            std::string consumerId;
            int partition;
            if (ss >> topic >> consumerId >> partition) {
                long offset = topicManager.getOffset(topic, consumerId, partition);
                response = "{\"offset\":" + std::to_string(offset) + "}\n";
            } else {
                response = "ERROR: Invalid GET_OFFSET command\n";
            }
        }
        else if (action == "CLUSTER_STATUS") {
            response = topicManager.getClusterStatus();
            if (response.empty() || response.back() != '\n') {
                response += "\n";
            }
        }
        else if (action == "BROKER_LIST") {
            response = topicManager.getBrokerList();
            if (response.empty() || response.back() != '\n') {
                response += "\n";
            }
        }
        else {
            response = "ERROR: Unknown command: " + action + "\n";
        }

        // Send response
        std::cout << "[DEBUG] Sending response: " << response;
        send(clientSocket, response.c_str(), response.length(), 0);
    }
    
    CLOSE_SOCKET(clientSocket);
}