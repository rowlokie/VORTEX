#include "Broker.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <cstring>

#ifndef _WIN32
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
    std::cout << "Broker started on port " << port << "\n";

    while (running) {
        sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);
        SOCKET_TYPE clientSocket = accept(server_fd, (struct sockaddr*)&clientAddr, &clientLen);
        if (clientSocket == INVALID_SOCKET_VAL) {
            if (!running) break;
            std::cerr << "Accept failed.\n";
            continue;
        }

        // Spin up a thread to handle client
#ifdef _WIN32
        ThreadData* data = new ThreadData{this, clientSocket};
        HANDLE hThread = CreateThread(NULL, 0, ClientThreadProc, data, 0, NULL);
        if (hThread) {
            CloseHandle(hThread); // Detach by closing handle immediately
        } else {
            std::cerr << "Failed to create client thread.\n";
            delete data;
            CLOSE_SOCKET(clientSocket);
        }
#else
        std::thread(&Broker::handleClientThread, this, clientSocket).detach();
#endif
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

void Broker::handleClientThread(Broker* broker, SOCKET_TYPE clientSocket) {
    broker->handleClient(clientSocket);
}

#ifdef _WIN32
DWORD WINAPI Broker::ClientThreadProc(LPVOID lpParam) {
    ThreadData* data = static_cast<ThreadData*>(lpParam);
    data->broker->handleClient(data->clientSocket);
    delete data;
    return 0;
}
#endif

void Broker::handleClient(SOCKET_TYPE clientSocket) {
    char buffer[1024];
    while (true) {
        std::memset(buffer, 0, sizeof(buffer));
        int bytesReceived = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (bytesReceived <= 0) {
            break;
        }

        std::string request(buffer);
        while (!request.empty() && (request.back() == '\r' || request.back() == '\n' || request.back() == ' ')) {
            request.pop_back();
        }

        if (request.empty()) {
            continue;
        }

        std::stringstream ss(request);
        std::string action;
        ss >> action;

        std::string response = "ERROR\n";

        if (action == "CREATE_TOPIC") {
            std::string topic;
            int partitions = 1;
            ss >> topic >> partitions;

            if (!topic.empty() && topicManager.createTopic(topic, partitions)) {
                std::cout << "[CREATE_TOPIC] topic=" << topic << " partitions=" << partitions << std::endl;
                response = "OK\n";
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

            int partitionId;
            long offset = topicManager.appendMessage(topic, key, message, partitionId);

            if (offset != -1) {
                response = "{\"status\":\"OK\",\"partition\":" + std::to_string(partitionId) + ",\"offset\":" + std::to_string(offset) + "}\n";
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
                }
            }
        } 
        else if (action == "GET_OFFSET") {
            std::string topic;
            std::string consumerId;
            int partition;
            if (ss >> topic >> consumerId >> partition) {
                long offset = topicManager.getOffset(topic, consumerId, partition);
                response = "{\"offset\":" + std::to_string(offset) + "}\n";
            }
        }

        send(clientSocket, response.c_str(), response.length(), 0);
    }  // ← THIS closes the while loop
    CLOSE_SOCKET(clientSocket);
}  // ← THIS closes the handleClient function

