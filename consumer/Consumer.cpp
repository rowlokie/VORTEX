#include "Consumer.h"
#include <iostream>
#include <cstring>
#include <sstream>
#include <map>

Consumer::Consumer(const std::string& host, int port)
    : host(host), port(port), clientSocket(INVALID_SOCKET_VAL), connected(false) {}

Consumer::~Consumer() {
    disconnect();
}

// ---------------- CONNECTION ----------------

bool Consumer::connectToServer() {
    if (connected) return true;

#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "[Consumer SDK] WSAStartup failed.\n";
        return false;
    }
#endif

    struct addrinfo hints, *res = nullptr;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string portStr = std::to_string(port);

    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) {
        std::cerr << "[Consumer SDK] Host resolution failed.\n";
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    clientSocket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (clientSocket == INVALID_SOCKET_VAL) {
        freeaddrinfo(res);
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    if (connect(clientSocket, res->ai_addr, (int)res->ai_addrlen) == SOCKET_ERROR_VAL) {
        CLOSE_SOCKET(clientSocket);
        freeaddrinfo(res);
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    freeaddrinfo(res);
    connected = true;

    std::cout << "[Consumer SDK] Connected to broker at " << host << ":" << port << "\n";
    return true;
}

void Consumer::disconnect() {
    if (!connected) return;

    if (clientSocket != INVALID_SOCKET_VAL) {
        CLOSE_SOCKET(clientSocket);
        clientSocket = INVALID_SOCKET_VAL;
    }

    connected = false;

#ifdef _WIN32
    WSACleanup();
#endif

    std::cout << "[Consumer SDK] Disconnected from broker.\n";
}

// ---------------- OFFSET (STILL GLOBAL API, BUT BROKER MUST STORE PER PARTITION) ----------------

long Consumer::getCommittedOffset(const std::string& topic, const std::string& consumerId) {
    if (!connected && !connectToServer()) return 0;

    std::string payload = "GET_OFFSET " + topic + " " + consumerId + "\n";

    if (::send(clientSocket, payload.c_str(), (int)payload.size(), 0) <= 0) {
        disconnect();
        return 0;
    }

    char buffer[1024];
    std::memset(buffer, 0, sizeof(buffer));

    int bytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) {
        disconnect();
        return 0;
    }

    std::string response(buffer);

    // NOTE: still returns single value (can be extended later)
    size_t pos = response.find("\"offset\":");
    if (pos != std::string::npos) {
        pos += 10;
        while (pos < response.size() && (response[pos] == ' ' || response[pos] == ':')) pos++;

        size_t end = pos;
        while (end < response.size() && isdigit(response[end])) end++;

        return std::stol(response.substr(pos, end - pos));
    }

    return 0;
}

// ---------------- COMMIT (STILL GLOBAL API, BUT SHOULD BE EXTENDED LATER) ----------------

bool Consumer::commitOffset(const std::string& topic,
                             const std::string& consumerId,
                             long offset) {
    if (!connected && !connectToServer()) return false;

    std::string payload =
        "COMMIT " + topic + " " + consumerId + " " + std::to_string(offset) + "\n";

    if (::send(clientSocket, payload.c_str(), (int)payload.size(), 0) <= 0) {
        disconnect();
        return false;
    }

    char buffer[1024];
    std::memset(buffer, 0, sizeof(buffer));

    int bytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) {
        disconnect();
        return false;
    }

    std::string response(buffer);
    return response.find("\"status\":\"OK\"") != std::string::npos;
}

// ---------------- MAIN FIX: PARTITION-AWARE POLL ----------------

std::vector<Record> Consumer::poll(
    const std::string& topic,
    const std::map<int, int>& partitionOffsets
) {
    std::vector<Record> records;

    if (!connected && !connectToServer())
        return records;

    // ✅ FIXED: Use traditional loop instead of structured binding
    for (const auto& pair : partitionOffsets) {
        int partition = pair.first;
        int offset = pair.second;

        std::string payload =
            "CONSUME " + topic + " " +
            std::to_string(partition) + " " +
            std::to_string(offset) + "\n";

        if (::send(clientSocket, payload.c_str(), (int)payload.size(), 0) <= 0) {
            disconnect();
            continue;
        }

        std::string response;
        char buffer[1024];

        while (true)
        {
            std::memset(buffer, 0, sizeof(buffer));

            int bytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
            if (bytes <= 0) {
                disconnect();
                break;
            }

            buffer[bytes] = '\0';
            response += buffer;

            if (response == "\n" ||
                (response.size() >= 2 &&
                 response.substr(response.size() - 2) == "\n\n")) {
                break;
            }
        }

        std::stringstream ss(response);
        std::string line;

        while (std::getline(ss, line))
        {
            if (line.empty()) continue;

            size_t p1 = line.find('|');
            size_t p2 = line.find('|', p1 + 1);

            if (p1 == std::string::npos || p2 == std::string::npos)
                continue;

            try {
                long off = std::stol(line.substr(0, p1));
                long ts  = std::stol(line.substr(p1 + 1, p2 - p1 - 1));
                std::string msg = line.substr(p2 + 1);

                records.push_back({off, ts, msg});
            }
            catch (...) {
                // ignore malformed
            }
        }
    }  // ← This closes the for loop

    return records;
}