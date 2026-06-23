#include "Producer.h"
#include <iostream>
#include <cstring>
#include <sstream>

#ifdef _WIN32
#define platform_sleep(ms) Sleep(ms)
#else
#include <unistd.h>
#define platform_sleep(ms) usleep((ms) * 1000)
#endif

Producer::Producer(const std::string& host, int port)
    : host(host), port(port), clientSocket(INVALID_SOCKET_VAL), connected(false) {}

Producer::~Producer() {
    disconnect();
}

bool Producer::connectToServer() {
    if (connected) return true;

#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "[SDK] WSAStartup failed.\n";
        return false;
    }
#endif

    struct addrinfo hints, *res = nullptr;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) {
        std::cerr << "[SDK] Host resolution failed: " << host << "\n";
#ifdef _WIN32
        WSACleanup();
#endif
        return false;
    }

    clientSocket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (clientSocket == INVALID_SOCKET_VAL) {
        std::cerr << "[SDK] Failed to create socket.\n";
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
    std::cout << "[SDK] Connected to broker at " << host << ":" << port << "\n";
    return true;
}

void Producer::disconnect() {
    if (connected) {
        if (clientSocket != INVALID_SOCKET_VAL) {
            CLOSE_SOCKET(clientSocket);
            clientSocket = INVALID_SOCKET_VAL;
        }
        connected = false;
        std::cout << "[SDK] Disconnected from broker.\n";
#ifdef _WIN32
        WSACleanup();
#endif
    }
}

bool Producer::createTopic(
    const std::string& topic,
    int partitions
) {
    int retries = 3;
    while (retries > 0) {
        if (!connected) {
            if (!connectToServer()) {
                retries--;
                std::cerr << "[SDK] Connection failed. Retrying in 500ms... (" << retries << " retries left)\n";
                platform_sleep(500);
                continue;
            }
        }

       std::string payload =
    "CREATE_TOPIC " +
    topic +
    " " +
    std::to_string(partitions)
    + "\n";
        int sentBytes = ::send(clientSocket, payload.c_str(), (int)payload.length(), 0);
        if (sentBytes <= 0) {
            std::cerr << "[SDK] Write failed. Attempting reconnect...\n";
            disconnect();
            retries--;
            platform_sleep(500);
            continue;
        }

        char buffer[1024];
        std::memset(buffer, 0, sizeof(buffer));
        int receivedBytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (receivedBytes <= 0) {
            std::cerr << "[SDK] Read failed. Attempting reconnect...\n";
            disconnect();
            retries--;
            platform_sleep(500);
            continue;
        }

        std::string response(buffer);
        while (!response.empty() && (response.back() == '\n' || response.back() == '\r' || response.back() == ' ')) {
            response.pop_back();
        }

        if (response == "OK") {
            return true;
        } else {
            std::cerr << "[SDK] Broker error response: " << response << "\n";
            return false;
        }
    }
    return false;
}

int Producer::send(const std::string& topic, const std::string& key,const std::string& message) {
    int retries = 3;
    while (retries > 0) {
        if (!connected) {
            if (!connectToServer()) {
                retries--;
                std::cerr << "[SDK] Connection failed. Retrying in 500ms... (" << retries << " retries left)\n";
                platform_sleep(500);
                continue;
            }
        }

        std::string payload =
    "PRODUCE " +
    topic +
    " " +
    key+
    " " +
    message +
    "\n";
        int sentBytes = ::send(clientSocket, payload.c_str(), (int)payload.length(), 0);
        if (sentBytes <= 0) {
            std::cerr << "[SDK] Write failed. Attempting reconnect...\n";
            disconnect();
            retries--;
            platform_sleep(500);
            continue;
        }

        char buffer[1024];
        std::memset(buffer, 0, sizeof(buffer));
        int receivedBytes = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        if (receivedBytes <= 0) {
            std::cerr << "[SDK] Read failed. Attempting reconnect...\n";
            disconnect();
            retries--;
            platform_sleep(500);
            continue;
        }

        std::string response(buffer);
        while (!response.empty() && (response.back() == '\n' || response.back() == '\r' || response.back() == ' ')) {
            response.pop_back();
        }

        // Lightweight JSON parse for {"status":"OK","offset":<offset>}
        std::string prefix = "\"offset\":";
        size_t pos = response.find(prefix);
        if (pos != std::string::npos) {
            size_t start = pos + prefix.length();
            while (start < response.length() && (response[start] == ' ' || response[start] == ':')) {
                start++;
            }
            size_t end = start;
            while (end < response.length() && std::isdigit(response[end])) {
                end++;
            }
            try {
                return std::stoi(response.substr(start, end - start));
            } catch (...) {
                std::cerr << "[SDK] Failed to parse offset from response: " << response << "\n";
                return -1;
            }
        } else {
            std::cerr << "[SDK] Broker non-JSON or error response: " << response << "\n";
            return -1;
        }
    }
    return -1;
}

std::vector<int> Producer::sendBatch(
    const std::string& topic,
    const std::string& key,
    const std::vector<std::string>& messages
)
{
    std::vector<int> offsets;

    for(const auto& msg : messages)
    {
        offsets.push_back(
            send(
                topic,
                key,
                msg
            )
        );
    }

    return offsets;
}
