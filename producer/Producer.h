#ifndef PRODUCER_H
#define PRODUCER_H

#include <string>
#include <vector>

#ifdef _WIN32
    #ifndef _WIN32_WINNT
        #define _WIN32_WINNT 0x0600 // Expose getaddrinfo / freeaddrinfo
    #endif
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <windows.h>
    #define SOCKET_TYPE SOCKET
    #define CLOSE_SOCKET(s) closesocket(s)
    #define INVALID_SOCKET_VAL INVALID_SOCKET
    #define SOCKET_ERROR_VAL SOCKET_ERROR
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <unistd.h>
    #define SOCKET_TYPE int
    #define CLOSE_SOCKET(s) close(s)
    #define INVALID_SOCKET_VAL -1
    #define SOCKET_ERROR_VAL -1
#endif

class Producer {
private:
    std::string host;
    int port;
    SOCKET_TYPE clientSocket;
    bool connected;

    bool connectToServer();
    void disconnect();

public:
    Producer(const std::string& host, int port);
    ~Producer();
    bool createTopic(
    const std::string& topic,
    int partitions
); // Returns true on success
   int send(
    const std::string& topic,
    const std::string& key,
    const std::string& message
);// Returns offset, or -1 on failure
    std::vector<int> sendBatch(
    const std::string& topic,
    const std::string& key,
    const std::vector<std::string>& messages
);
};

#endif // PRODUCER_H
