#ifndef CONSUMER_H
#define CONSUMER_H

#include <string>
#include <vector>

#ifdef _WIN32
    #ifndef _WIN32_WINNT
        #define _WIN32_WINNT 0x0600
    #endif
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include<map>
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

struct Record {
    long offset;
    long timestamp;
    std::string message;
};

class Consumer {
private:
    std::string host;
    int port;
    SOCKET_TYPE clientSocket;
    bool connected;

    bool connectToServer();
    void disconnect();

public:
    Consumer(const std::string& host, int port);
    ~Consumer();
    
    long getCommittedOffset(const std::string& topic, const std::string& consumerId); // GET_OFFSET SDK
    bool commitOffset(const std::string& topic, const std::string& consumerId, long offset); // COMMIT SDK
   std::vector<Record> poll(const std::string& topic, const std::map<int, int>& partitionOffsets);
};

#endif // CONSUMER_H
