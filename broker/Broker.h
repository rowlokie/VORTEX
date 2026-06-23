#ifndef BROKER_H
#define BROKER_H

#include "TopicManager.h"
#include <string>
#include "Metadata.h"
#include<unordered_map>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <windows.h>
    typedef int socklen_t;
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

class Broker {
private:
    int port;
    TopicManager topicManager;
    SOCKET_TYPE server_fd;
    bool running;

    // Observability stats counters
    PlatformMutex statsMutex;
    long messagesProduced;
    long messagesConsumed;

    void handleClient(SOCKET_TYPE clientSocket);
    static void handleClientThread(Broker* broker, SOCKET_TYPE clientSocket);

#ifdef _WIN32
    static DWORD WINAPI ClientThreadProc(LPVOID lpParam);
    struct ThreadData {
        Broker* broker;
        SOCKET_TYPE clientSocket;
    };
#endif

public:
    Broker(int port);
    ~Broker();
    bool start();
    void stop();
};

#endif // BROKER_H
