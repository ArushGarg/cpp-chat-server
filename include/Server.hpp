#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <memory>
#include <fstream>
#include <mutex>
#include "ThreadPool.hpp"

struct ClientConnection {
    int fd = -1;
    std::string username;          // empty until /nick is set
    std::string room;              // empty until they join one
    std::string inbuf;             // partial incoming data (no trailing \n yet)
    std::deque<std::string> outq;  // queued outgoing messages, each already \n terminated
    size_t outSent = 0;            // bytes of outq.front() already written
    bool wantWritable = false;     // whether we're currently registered for EPOLLOUT
};

class Server {
public:
    explicit Server(int port, size_t threadPoolSize = 4);
    ~Server();

    void run(); // blocks forever, runs the epoll reactor loop

private:
    // --- setup ---
    int createListenSocket();
    void setNonBlocking(int fd);

    // --- epoll helpers ---
    void addToEpoll(int fd, uint32_t events);
    void modEpoll(int fd, uint32_t events);
    void removeFromEpoll(int fd);

    // --- connection lifecycle ---
    void acceptNewConnections();
    void handleReadable(int fd);
    void handleWritable(int fd);
    void closeClient(int fd);

    // --- protocol ---
    void processLine(ClientConnection& client, const std::string& line);
    void handleCommand(ClientConnection& client, const std::string& line);
    void broadcastToRoom(const std::string& room, const std::string& msg, int excludeFd = -1);
    void sendLine(ClientConnection& client, const std::string& msg);
    void queueSend(int fd, const std::string& msg); // msg WITHOUT trailing \n
    void flushOutgoing(ClientConnection& client);

    void joinRoom(ClientConnection& client, const std::string& room);
    void leaveRoom(ClientConnection& client);
    ClientConnection* findByUsername(const std::string& name);

    void logAsync(const std::string& line); // off-loaded to thread pool

    int port_;
    int listenFd_ = -1;
    int epollFd_ = -1;

    // Only ever touched from the single reactor thread -> no locking needed.
    std::unordered_map<int, ClientConnection> clients_;
    std::unordered_map<std::string, std::unordered_set<int>> rooms_;

    ThreadPool pool_;

    std::mutex logMutex_;
    std::ofstream logFile_;
};
