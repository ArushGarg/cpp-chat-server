#include "Server.hpp"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <sstream>
#include <chrono>
#include <ctime>
#include <algorithm>
#include <stdexcept>

namespace {
constexpr int MAX_EVENTS = 256;
constexpr size_t READ_CHUNK = 4096;
constexpr size_t MAX_INBUF = 8192; // guard against a client never sending \n

std::string timestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    char buf[32];
    std::tm tmBuf;
    localtime_r(&t, &tmBuf);
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tmBuf);
    return std::string(buf);
}
} // namespace

Server::Server(int port, size_t threadPoolSize)
    : port_(port), pool_(threadPoolSize) {
    logFile_.open("chat.log", std::ios::app);
    listenFd_ = createListenSocket();

    epollFd_ = epoll_create1(0);
    if (epollFd_ < 0) throw std::runtime_error("epoll_create1 failed");

    addToEpoll(listenFd_, EPOLLIN);
}

Server::~Server() {
    if (listenFd_ >= 0) close(listenFd_);
    if (epollFd_ >= 0) close(epollFd_);
}

int Server::createListenSocket() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error("bind() failed - port in use?");
    }
    if (listen(fd, SOMAXCONN) < 0) {
        throw std::runtime_error("listen() failed");
    }
    setNonBlocking(fd);
    return fd;
}

void Server::setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void Server::addToEpoll(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        std::cerr << "epoll_ctl ADD failed for fd " << fd << ": " << strerror(errno) << "\n";
    }
}

void Server::modEpoll(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    epoll_ctl(epollFd_, EPOLL_CTL_MOD, fd, &ev);
}

void Server::removeFromEpoll(int fd) {
    epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
}

void Server::run() {
    std::vector<epoll_event> events(MAX_EVENTS);
    std::cout << "Chat server listening on port " << port_ << " (pid " << getpid() << ")\n";

    while (true) {
        int n = epoll_wait(epollFd_, events.data(), MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "epoll_wait error: " << strerror(errno) << "\n";
            break;
        }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == listenFd_) {
                acceptNewConnections();
                continue;
            }

            if (ev & (EPOLLHUP | EPOLLERR)) {
                closeClient(fd);
                continue;
            }
            if (ev & EPOLLIN) {
                handleReadable(fd);
                // client may have been closed inside handleReadable
                if (clients_.find(fd) == clients_.end()) continue;
            }
            if (ev & EPOLLOUT) {
                handleWritable(fd);
            }
        }
    }
}

void Server::acceptNewConnections() {
    while (true) {
        sockaddr_in clientAddr{};
        socklen_t len = sizeof(clientAddr);
        int fd = accept(listenFd_, reinterpret_cast<sockaddr*>(&clientAddr), &len);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; // drained backlog
            if (errno == EINTR) continue;
            std::cerr << "accept() error: " << strerror(errno) << "\n";
            break;
        }

        setNonBlocking(fd);
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        ClientConnection conn;
        conn.fd = fd;
        clients_[fd] = std::move(conn);

        addToEpoll(fd, EPOLLIN | EPOLLET); // edge-triggered

        sendLine(clients_[fd], "Welcome! Set a username with: /nick <name>");
        logAsync("[" + timestamp() + "] connection accepted fd=" + std::to_string(fd));
    }
}

void Server::handleReadable(int fd) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) return;
    ClientConnection& client = it->second;

    char buf[READ_CHUNK];
    while (true) {
        ssize_t r = read(fd, buf, sizeof(buf));
        if (r > 0) {
            client.inbuf.append(buf, r);

            // extract complete newline-terminated lines
            size_t pos;
            while ((pos = client.inbuf.find('\n')) != std::string::npos) {
                std::string line = client.inbuf.substr(0, pos);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                client.inbuf.erase(0, pos + 1);
                processLine(client, line);
                // command handling (e.g. /quit) may have closed this client
                if (clients_.find(fd) == clients_.end()) return;
            }

            if (client.inbuf.size() > MAX_INBUF) {
                sendLine(client, "ERROR: line too long, disconnecting");
                closeClient(fd);
                return;
            }
        } else if (r == 0) {
            closeClient(fd); // peer closed connection
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; // drained
            if (errno == EINTR) continue;
            closeClient(fd);
            return;
        }
    }
}

void Server::handleWritable(int fd) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) return;
    flushOutgoing(it->second);
}

void Server::closeClient(int fd) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) return;

    ClientConnection& client = it->second;
    std::string name = client.username.empty() ? ("fd" + std::to_string(fd)) : client.username;

    if (!client.room.empty()) {
        broadcastToRoom(client.room, "* " + name + " left the chat", fd);
        leaveRoom(client);
    }

    logAsync("[" + timestamp() + "] " + name + " disconnected");

    removeFromEpoll(fd);
    close(fd);
    clients_.erase(it);
}

void Server::processLine(ClientConnection& client, const std::string& line) {
    if (line.empty()) return;
    if (line[0] == '/') {
        handleCommand(client, line);
        return;
    }
    if (client.username.empty()) {
        sendLine(client, "ERROR: set a username first with /nick <name>");
        return;
    }
    if (client.room.empty()) {
        sendLine(client, "ERROR: join a room first with /join <room>");
        return;
    }
    std::string out = client.username + ": " + line;
    broadcastToRoom(client.room, out, client.fd);
    logAsync("[" + timestamp() + "][" + client.room + "] " + out);
}

void Server::handleCommand(ClientConnection& client, const std::string& line) {
    std::istringstream iss(line);
    std::string cmd;
    iss >> cmd;

    if (cmd == "/nick") {
        std::string name;
        iss >> name;
        if (name.empty()) {
            sendLine(client, "ERROR: usage /nick <name>");
            return;
        }
        if (findByUsername(name) != nullptr) {
            sendLine(client, "ERROR: username already taken");
            return;
        }
        std::string old = client.username;
        client.username = name;
        sendLine(client, "OK nickname set to " + name);
        logAsync("[" + timestamp() + "] fd" + std::to_string(client.fd) +
                 (old.empty() ? " registered as " : " renamed to ") + name);

    } else if (cmd == "/join") {
        std::string room;
        iss >> room;
        if (client.username.empty()) {
            sendLine(client, "ERROR: set a username first with /nick <name>");
            return;
        }
        if (room.empty()) {
            sendLine(client, "ERROR: usage /join <room>");
            return;
        }
        joinRoom(client, room);

    } else if (cmd == "/leave") {
        if (client.room.empty()) {
            sendLine(client, "ERROR: not in a room");
            return;
        }
        broadcastToRoom(client.room, "* " + client.username + " left the room", client.fd);
        leaveRoom(client);
        sendLine(client, "OK left room");

    } else if (cmd == "/msg") {
        std::string target;
        iss >> target;
        std::string rest;
        std::getline(iss, rest);
        if (!rest.empty() && rest[0] == ' ') rest.erase(0, 1);
        if (target.empty() || rest.empty()) {
            sendLine(client, "ERROR: usage /msg <user> <message>");
            return;
        }
        ClientConnection* dst = findByUsername(target);
        if (!dst) {
            sendLine(client, "ERROR: no such user online");
            return;
        }
        sendLine(*dst, "[DM from " + client.username + "] " + rest);
        sendLine(client, "[DM to " + target + "] " + rest);

    } else if (cmd == "/who") {
        if (client.room.empty()) {
            sendLine(client, "ERROR: not in a room");
            return;
        }
        std::string list = "Users in " + client.room + ": ";
        for (int fd : rooms_[client.room]) {
            list += clients_[fd].username + " ";
        }
        sendLine(client, list);

    } else if (cmd == "/rooms") {
        std::string list = "Rooms: ";
        for (auto& [name, members] : rooms_) {
            list += name + "(" + std::to_string(members.size()) + ") ";
        }
        if (rooms_.empty()) list += "(none yet)";
        sendLine(client, list);

    } else if (cmd == "/quit") {
        sendLine(client, "Goodbye!");
        closeClient(client.fd);

    } else {
        sendLine(client, "ERROR: unknown command " + cmd);
    }
}

void Server::joinRoom(ClientConnection& client, const std::string& room) {
    if (!client.room.empty()) {
        if (client.room == room) {
            sendLine(client, "ERROR: already in that room");
            return;
        }
        broadcastToRoom(client.room, "* " + client.username + " left the room", client.fd);
        leaveRoom(client);
    }
    client.room = room;
    rooms_[room].insert(client.fd);
    sendLine(client, "OK joined " + room);
    broadcastToRoom(room, "* " + client.username + " joined the room", client.fd);
    logAsync("[" + timestamp() + "] " + client.username + " joined room " + room);
}

void Server::leaveRoom(ClientConnection& client) {
    if (client.room.empty()) return;
    auto it = rooms_.find(client.room);
    if (it != rooms_.end()) {
        it->second.erase(client.fd);
        if (it->second.empty()) rooms_.erase(it);
    }
    client.room.clear();
}

ClientConnection* Server::findByUsername(const std::string& name) {
    for (auto& [fd, c] : clients_) {
        if (c.username == name) return &c;
    }
    return nullptr;
}

void Server::broadcastToRoom(const std::string& room, const std::string& msg, int excludeFd) {
    auto it = rooms_.find(room);
    if (it == rooms_.end()) return;
    for (int fd : it->second) {
        if (fd == excludeFd) continue;
        queueSend(fd, msg);
    }
}

void Server::sendLine(ClientConnection& client, const std::string& msg) {
    queueSend(client.fd, msg);
}

void Server::queueSend(int fd, const std::string& msg) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) return;
    it->second.outq.push_back(msg + "\n");
    flushOutgoing(it->second);
}

void Server::flushOutgoing(ClientConnection& client) {
    while (!client.outq.empty()) {
        const std::string& front = client.outq.front();
        const char* data = front.data() + client.outSent;
        size_t remaining = front.size() - client.outSent;

        ssize_t written = write(client.fd, data, remaining);
        if (written > 0) {
            client.outSent += static_cast<size_t>(written);
            if (client.outSent == front.size()) {
                client.outq.pop_front();
                client.outSent = 0;
            }
        } else if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // socket buffer full: wait for EPOLLOUT to resume
            if (!client.wantWritable) {
                client.wantWritable = true;
                modEpoll(client.fd, EPOLLIN | EPOLLET | EPOLLOUT);
            }
            return;
        } else if (written < 0 && errno == EINTR) {
            continue;
        } else {
            closeClient(client.fd);
            return;
        }
    }

    // queue drained: stop listening for EPOLLOUT
    if (client.wantWritable) {
        client.wantWritable = false;
        modEpoll(client.fd, EPOLLIN | EPOLLET);
    }
}

void Server::logAsync(const std::string& line) {
    pool_.enqueue([this, line] {
        std::lock_guard<std::mutex> lock(logMutex_);
        logFile_ << line << "\n";
        logFile_.flush();
    });
}
