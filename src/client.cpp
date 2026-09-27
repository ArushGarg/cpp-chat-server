#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <poll.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cstdlib>

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 5555;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = std::atoi(argv[2]);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "socket() failed\n";
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        std::cerr << "invalid address: " << host << "\n";
        return 1;
    }

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "connect() failed: " << strerror(errno) << "\n";
        return 1;
    }

    std::cout << "Connected to " << host << ":" << port
              << "  (try: /nick yourname, /join lobby, /who, /msg user hi, /quit)\n";

    pollfd fds[2];
    fds[0].fd = STDIN_FILENO;
    fds[0].events = POLLIN;
    fds[1].fd = fd;
    fds[1].events = POLLIN;

    char buf[4096];
    std::string sockbuf;

    while (true) {
        int ret = poll(fds, 2, -1);
        if (ret < 0) {
            if (errno == EINTR) continue;
            break;
        }

        if (fds[1].revents & (POLLHUP | POLLERR)) {
            std::cout << "Disconnected from server.\n";
            break;
        }

        if (fds[1].revents & POLLIN) {
            ssize_t r = read(fd, buf, sizeof(buf));
            if (r <= 0) {
                std::cout << "Disconnected from server.\n";
                break;
            }
            sockbuf.append(buf, r);
            size_t pos;
            while ((pos = sockbuf.find('\n')) != std::string::npos) {
                std::cout << sockbuf.substr(0, pos) << "\n";
                sockbuf.erase(0, pos + 1);
            }
        }

        if (fds[0].revents & POLLIN) {
            std::string line;
            if (!std::getline(std::cin, line)) {
                std::cout << "EOF on stdin, closing.\n";
                break;
            }
            line += "\n";
            ssize_t sent = 0;
            while (sent < static_cast<ssize_t>(line.size())) {
                ssize_t w = write(fd, line.data() + sent, line.size() - sent);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    std::cerr << "write failed\n";
                    close(fd);
                    return 1;
                }
                sent += w;
            }
            if (line.substr(0, 5) == "/quit") break;
        }
    }

    close(fd);
    return 0;
}
