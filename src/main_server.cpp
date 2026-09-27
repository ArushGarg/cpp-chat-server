#include "Server.hpp"
#include <iostream>
#include <cstdlib>
#include <csignal>

int main(int argc, char* argv[]) {
    int port = 5555;
    if (argc >= 2) port = std::atoi(argv[1]);

    // Ignore SIGPIPE: writing to a closed socket would otherwise kill the process.
    signal(SIGPIPE, SIG_IGN);

    try {
        Server server(port, /*threadPoolSize=*/4);
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
