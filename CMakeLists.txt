cmake_minimum_required(VERSION 3.10)
project(ChatServer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

find_package(Threads REQUIRED)

include_directories(include)

add_executable(chat_server src/Server.cpp src/main_server.cpp)
target_link_libraries(chat_server PRIVATE Threads::Threads)

add_executable(chat_client src/client.cpp)
target_link_libraries(chat_client PRIVATE Threads::Threads)
