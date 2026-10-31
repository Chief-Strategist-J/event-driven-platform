cmake_minimum_required(VERSION 3.16)
project(event_driven_platform_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(platform_cpp main.cpp ${SOURCES})
