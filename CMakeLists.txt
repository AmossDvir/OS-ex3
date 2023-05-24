cmake_minimum_required(VERSION 3.16.3)
project(ex3)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

include_directories(.)

add_executable(ex3
        MapReduceClient.h
        Barrier/Barrier.h
        "Sample Client/SampleClient.cpp"
        Barrier/Barrier.cpp
        MapReduceFramework.cpp
        MapReduceFramework.h)
