# Project configuration
cmake_minimum_required(VERSION 3.22)
project(ex3 VERSION 1.0 LANGUAGES C CXX)


# Ensure system has pthreads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(MapReduceFramework
        MapReduceClient.h
        MapReduceFramework.cpp MapReduceFramework.h
        JobContext.cpp
        JobContext.h
        Barrier/Barrier.cpp
        Barrier/Barrier.h
        # ------------- Add your own .h/.cpp files here -------------------
        )


set_property(TARGET MapReduceFramework PROPERTY CXX_STANDARD 11)
target_compile_options(MapReduceFramework PUBLIC -Wall -Wextra)
target_include_directories(MapReduceFramework PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# link pthreads to your framework
target_link_libraries(MapReduceFramework PUBLIC Threads::Threads)

# Add tests
add_subdirectory(mattanTests)
