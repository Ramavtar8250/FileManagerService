cmake_minimum_required(VERSION 3.10)
project(file_rest_api)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Find required packages
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# Add executable
add_executable(file_rest_api 
    src/main.cpp
)

# Link libraries
target_link_libraries(file_rest_api PRIVATE 
    Threads::Threads
    OpenSSL::Crypto  # Link OpenSSL Crypto library
    OpenSSL::SSL     # Link OpenSSL SSL library
)

# Windows-specific settings
if(WIN32)
    target_link_libraries(file_rest_api PRIVATE wsock32 ws2_32)
endif()

# Create uploads directory if it doesn't exist
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/uploads)