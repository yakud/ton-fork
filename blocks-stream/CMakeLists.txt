cmake_minimum_required(VERSION 3.0.2 FATAL_ERROR)

if (NOT OPENSSL_FOUND)
    find_package(OpenSSL REQUIRED)
endif()

SET(CMAKE_BUILD_TYPE Debug)
SET(CMAKE_C_FLAGS_DEBUG "-D_DEBUG")
SET(CMAKE_CXX_FLAGS_DEBUG "-g")
#set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

SET(Boost_USE_STATIC_LIBS ON CACHE STRING "ON or OFF")
#FIND_PACKAGE(Boost 1.71 REQUIRED COMPONENTS
FIND_PACKAGE(Boost 1.65.1 REQUIRED COMPONENTS
        thread unit_test_framework filesystem date_time system iostreams program_options)

#message("Add prometheus")
#include_directories(${CMAKE_SOURCE_DIR}/prometheus-cpp/_build/deploy/usr/local/include)
#link_directories(${CMAKE_SOURCE_DIR}/prometheus-cpp/_build/deploy/usr/local/lib)

#include_directories(${Boost_INCLUDE_DIR})

set(BLOCKS_STREAM_SOURCE
        src/blocks-stream.cpp
        src/blocks-stream.hpp
        src/blocks_reader.cpp
        src/blocks_reader.hpp
        reader_main.cpp)

#add_executable(blocks-stream ${BLOCKS_STREAM_SOURCE})
add_executable(blocks-stream-reader reader_main.cpp)
#add_executable(tlbc_generator tlbc_generator.cpp)

#target_link_libraries(blocks-stream overlay tdutils tdactor adnl tl_api dht
#        rldp catchain validatorsession full-node validator ton_validator validator
#        fift-lib memprof ${JEMALLOC_LIBRARIES})

target_link_libraries(blocks-stream-reader
        tdutils tdutils tdactor adnllite tl_api tl_lite_api tl-lite-utils ton_crypto ton_block dht overlay
        Boost::date_time Boost::system Boost::iostreams Boost::program_options)
#target_link_libraries(tlbc_generator tdutils tdutils tdactor adnllite tl_api tl_lite_api tl-lite-utils ton_crypto ton_block dht overlay tddb src_parser)