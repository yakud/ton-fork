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

#SET(Boost_USE_STATIC_LIBS ON CACHE STRING "ON or OFF")
SET(Boost_USE_STATIC_LIBS OFF CACHE STRING "ON or OFF")
FIND_PACKAGE(Boost 1.71 REQUIRED COMPONENTS
#FIND_PACKAGE(Boost 1.66 REQUIRED COMPONENTS
        thread unit_test_framework filesystem date_time system iostreams program_options)

set(BLOCKS_STREAM_SOURCE
        src/stream-writer.hpp
        src/stream-writer.cpp
        src/stream-reader.hpp
        src/stream-reader.cpp
        src/blocking-queue.hpp
        src/block-converter.hpp
        src/block-converter.cpp
        src/tcp_stream.hpp
        src/tcp_stream.cpp
        src/account-extractor.cpp
        src/account-extractor.hpp
        src/stream_indexer.hpp
        src/stream_indexer.cpp
        src/tlb_blocks_index.hpp
        src/tlb_blocks_index.cpp
        src/blocks_fetcher.cpp
        src/blocks_fetcher.hpp

        streamdb/streamdb.hpp
        streamdb/file_impl.cpp streamdb/file_impl.hpp
        streamdb/block_bucket.cpp streamdb/block_bucket.hpp
        streamdb/file_log.cpp streamdb/file_log.cpp
        streamdb/shard_state_diff.cpp
        streamdb/stream_writer.hpp
        streamdb/blocking_queue.hpp
        )

#add_executable(blocks-stream ${BLOCKS_STREAM_SOURCE})
add_executable(blocks-stream-reader reader_main.cpp ${BLOCKS_STREAM_SOURCE})
add_executable(blocks-fetcher blocks_fetcher.cpp ${BLOCKS_STREAM_SOURCE})

target_link_libraries(blocks-stream-reader
        tdutils tdutils tdactor adnl tl_api tl_lite_api tl-lite-utils ton_crypto ton_block dht overlay
        Boost::date_time Boost::system Boost::iostreams Boost::program_options validator catchain validatorsession ton_crypto ton_block
        rldp full-node ton_validator fift-lib memprof)

target_link_libraries(blocks-fetcher
        tdutils tdutils tdactor adnl tl_api tl_lite_api tl-lite-utils ton_crypto ton_block dht overlay
        Boost::date_time Boost::system Boost::iostreams Boost::program_options validator catchain validatorsession ton_crypto ton_block
        rldp full-node ton_validator fift-lib memprof)
