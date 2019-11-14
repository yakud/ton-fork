//
// Created by user on 9/24/19.
//

#include <fstream>
#include <iostream>
#include <thread>
#include <cstring>
#include <mutex>
#include "blocking-queue.hpp"

#ifndef BLOCKS_STREAM_BLOCKS_READER_HPP
#define BLOCKS_STREAM_BLOCKS_READER_HPP

namespace ton {
namespace ext {

const uint32_t BUFFER_SIZE_BLOCK = 50 * 1024 * 1024;

struct BlocksReaderConfig {
    std::string log_filename;
    std::string index_filename;

    long int log_seek = 0;
    long int index_seek = 0;
};

class BlocksReader {
protected:
    BlockingQueue<std::string> *queue{};
    BlocksReaderConfig *config;
    std::ifstream ifs_log;
    std::ifstream ifs_index;
    std::fstream ofs_index_seek;

    std::atomic<bool> need_stop;
    std::mutex m;

public:
    BlocksReader(BlocksReaderConfig *conf, BlockingQueue<std::string> *output);
    ~BlocksReader(){close_files();}

    void open_files();
    void close_files();

    void store_seek();
    void load_seek();
    std::string seek_filename() {
        return config->index_filename + ".seek";
    }

    std::thread spawn();
    void run();
    void stop();
};

}
}

#endif //BLOCKS_STREAM_BLOCKS_READER_HPP