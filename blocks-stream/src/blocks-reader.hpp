//
// Created by user on 9/24/19.
//

#include <fstream>
#include <iostream>
#include "blocking-queue.hpp"
//#include <vector>
//#include <td/utils/buffer.h>
//#include <vm/boc.h>
//#include <crypto/block/block-auto.h>
//
//#include <queue>
//#include <cstdlib>
//#include <cstring>
//#include <iostream>
//#include <condition_variable>
//#include <string>
#include <thread>
//#include <mutex>
//#include <boost/asio.hpp>
//#include <cstdlib>
//#include <chrono>
#include <common/checksum.h>
#include <crypto/vm/dict.h>
//
//#include <fstream>
//#include <iostream>
//#include <vector>
#include <td/utils/buffer.h>
#include <vm/boc.h>
#include <crypto/block/block-auto.h>
//
#include <crypto/block/block-parse.h>
#include <validator/impl/block.hpp>
//#include <csignal>
#include <atomic>

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
    ~BlocksReader(){CloseFiles();}

    void OpenFiles();
    void CloseFiles();

    void StoreSeek();
    void LoadSeek();
    std::string SeekFilename() {
        return config->index_filename + ".seek";
    }

    std::thread Spawn();
    void Run();
    void Stop();
};

}
}

#endif //BLOCKS_STREAM_BLOCKS_READER_HPP
