//
// Created by user on 9/23/19.
//

#include <fstream>
#include <iostream>
#include <sstream>
#include "blocking-queue.hpp"

#ifndef TON_BLOCKSSTREAM_H
#define TON_BLOCKSSTREAM_H

using namespace std;

namespace ton {
namespace ext {

class BlocksStream {
public:
    static BlocksStream& GetInstance() {
        static BlocksStream instance{};
        return instance;
    };

    bool Init(const std::string& filename);
    BlocksStream() = default;
    ~BlocksStream();

    bool write_block(const std::string &stream);
    void writer();

protected:
    BlockingQueue<std::string> *blocksQueue{};
    std::ofstream outfile;
    std::ofstream outfileIndex;
    atomic<bool> need_stop{false};

};

}
}

#endif //TON_BLOCKSSTREAM_H
