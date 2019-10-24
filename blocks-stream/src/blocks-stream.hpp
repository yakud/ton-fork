//
// Created by user on 9/23/19.
//

#include <fstream>
#include <mutex>
#include <iostream>
#include <sstream>
#include <queue>
#include <condition_variable>
#include <atomic>
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

    bool Init(const std::string& f) {
        outfileIndex.open(f + ".index", std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
        outfile.open(f, std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
        return true;
    }

    BlocksStream() = default;
    ~BlocksStream() {
        outfile.close();
        outfileIndex.close();
        delete(blocksQueue);
    }

    bool WriteData(const std::string &stream) {
        blocksQueue->push(stream);
        return true;
    }

    void Writer() {
        // todo: params
        BlockingQueue<std::string> bq(10000);
        blocksQueue = &bq;

        std::ostringstream header_buffer;
        std::ostringstream body_buffer;

        // todo: graceful shutdown
        while (true) {
            if (need_stop) {
                break;
            }
            auto nextMessage = blocksQueue->pop();

            body_buffer << nextMessage;

            auto num = uint32_t(nextMessage.length());
            header_buffer.write(reinterpret_cast<const char *>(&num), sizeof(num));

            outfile << body_buffer.str();
            outfile.flush();
            outfileIndex << header_buffer.str();
            outfileIndex.flush();

            body_buffer.str("");
            body_buffer.clear();
            header_buffer.str("");
            header_buffer.clear();
        }
    }

protected:
    BlockingQueue<std::string> *blocksQueue{};
    std::ofstream outfile;
    std::ofstream outfileIndex;
    bool need_stop = false;

};

}
}

#endif //TON_BLOCKSSTREAM_H
