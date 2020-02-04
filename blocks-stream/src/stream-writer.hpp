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

class FileStreamWriter {
public:
    static FileStreamWriter& get_instance_blocks() {
        static FileStreamWriter instance{};
        return instance;
    };

    static FileStreamWriter& get_instance_state() {
        static FileStreamWriter instanceState{};
        return instanceState;
    };

    bool Init(const std::string& filename) {
        outfileIndex.open(filename + ".index", std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
        outfile.open(filename, std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
        return true;
    }

    FileStreamWriter() = default;
    ~FileStreamWriter() {
        if (outfile.is_open())
            outfile.close();

        if (outfileIndex)
            outfileIndex.close();

//        delete(queue);
    }

    bool write(const std::string &stream) {
        queue->push(stream);
        return true;
    }

    void writer() {
        // todo: params
        BlockingQueue<std::string> bq(10000);
        queue = &bq;

        std::ostringstream header_buffer;
        std::ostringstream body_buffer;
        std::string nextMessage;

        // todo: graceful shutdown
        while (true) {
            if (need_stop) {
                break;
            }
            if (!queue->pop(nextMessage)) {
                if (queue->closed()) {
                    break;
                }
                continue;
            }

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
            nextMessage.clear();
        }
    }

protected:
    BlockingQueue<std::string> *queue{};
    std::ofstream outfile;
    std::ofstream outfileIndex;
    atomic<bool> need_stop{false};

};

}
}

#endif //TON_BLOCKSSTREAM_H
