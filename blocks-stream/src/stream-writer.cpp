//
// Created by user on 9/23/19.
//

#include "stream-writer.hpp"

/*
bool ton::ext::BlocksStream::Init(const std::string &filename) {
    outfileIndex.open(filename + ".index", std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
    outfile.open(filename, std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
    return true;
}

void ton::ext::BlocksStream::writer() {
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

bool ton::ext::BlocksStream::write_block(const std::string &stream) {
    queue->push(stream);
    return true;
}

ton::ext::BlocksStream::~BlocksStream() {
    if (outfile.is_open())
        outfile.close();

    if (outfileIndex)
        outfileIndex.close();

    delete(queue);
}
*/