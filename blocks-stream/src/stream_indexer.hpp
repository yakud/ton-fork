//
// Created by user on 2/22/20.
//

#ifndef TON_STREAM_INDEXER_HPP
#define TON_STREAM_INDEXER_HPP

#include "tcp_stream.hpp"
#include "blocking-queue.hpp"
#include "tlb_blocks_index.hpp"

namespace ton {
namespace ext {

class StreamIndexer {
protected:
    BlockingQueue <TcpStreamPacket> *queue{};
    ton::ext::TlbBlocksIndex *tlb_index{};

public:
    explicit StreamIndexer(BlockingQueue<TcpStreamPacket> *input, ton::ext::TlbBlocksIndex *tlb_index):
        queue(input), tlb_index(tlb_index) {}
    ~StreamIndexer() = default;

    void run();
    std::thread spawn();
};

}
}


#endif //TON_STREAM_INDEXER_HPP
