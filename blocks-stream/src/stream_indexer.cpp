//
// Created by user on 2/22/20.
//

#include "stream_indexer.hpp"

void ton::ext::StreamIndexer::run() {
//    uint32_t message_length;
//    auto buffer = new char[MESSAGE_SIZE_BLOCK];

    unsigned long int message_count = 0;
    TcpStreamPacket next_message;
    bool stoped = false;

    std::cout << "Started read queue\n";
    try {
        while (!stoped) {
            if (!queue->pop(next_message)) {
                if (queue->closed()) {
                    std::cout << "Queue is closed and empty. Ending stream. " << "\n";
                    stoped = true;
                    break;
                }
            }

            if (next_message.data.empty()) {
                continue;
            }

            try {
                switch (next_message.type) {
                    default:
                        std::cerr << "Undefined message type: " << next_message.type << " skip row.\n";
                        break;

                    case TcpStreamPacket::block:
                        auto block_id = BlockConverter::bin_to_block_id(
                                td::BufferSlice(next_message.data.data(), int(next_message.data.size()))
                        );

                        std::cout << "offset: " << next_message.offset
                            << " | length: " << next_message.data.size()
                            << " | block: " << block_id.to_str()
                            << std::endl;

                        tlb_index->add_block_meta(block_id, BlockStreamMeta{
                            .offset = next_message.offset,
                            .size = int64_t(next_message.data.size()),
                        });

                        break;

                    // TODO: should be implement in feature
//                    case TcpStreamPacket::state:
//                        break;

                }
            } catch (std::exception &e) {
                std::cerr << "Exception deserialize_block: " << e.what() << "\n";
                continue;
            }

            next_message.data.clear();
            ++message_count;

            if (message_count % 10 == 0) {
                std::cout << ".";
            }
        }
    } catch (std::exception &e) {
        std::cout << "Error indexer stream " << e.what() << "\n";
    }

    std::cout << "indexer closed\n";
}

std::thread ton::ext::StreamIndexer::spawn() {
    return std::thread( [this] { this->run(); } );
}

