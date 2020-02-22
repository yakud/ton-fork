//
// Created by user on 2/22/20.
//

#include <ton/ton-types.h>
#include "blocks_fetcher.hpp"
#include "tcp_stream.hpp"

using boost::asio::ip::tcp;

const int max_length = 1024;
const std::string block_id_error = "block_id_error";
const std::string block_not_found = "block_not_found";

ton::ext::BlocksFetcher::BlocksFetcher::BlocksFetcher(std::string host, unsigned short port, TlbBlocksIndex *blocks_index):
    host(std::move(host)), port(port), blocks_index(blocks_index) {
}

void ton::ext::BlocksFetcher::BlocksFetcher::run_server(boost::asio::io_context& io_context) {
    boost::system::error_code ec;
    auto ip_address = boost::asio::ip::address::from_string(host, ec);

    if (ec.value() != 0) {
        // Provided IP address is invalid. Breaking execution.
        throw std::runtime_error("Failed to parse the IP address.");
    }

    tcp::acceptor acceptor(io_context, tcp::endpoint(ip_address, port));
    for (;;) {
        std::thread(session_start, acceptor.accept(), blocks_index).detach();
    }
}

void ton::ext::session_start(tcp::socket sock, TlbBlocksIndex *blocks_index) {
    std::cout << "New thread for connection" << std::endl;
    auto message_buffer = new char[MESSAGE_SIZE_BLOCK];
    auto block_buffer = std::vector<char>(MESSAGE_SIZE_BLOCK);

    try {
        for (;;) {
            char data[max_length];

            boost::system::error_code error;
            size_t length = sock.read_some(boost::asio::buffer(data), error);
            if (error == boost::asio::error::eof) {
                break; // Connection closed cleanly by peer.
            } else if (error) {
//                throw boost::system::system_error(error); // Some other error.
                return;
            }

            std::basic_string<char> message;

            ton::BlockId block_id;
            auto r = std::sscanf(
                    data,
                    "(%d,%" SCNx64 ",%u)",
                    &block_id.workchain,
                    &block_id.shard,
                    &block_id.seqno
            );
            if (r != 3) {
                message = block_id_error;
            } else {
                try {
                    auto block = blocks_index->get_block_tlb(block_id, block_buffer);
//                    auto block = blocks_index->get_block_pretty_custom(block_id, block_buffer);
                    message = block.as_slice().str();
                } catch (std::exception &e) {
                    message = block_not_found;
                }
            }

            auto message_length = uint32_t(message.size());
            if (message_length >= MESSAGE_SIZE_BLOCK) {
                std::cerr << "too large message: " << message_length << "\n";
                break;
            }

            std::memcpy(&message_buffer[0], reinterpret_cast<const char *>(&message_length), sizeof(message_length));
            std::memcpy(&message_buffer[sizeof(message_length)], message.data(), message_length);
            auto buffer_size = sizeof(message_length) + message_length;

            try {
                if (!boost::asio::write(sock, boost::asio::buffer(message_buffer, buffer_size))) {
                    std::cerr << "Send message error!" << "\n";
                    break;
                }
            } catch (boost::system::system_error &e) {
                std::cerr << "Send message error: " << e.what() << "\n";
                break;
            }
        }
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception in thread: " << e.what() << "\n";
    }

    std::cout << "End thread for connection" << std::endl;
}

void ton::ext::BlocksFetcher::BlocksFetcher::session(tcp::socket &&sock) {
    std::cout << "New thread for connection" << std::endl;
    auto message_buffer = new char[MESSAGE_SIZE_BLOCK];
    auto block_buffer = std::vector<char>(MESSAGE_SIZE_BLOCK);

    try {
        for (;;) {
            char data[max_length];

            boost::system::error_code error;
            size_t length = sock.read_some(boost::asio::buffer(data), error);
            if (error == boost::asio::error::eof) {
                break; // Connection closed cleanly by peer.
            } else if (error) {
                throw boost::system::system_error(error); // Some other error.
            }

            std::basic_string<char> message;

            ton::BlockId block_id;
            auto r = std::sscanf(
                    data,
                    "(%d,%" SCNx64 ",%u)",
                    &block_id.workchain,
                    &block_id.shard,
                    &block_id.seqno
            );
            if (r != 3) {
                message = block_id_error;
            } else {
                std::cout << "start found block: " << block_id.to_str() << "\n";
                try {
                    message = blocks_index->get_block_pretty_custom(block_id, block_buffer);
                    std::cout << "block_length: " << message.length() << "\n";
                } catch (std::exception &e) {
                    message = block_not_found;
                }
            }

            auto message_length = uint32_t(message.size());
            if (message_length >= MESSAGE_SIZE_BLOCK) {
                std::cerr << "too large message: " << message_length << "\n";
                break;
            }

            std::memcpy(&message_buffer[0], reinterpret_cast<const char *>(&message_length), sizeof(message_length));
            std::memcpy(&message_buffer[sizeof(message_length)], message.data(), message_length);
            auto buffer_size = sizeof(message_length) + message_length;

            try {
                if (!boost::asio::write(sock, boost::asio::buffer(message_buffer, buffer_size))) {
                    std::cerr << "Send message error!" << "\n";
                    break;
                }
            } catch (boost::system::system_error &e) {
                std::cerr << "Send message error: " << e.what() << "\n";
                break;
            }
        }
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception in thread: " << e.what() << "\n";
    }

    std::cout << "End thread for connection" << std::endl;
}
