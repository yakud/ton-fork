//
// Created by user on 10/24/19.
//

#include <string>
#include <iostream>
#include <fstream>
#include <utility>
#include <boost/asio.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>
#include <cstring>
#include <thread>

#include "blocking-queue.hpp"
#include "block-converter.hpp"

#ifndef TON_TCP_STREAM_HPP
#define TON_TCP_STREAM_HPP

using boost::asio::ip::tcp;

namespace ton {
namespace ext {

const uint32_t MESSAGE_SIZE_BLOCK = 5 * 1024 * 1024;

struct TcpStreamConfig {
    std::string host;
    std::string port;
};

class TcpStream {
protected:
    TcpStreamConfig* config{};
    BlockingQueue<std::string> *queue{};
//    tcp::socket* socket{};
//    static int last_id;

public:
    TcpStream(std::string _host, std::string _port, BlockingQueue<std::string> *input) {
        config = new TcpStreamConfig{
                .host = std::move(_host),
                .port = std::move(_port)
        };
        queue = input;
    }

    ~TcpStream() {
//        if (socket != nullptr) {
//            if (socket->is_open())
//                socket->close();
//        }
    };

    tcp::socket connect() {
        std::cout << "starting connect to tcp socket: " << config->host << ":" << config->port << std::endl;
        boost::asio::io_service io_service{};

        tcp::resolver resolver(io_service);
        tcp::resolver::query query(tcp::v4(), config->host, config->port);
        tcp::resolver::iterator iterator = resolver.resolve(query);

        tcp::socket sock(io_service);
        boost::asio::connect(sock, iterator);
        std::cout << "success connected to tcp socket: " << config->host << ":" << config->port << std::endl;

        return sock;
    }

    void run(int id) {
//        auto id = TcpStream::last_id++;
        std::cout << "Run tcp stream\n";
        auto sock = connect();

        uint32_t num;
        char buffer[MESSAGE_SIZE_BLOCK];

        unsigned long int message_count = 0;
        unsigned int buffer_size = 0;
        std::string next_message;
        std::string next_message_pretty;
        bool stoped = false;

        std::cout << "Started reade queue\n";
        try {
            while (!stoped) {
                if (!queue->pop(next_message)) {
                    if (queue->closed()) {
                        std::cout << "Queue is closed and empty. Ending stream. " << id << "\n";
                        stoped = true;
                        break;
                    }
                }
                if (next_message.empty()) {
                    continue;
                }

                try {
                    next_message_pretty = BlockConverter::bin_to_pretty_custom(
                            td::BufferSlice(next_message.data(), int(next_message.size()))
                    );
                } catch (std::exception &e) {
                    std::cerr << "Exception deserialize_block: " << e.what() << "\n";
                    continue;
                }

                if (next_message_pretty.empty()) {
                    std::cout << ";";
                    continue;
                }

                num = uint32_t(next_message_pretty.size());
                if (num >= MESSAGE_SIZE_BLOCK) {
                    std::cerr << "too large message pretty: " << num << "\n";
                    break;
                }

                std::memcpy(&buffer[0], reinterpret_cast<const char *>(&num), sizeof(num));
                std::memcpy(&buffer[sizeof(num)], next_message_pretty.data(), num);
                buffer_size += sizeof(num) + num;

                try {
                    if (!boost::asio::write(sock, boost::asio::buffer(buffer, buffer_size))) {
                        std::cerr << "Send message error!" << "\n";
                        break;
                    }
                } catch (boost::system::system_error &e) {
                    std::cerr << "Send message error: " << e.what() << "\n";
                    break;
                }

                next_message.clear();
                next_message_pretty.clear();
                buffer_size = 0;
                ++message_count;

                if (message_count % 10 == 0) {
                    std::cout << ".";
                }
            }
        } catch (std::exception &e) {
            std::cout << "Error tcp stream" << e.what() << "\n";
        }

        std::cout << "CLOSE SOCK " << id << "\n";
        try {
//            if (sock.is_open())
//                sock.close();
        } catch (std::exception &e) {
            std::cout << "Error sock.close" << e.what() << "\n";
        }

        std::cout << "STREAM CLOSED " << id << "\n";
    }

    std::thread spawn(int id) {
        return std::thread( [this, id] { this->run(id); } );
    }
};

}
}

#endif //TON_TCP_STREAM_HPP
