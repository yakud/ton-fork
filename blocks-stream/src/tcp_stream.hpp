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
#include <boost/crc.hpp>
#include <cstring>
#include <thread>

#include "blocking-queue.hpp"
#include "block-converter.hpp"

#ifndef TON_TCP_STREAM_HPP
#define TON_TCP_STREAM_HPP

using boost::asio::ip::tcp;

namespace ton {
namespace ext {

const uint32_t MESSAGE_SIZE_BLOCK = 10 * 1024 * 1024;

struct TcpStreamConfig {
    std::string host;
    std::string port;
};

struct TcpStreamPacket {
    enum types : uint8_t {
        block = 1,
        state = 2
    };

    types type;
    int64_t offset;
    std::string data;
};

class TcpStream {
protected:
    TcpStreamConfig* config{};
    BlockingQueue<TcpStreamPacket> *queue{};
//    tcp::socket* socket{};
//    static int last_id;

public:
    TcpStream(std::string _host, std::string _port, BlockingQueue<TcpStreamPacket> *input) {
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

    tcp::socket connect();
    void run(int id);
    std::thread spawn(int id);
};

}
}

#endif //TON_TCP_STREAM_HPP
