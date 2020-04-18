//
// Created by user on 10/24/19.
//

#include "tcp_stream.hpp"

tcp::socket ton::ext::TcpStream::connect() {
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

void ton::ext::TcpStream::run(int id) {
    std::cout << "Run tcp stream\n";
    auto sock = connect();

    uint32_t message_length;
    auto buffer = new char[MESSAGE_SIZE_BLOCK];

    unsigned long int message_count = 0;
    unsigned int buffer_size = 0;
    TcpStreamPacket next_message;
    std::string next_message_pretty;
    bool stoped = false;

    std::cout << "Started read queue\n";
    try {
        while (!stoped) {
            if (!queue->pop(next_message)) {
                if (queue->closed()) {
                    std::cout << "Queue is closed and empty. Ending stream. " << id << "\n";
                    stoped = true;
                    break;
                }
            }

            if (next_message.data.empty()) {
                continue;
            }

//            std::cout << "Message size: " << next_message.data.size() << "\n";
            try {
                switch (next_message.type) {
                    default:
                        std::cerr << "Undefined message type: " << next_message.type << " skip row.\n";
                        break;

                    case next_message.block:
                        next_message_pretty = BlockConverter::bin_to_pretty_custom(
                                td::BufferSlice(next_message.data.data(), int(next_message.data.size()))
                        );
                        break;

                    case next_message.state:
                        auto data = next_message.data.c_str();

                        auto header_size_buffer = new char[sizeof(uint32_t)];
                        std::memcpy(&header_size_buffer[0], &data[0], sizeof(uint32_t));

                        auto header_size = *(reinterpret_cast<uint32_t *>(header_size_buffer));
                        auto msg_size = next_message.data.size() - header_size -  sizeof(uint32_t);

                        next_message_pretty = BlockConverter::state_to_pretty_custom(
                                td::BufferSlice(&data[sizeof(uint32_t)], header_size),
                                td::BufferSlice(&data[sizeof(uint32_t)+header_size], int(msg_size))
                        );
                        break;

                }
            } catch (std::exception &e) {
                std::cerr << "Exception deserialize_block: " << e.what() << "\n";
                continue;
            }

            if (next_message_pretty.empty()) {
                std::cout << ";";
                continue;
            }

            message_length = uint32_t(next_message_pretty.size());
            if (message_length >= MESSAGE_SIZE_BLOCK) {
                std::cerr << "too large message pretty: " << message_length << "\n";
                break;
            }

            // todo: add crc32 to header
//            boost::crc_32_type  result;
//            result.process_bytes( next_message_pretty.data(), next_message_pretty.length() );
//            auto checksum_crc32 = result.checksum();
//
//            std::memcpy(&buffer[0], reinterpret_cast<const char *>(&checksum_crc32), sizeof(checksum_crc32));
//            std::memcpy(&buffer[sizeof(message_length)], reinterpret_cast<const char *>(&message_length), sizeof(message_length));
//            std::memcpy(&buffer[sizeof(checksum_crc32) + sizeof(message_length)], next_message_pretty.data(), message_length);
//            buffer_size += sizeof(checksum_crc32) + sizeof(message_length) + message_length;

            std::memcpy(&buffer[0], reinterpret_cast<const char *>(&message_length), sizeof(message_length));
            std::memcpy(&buffer[sizeof(message_length)], next_message_pretty.data(), message_length);
            buffer_size += sizeof(message_length) + message_length;

            try {
                if (!boost::asio::write(sock, boost::asio::buffer(buffer, buffer_size))) {
                    std::cerr << "Send message error!" << "\n";
                    break;
                }
            } catch (boost::system::system_error &e) {
                std::cerr << "Send message error: " << e.what() << "\n";
                break;
            }

            next_message.data.clear();
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
//        if (sock.is_open())
//            sock.close();
    } catch (std::exception &e) {
        std::cout << "Error sock.close" << e.what() << "\n";
    }

    std::cout << "STREAM CLOSED " << id << "\n";
}

std::thread ton::ext::TcpStream::spawn(int id) {
    return std::thread( [this, id] { this->run(id); } );
}
