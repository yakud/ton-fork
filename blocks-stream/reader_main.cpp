//
// Created by user on 9/24/19.
//

//#include <iostream>
//#include <vector>
//#include <td/utils/buffer.h>
//#include <vm/boc.h>
//#include <crypto/block/block-auto.h>
//
//#include <cstdlib>
//#include <cstring>
//#include <string>
//#include <thread>
//#include <mutex>
//#include <boost/asio.hpp>
//#include <chrono>
//#include <common/checksum.h>
//#include <crypto/vm/dict.h>
//
//#include <crypto/block/block-parse.h>
//#include <csignal>
//
//#include <td/utils/OptionsParser.h>
//#include <common/errorcode.h>

#include <condition_variable>
#include <td/utils/OptionsParser.h>
#include <common/errorcode.h>

#include <blocks-stream/src/blocks-reader.hpp>
#include <blocks-stream/src/tcp_stream.hpp>

//std::atomic<bool> sig_caught;
std::mutex sig_caught_mutex;
std::condition_variable sig_caught;

void signal_handler( int signal_num ) {
    std::cout << "The interrupt signal is (" << signal_num << "). \n";
    sig_caught.notify_one();
//    sig_caught.store(true);
}

// https://github.com/jupp0r/prometheus-cpp needed
int main(int argc, char *argv[]) {
    ton::ext::BlocksReaderConfig conf = {};
    std::string stream_host;
    std::string stream_port;
    td::int32 workers;

    td::OptionsParser p;
    p.set_description("stream reader for TON network");
    p.add_option('s', "streamfile", "stream blocks file", [&](td::Slice fname) {
        conf.log_filename = fname.str();
        return td::Status::OK();
    });
    p.add_option('i', "indexfile", "stream index file", [&](td::Slice fname) {
        conf.index_filename = fname.str();
        return td::Status::OK();
    });
    p.add_option('h', "host", "stream tcp receiver host", [&](td::Slice fname) {
        stream_host = fname.str();
        return td::Status::OK();
    });
    p.add_option('p', "port", "stream tcp receiver port", [&](td::Slice fname) {
        stream_port = fname.str();
        return td::Status::OK();
    });
    p.add_option('w', "workers", "workers count", [&](td::Slice fname) {
        td::int32 v;
        try {
            v = std::stoi(fname.str());
        } catch (...) {
            return td::Status::Error(ton::ErrorCode::error, "bad value for --workers: not a number");
        }
        if (v < 1 || v > 256) {
            return td::Status::Error(ton::ErrorCode::error, "bad value for --workers: should be in range [1..256]");
        }
        workers = v;
        return td::Status::OK();
    });
    auto S = p.run(argc, argv);
    if (S.is_error()) {
        std::cerr << "failed to parse options: " << S.error().public_message();
        std::_Exit(2);
    }

    ton::ext::BlockingQueue<std::string> queue(1000);

    // Init reader
    ton::ext::BlocksReader blocks_reader(&conf, &queue);
    try {
//        blocks_reader.load_seek();
    } catch (std::system_error &e) {
        std::cout << "error load seek: " << e.what() << std::endl;
    }
//    blocks_reader.open_files();

    std::cout << "start reading index from: " << conf.index_seek << std::endl;
    std::cout << "start reading log from: " << conf.log_seek << std::endl;
    std::cout << "log: " << conf.log_filename << std::endl;
    std::cout << "index: " << conf.index_filename << std::endl;

    // Init TCP stream
    std::cout << "starting streams..." << std::endl;
    std::vector<ton::ext::TcpStream> tcp_streams;
    std::vector<std::thread> tcp_streams_thread;
    tcp_streams.reserve(workers);
    tcp_streams_thread.reserve(workers);

//    ton::ext::TcpStream s1(stream_host, stream_port, &queue);
//    auto st1 = s1.spawn();

    for (auto i = 0; i < workers; i ++) {
        tcp_streams.emplace_back(stream_host, stream_port, &queue);
        auto t = tcp_streams.at(i).spawn(i);
        tcp_streams_thread.emplace_back(std::move(t));

        std::cout << "started stream: " << i << std::endl;
    }

    std::cout << "total started streams: " << workers << std::endl;

    auto reader_thread = blocks_reader.spawn();

    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
    std::unique_lock<std::mutex> lock(sig_caught_mutex);
    sig_caught.wait(lock);

    std::cout << "caught a SIG.\n";
    std::cout << "Reader stop\n";
    blocks_reader.stop();
    std::cout << "Waiting for reader\n";
    reader_thread.join();

    std::cout << "Queue close\n";
    queue.close();

    while (!queue.empty()) {
        std::cout << "queue.Size(): " << queue.size() << "!\n";
    }

    std::cout << "Waiting for tcp streams...\n";

    // TODO:
    for (auto i = 0; i < workers; i ++) {
        auto ss = std::move(tcp_streams_thread[i]);
        if (ss.joinable()) {
            ss.join();
        }
        std::cout << "MAIN STREAM JOINED\n";
    }

//    tcp_streams.clear();
//    tcp_streams_thread.clear();
//    for (auto & t : tcp_streams_thread) {
//        t.join();
//    }

    std::cout << "Done ALL!\n";

    return 0;
}