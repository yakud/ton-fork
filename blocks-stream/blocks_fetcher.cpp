//
// Created by user on 2/22/20.
//

#include <blocks-stream/src/blocks_fetcher.hpp>

const std::string DEFAULT_SERVER_PORT = "13699";

std::mutex sig_caught_mutex;
std::condition_variable sig_caught;

void signal_handler( int signal_num ) {
    std::cout << "The interrupt signal is (" << signal_num << "). \n";
    sig_caught.notify_one();
}

int main(int argc, char *argv[]) {
    auto confBlocks = ton::ext::StreamReaderConfig {};
    confBlocks.type = ton::ext::TcpStreamPacket::block;

    std::string tlb_blocks_index_file;
    std::string server_host;
    std::string server_port;
    td::int32 workers;

    td::OptionsParser p;
    p.set_description("TL-B blocks fetcher server for TON network");
    p.add_option('s', "streamblocksfile", "stream blocks file", [&](td::Slice fname) {
        confBlocks.log_filename = fname.str();
        return td::Status::OK();
    });
    p.add_option('i', "streamblocksindexfile", "stream blocks index file", [&](td::Slice fname) {
        confBlocks.index_filename = fname.str();
        confBlocks.seek_filename = confBlocks.index_filename + ".indexer.seek";
        return td::Status::OK();
    });
    p.add_option('t', "tlbblocksindexfile", "cache file with blocks stream offset", [&](td::Slice fname) {
        tlb_blocks_index_file = fname.str();
        return td::Status::OK();
    });
    p.add_option('h', "host", "server host", [&](td::Slice fname) {
        server_host = fname.str();
        return td::Status::OK();
    });
    p.add_option('p', "port", "server port", [&](td::Slice fname) {
        server_port = fname.str();
        if (server_port.length() == 0) {
            server_port = DEFAULT_SERVER_PORT;
        }

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

    ton::ext::BlockingQueue<ton::ext::TcpStreamPacket> queue(1000);

    // Init reader blocks
    ton::ext::StreamReader blocks_reader(&confBlocks, &queue);
    try {
        blocks_reader.load_seek();
    } catch (std::system_error &e) {
        std::cout << "error load seek: " << e.what() << std::endl;
    }
    blocks_reader.open_files();

    std::cout << "blocks log: " << confBlocks.log_filename << std::endl;
    std::cout << "blocks index: " << confBlocks.index_filename << std::endl;
    std::cout << "start reading index from: " << confBlocks.index_seek << std::endl;
    std::cout << "start reading log from: " << confBlocks.log_seek << std::endl;

    ton::ext::TlbBlocksIndex tlbBlockIndex(confBlocks.log_filename, tlb_blocks_index_file);
    try {
        tlbBlockIndex.load_index_block_meta();
    } catch (std::runtime_error &e) {
        std::cout << "error load index block meta: " << e.what() << std::endl;
        exit(1);
    }

    ton::ext::StreamIndexer stream_indexer(&queue, &tlbBlockIndex);
    auto indexer_thread = stream_indexer.spawn();
    auto reader_thread = blocks_reader.spawn();

    // Init server
    if (server_port.length() == 0) {
        server_port = DEFAULT_SERVER_PORT;
    }

    auto server_port_int = static_cast<unsigned short>(std::atoi(server_port.c_str()));
    auto server = ton::ext::BlocksFetcher(server_host, server_port_int, &tlbBlockIndex);

    std::cout << "started server on: " << server_host << ":" << server_port << std::endl;
    boost::asio::io_context io_context;
    try {
        server.run_server(io_context);
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
    std::unique_lock<std::mutex> lock(sig_caught_mutex);
    sig_caught.wait(lock);

    io_context.stop();
    while (!io_context.stopped()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    blocks_reader.stop();
    std::cout << "Waiting for reader...\n";
    reader_thread.join();

    queue.close();

    std::cout << "Waiting for indexer...\n";
    indexer_thread.join();

    return 0;
}
