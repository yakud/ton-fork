#include "stream-reader.hpp"

ton::ext::StreamReader::StreamReader(ton::ext::StreamReaderConfig *conf, ton::ext::BlockingQueue<TcpStreamPacket> *output) {
    queue = output;
    config = conf;
    need_stop.store(false);
    open_files();
}

void ton::ext::StreamReader::open_files() {
    std::unique_lock<std::mutex> lock(m);

    // Open stream log
    if (ifs_log.is_open()) {
        ifs_log.close();
    }
    ifs_log.open(config->log_filename, std::ifstream::in | std::ifstream::binary);
    if (ifs_log.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to open "+config->log_filename);
    }
    ifs_log.seekg(config->log_seek,std::ios::beg);

    // Open stream index
    if (ifs_index.is_open()) {
        ifs_index.close();
    }
    ifs_index.open(config->index_filename, std::ifstream::in | std::ifstream::binary);
    if (ifs_index.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to open "+config->index_filename);
    }
    ifs_index.seekg(config->index_seek, std::ios::beg);

    // Open stream index seek
    if (fs_index_seek.is_open()) {
        fs_index_seek.close();
    }
    fs_index_seek.open( config->seek_filename, std::ios::in|std::ios::out|std::ios::binary);
    if (fs_index_seek.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to open "+config->seek_filename);
    }
}

void ton::ext::StreamReader::close_files() {
    if (ifs_log.is_open()) {
        ifs_log.close();
    }
    if (ifs_index.is_open()) {
        ifs_index.close();
    }
    if (fs_index_seek.is_open()) {
        fs_index_seek.close();
    }
}

void ton::ext::StreamReader::run() {
    std::vector<char> header_buffer(sizeof(uint32_t));
    std::vector<char> block_buffer(BUFFER_SIZE_BLOCK);

    uint32_t data_size;

    std::cout << "loading reader\n";
    while (!need_stop.load()) {
        // read index
        if (!ifs_index.read(&header_buffer[0], sizeof(uint32_t)) || ifs_index.gcount() != sizeof(uint32_t)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            try {
                open_files();
            } catch (std::system_error &e) {
                std::cerr << "error open files: " << e.what() << std::endl;
                break;
            }
            continue;
        }

        // calculate data size and read log
        data_size = *(reinterpret_cast<uint32_t *>(header_buffer.data()));
        if (!ifs_log.read(&block_buffer[0], data_size) || ifs_log.gcount() != data_size) {
//            std::cout << ".";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            try {
                open_files();
            } catch (std::system_error &e) {
                std::cerr << "error open files: " << e.what() << std::endl;
                break;
            }
            continue;
        }
        config->index_seek = ifs_index.tellg();
        config->log_seek = ifs_log.tellg();

        // push to queue
        try {
            queue->push(TcpStreamPacket{
                config->type,
                config->log_seek - data_size,
                std::string(&block_buffer[0], data_size),
            });
        } catch (std::exception &e) {
            std::cout << "EXCEPTION catch" << std::endl;
            break;
        }
        header_buffer.clear();
        block_buffer.clear();

        store_seek();
//        std::cout << "SEEK STORED!" << config->seek_filename << "\n";
    }
    close_files();
    std::cout << "Reader end" << std::endl;
}

void ton::ext::StreamReader::store_seek() {
    std::unique_lock<std::mutex> lock(m);
    char buffer[sizeof(long int) * 2];
    std::memcpy(&buffer[0], reinterpret_cast<const char *>(&config->index_seek), sizeof(config->index_seek));
    std::memcpy(&buffer[sizeof(config->index_seek)], reinterpret_cast<const char *>(&config->log_seek), sizeof(config->log_seek));
    fs_index_seek.seekp(0, std::ios::beg);
    if (!fs_index_seek.write(buffer, sizeof(config->index_seek)+sizeof(config->log_seek))) {
        std::cerr << "Error write seek index: " << config->index_seek << ":" << config->log_seek << std::endl;
        throw std::system_error(errno, std::system_category(), "failed to write "+config->seek_filename);
    }
    if (!fs_index_seek.flush()) {
        std::cerr << "Error write seek index: " << config->index_seek << ":" << config->log_seek << std::endl;
        throw std::system_error(errno, std::system_category(), "failed to write "+config->seek_filename);
    }
//    std::cout << buffer << "\n";
}

void ton::ext::StreamReader::load_seek() {
    std::unique_lock<std::mutex> lock(m);
    char buffer[sizeof(long int)];

    // restore index_seek
//    ifs_index_seek.seekg(0, std::ios::beg);
    if (!fs_index_seek.read(buffer, sizeof(long int))) {
        throw std::system_error(errno, std::system_category(), "failed to read index_seek from "+config->seek_filename);
    }
    if (fs_index_seek.gcount() == 0) {
        return;
    }
    if (fs_index_seek.gcount() < int(sizeof(buffer))) {
        throw std::runtime_error("failed to read "+config->seek_filename + "expected exactly buffer size got less");
    }
    config->index_seek = *(reinterpret_cast<long int *>(buffer));

    // restore log_seek
    fs_index_seek.seekg(sizeof(long int), std::ios::beg);
    if (!fs_index_seek.read(buffer, sizeof(long int)) && fs_index_seek.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to read log_seek from "+config->seek_filename);
    }
    if (fs_index_seek.gcount() < int(sizeof(buffer))) {
        throw std::runtime_error("failed to read "+config->seek_filename + "expected exactly buffer size got less");
    }
    config->log_seek = *(reinterpret_cast<long int *>(buffer));
}

std::thread ton::ext::StreamReader::spawn() {
    return std::thread( [this] { run(); } );
}

void ton::ext::StreamReader::stop() {
    need_stop.store(true);
}
