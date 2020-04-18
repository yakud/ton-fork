//
// Created by user on 4/18/20.
//

#include "file_log.hpp"

streamdb::FileLog::FileLog(const char *path): path(path) {
    reopen();
}

streamdb::FileLog::~FileLog() {
    if (fs.is_open()) {
        fs.close();
    }
}

void streamdb::FileLog::reopen() {
    if (fs.is_open()) {
        fs.close();
    }

    fs.open(path, std::ios::in|std::ios::out|std::ios::binary|std::ios::app);
    if (fs.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to open "+std::string(path));
    }
}

void streamdb::FileLog::write(const char * data, uint64_t size) {
    if (!fs.good()) {
        reopen();
    }

    fs.write(data, size);
    fs.flush();
}

uint64_t streamdb::FileLog::read_next(char *data, uint64_t size) {
    if (!fs.good()) {
        reopen();
    }

    fs.read(data, size);
    if (!fs.read(data, size) || uint64_t(fs.gcount()) != size) {
        throw std::system_error(errno, std::system_category(), "failed to read next block by "+std::string(path));
    }

    return fs.gcount();
}

void streamdb::FileLog::seek_to(uint64_t pos) {
    if (!fs.good()) {
        reopen();
    }

    fs.seekg(pos, std::ios::beg);
}

void streamdb::FileLog::seek_end() {
    if (!fs.good()) {
        reopen();
    }

    fs.seekg(0, std::ios::end);
}

void streamdb::FileLog::close() {
    if (fs.is_open())
        fs.close();
}
