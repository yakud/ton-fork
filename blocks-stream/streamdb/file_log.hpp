//
// Created by user on 4/18/20.
//

#ifndef TON_FILE_LOG_HPP
#define TON_FILE_LOG_HPP

#include <fstream>

namespace streamdb {

class FileLog {
public:
    explicit FileLog(const char *path);
    ~FileLog();

    void reopen();
    void close();
    void write(const char * data, uint64_t size);
    uint64_t read_next(char * data, uint64_t size);
    void seek_to(uint64_t pos);
    void seek_end();

protected:
    const char *path;
    std::fstream fs;
};

}

#endif //TON_FILE_LOG_HPP
