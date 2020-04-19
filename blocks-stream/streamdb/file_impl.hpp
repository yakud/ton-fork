//
// Created by user on 4/18/20.
//


#ifndef TON_FILE_IMPL_HPP
#define TON_FILE_IMPL_HPP

#include <system_error>
#include "file_impl.hpp"
#include "file_log.hpp"

namespace streamdb {

class FileDB  {
public:
    FileDB(const char * data_path, const char * index_path);
    ~FileDB();

    uint64_t write_bucket(BlockBucket *bucket, char *buffer, uint64_t max_size);
    BlockBucket read_bucket(char *buffer, uint64_t max_size);

protected:
    FileLog *data_log;
    FileLog *index_log;
};

}


#endif //TON_FILE_IMPL_HPP
