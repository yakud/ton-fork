//
// Created by user on 4/18/20.
//


#ifndef TON_FILE_IMPL_HPP
#define TON_FILE_IMPL_HPP

#include <system_error>
#include "streamdb.hpp"
#include "file_impl.hpp"
#include "file_log.hpp"
#include "file_log.cpp"

namespace streamdb {

class FileDB : public DB  {
public:
    FileDB(const char * data_path, const char * index_path);
    ~FileDB() override;

    uint64_t write_bucket(BlockBucket && bucket, char *buffer, uint64_t max_size) override;
    BlockBucket read_bucket(char *buffer, uint64_t max_size) override;

protected:
    FileLog *data_log;
    FileLog *index_log;
};

}


#endif //TON_FILE_IMPL_HPP
