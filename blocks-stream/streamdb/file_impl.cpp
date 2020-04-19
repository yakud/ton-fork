//
// Created by user on 4/18/20.
//

#include "file_impl.hpp"

streamdb::FileDB::FileDB(const char *data_path, const char *index_path) {
    data_log = new FileLog(data_path);
    index_log = new FileLog(index_path);
}

streamdb::FileDB::~FileDB() {
    if (data_log != nullptr)
        data_log->close();

    if (index_log != nullptr)
        index_log->close();
}

uint64_t streamdb::FileDB::write_bucket(BlockBucket *bucket, char *buffer, uint64_t max_size) {
    if (data_log == nullptr || index_log == nullptr) {
        throw std::runtime_error("db broken");
    }

    auto size = bucket->serialize(buffer, max_size);
    data_log->write(buffer, size);
    index_log->write(reinterpret_cast<char *>(&size), sizeof(size));

    // tryna deserialize
    BlockBucket::deserialize(buffer, size).get_block();

    return size;
}

streamdb::BlockBucket streamdb::FileDB::read_bucket(char *buffer, uint64_t max_size) {
    uint64_t size = 0;
    char buff[sizeof(uint64_t)];
    if (index_log->read_next(&buff[0], sizeof(size)) != sizeof(size)) {
        throw std::runtime_error("wrong bytes count read from index log");
    }
    size = *(reinterpret_cast<uint64_t *>(&buff[0]));

    if (size > max_size) {
        throw std::runtime_error("too large to read next block_bucket");
    }

    if (data_log->read_next(buffer, size) != size) {
        throw std::runtime_error("wrong bytes count read from data log");
    }

    return streamdb::BlockBucket::deserialize(buffer, size);
}
