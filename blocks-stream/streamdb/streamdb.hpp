//
// Created by user on 4/18/20.
//

#ifndef TON_STREAMDB_HPP
#define TON_STREAMDB_HPP

#include <cstdint>
#include "block_bucket.hpp"

namespace streamdb {

class DB {
public:
    virtual ~DB() = default;
    virtual uint64_t write_bucket(BlockBucket* bucket, char *buffer, uint64_t max_size) = 0;
    virtual BlockBucket read_bucket(char *buffer, uint64_t max_size) = 0;
};

};


#endif //TON_STREAMDB_HPP
