//
// Created by user on 4/18/20.
//

#ifndef TON_BLOCK_BUCKET_HPP
#define TON_BLOCK_BUCKET_HPP

#include <cstdint>
#include <utility>
#include <crypto/vm/cells/CellSlice.h>
#include <crypto/block/block-auto.h>
#include <validator/interfaces/block.h>
#include <crypto/vm/boc.h>

namespace streamdb {

class BlockBucket  {
public:
    BlockBucket(block::gen::ShardStateUnsplit::Record shard_state, td::Ref<ton::validator::BlockData> &block);

    uint64_t serialize(char *buffer, uint64_t max_size);
    static BlockBucket deserialize(const char * buffer, uint64_t _size);

    block::gen::ShardStateUnsplit::Record get_shard_state();
    td::Ref<ton::validator::BlockData> get_block();

protected:

    block::gen::ShardStateUnsplit::Record shard_state;
    td::Ref<ton::validator::BlockData> block;

};

}

#endif //TON_BLOCK_BUCKET_HPP
