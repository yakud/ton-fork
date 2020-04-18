//
// Created by user on 4/18/20.
//


#include <validator/impl/block.hpp>
#include "block_bucket.hpp"

uint64_t streamdb::BlockBucket::serialize(char *buffer, uint64_t max_size) {
    uint64_t offset = 0;

    // Block id
    {
        auto block_id = block->block_id();
        memcpy(&buffer[offset], reinterpret_cast<const char *>(&block_id), sizeof(block_id));
        offset += sizeof(block_id);
    }

    // State
    {
        block::gen::ShardStateUnsplit state_unsplit;
        td::Ref<vm::Cell> state_cell;
        if (!state_unsplit.cell_pack(state_cell, shard_state)) {
            throw std::runtime_error("error serialize shard_state");
        }

        auto state_boc = vm::std_boc_serialize(std::move(state_cell));
        if (state_boc.is_error()) {
            throw std::runtime_error("error serialize shard_state boc");
        }

        auto state_data = state_boc.move_as_ok();
        uint64_t state_length = state_data.length();
        memcpy(&buffer[offset], reinterpret_cast<const char *>(&state_length), sizeof(state_length));
        offset += sizeof(state_length);

        if (offset + state_length > max_size) {
            std::cerr << "too large shard_state: " << state_length << " bytes\n";
            throw std::runtime_error("too large shard_state");
        }

        memcpy(&buffer[offset], state_data.data(), state_length);
        offset += state_length;
    }

    // Block
    {
        auto block_data = vm::load_cell_slice(block->root_cell());
        uint64_t block_length = block_data.size();

        memcpy(&buffer[offset], reinterpret_cast<const char *>(&block_length), sizeof(block_length));
        offset += sizeof(block_length);

        if (offset + block_length > max_size) {
            std::cerr << "too large block_data: " << block_length << " bytes\n";
            throw std::runtime_error("too large block_data");
        }

        memcpy(&buffer[offset], block_data.data(), block_length);
        offset += block_length;
    }

    return offset;
}

streamdb::BlockBucket streamdb::BlockBucket::deserialize(const char *buffer, uint64_t _size) {
    uint64_t offset = 0;

    // Block id
    ton::BlockIdExt block_id;
    block_id = *(reinterpret_cast<ton::BlockIdExt *>(buffer[offset]));
    offset += sizeof(block_id);

    // State
    uint64_t state_length = *(reinterpret_cast<uint64_t *>(buffer[offset]));
    offset += sizeof(state_length);
    auto state_data = td::BufferSlice(&buffer[offset], state_length);
    offset += state_length;

    auto res = vm::std_boc_deserialize(state_data);
    if (res.is_error()) {
        throw std::runtime_error("error deserialize state_data boc");
    }

    block::gen::ShardStateUnsplit shard_state;
    block::gen::ShardStateUnsplit::Record shard_state_record;
    shard_state.cell_unpack(res.move_as_ok(), shard_state_record);

    // Block
    uint64_t block_length = *(reinterpret_cast<uint64_t *>(buffer[offset]));
    offset += sizeof(block_length);
    auto block_data = td::BufferSlice(&buffer[offset], block_length);
    offset += block_length;

    auto block_q = ton::validator::BlockQ::create(block_id, std::move(block_data));
    if (block_q.is_error()) {
        throw std::runtime_error("error deserialize block_data boc");
    }

    auto block = block_q.move_as_ok();

    if (offset != _size) {
        throw std::runtime_error("error wrong size for deserialize");
    }

    auto block_data_q = td::Ref<ton::validator::BlockData>(block);

    return BlockBucket(shard_state_record, block_data_q);
}

streamdb::BlockBucket::BlockBucket(block::gen::ShardStateUnsplit::Record shard_state, td::Ref<ton::validator::BlockData> &block) :
        shard_state(std::move(shard_state)), block(block){}

block::gen::ShardStateUnsplit::Record streamdb::BlockBucket::get_shard_state() {
    return shard_state;
}

td::Ref<ton::validator::BlockData> streamdb::BlockBucket::get_block() {
    return block;
}

//streamdb::BlockBucket streamdb::BlockBucket::create_deserialize(const char *buffer, uint64_t _size) {


//    return streamdb::BlockBucket(ShardStateUnsplit::Record(), td::Ref());
//};