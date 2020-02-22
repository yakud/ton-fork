//
// Created by user on 2/22/20.
//

#ifndef TON_TLB_BLOCKS_INDEX_HPP
#define TON_TLB_BLOCKS_INDEX_HPP

#include <ton/ton-types.h>
#include <fstream>
#include <map>
#include "block-converter.hpp"

namespace ton {
namespace ext {

//const uint32_t BUFFER_SIZE_BLOCK = 50 * 1024 * 1024;

struct BlockStreamMeta {
    int64_t offset;
    int64_t size;
};

class TlbBlocksIndex {
protected:
    std::ifstream ifs_log;
    std::ifstream ifs_tlb_index;
    std::ofstream ofs_tlb_index;
    std::map<ton::BlockId, BlockStreamMeta> blocks_index;

    const std::string& stream_filename;
    const std::string& tlb_blocks_index_filename;

public:
    TlbBlocksIndex(const std::string& _stream_filename, const std::string& _tlb_blocks_index_filename);

    void open_files();
    void add_block_meta(ton::BlockId blocks_id, BlockStreamMeta block_meta);
    td::BufferSlice get_block_tlb(ton::BlockId blocks_id, std::vector<char> &block_buffer);
    std::basic_string<char> get_block_pretty_custom(BlockId blocks_id, std::vector<char> &block_buffer);

    void write_index_block_meta(BlockId blocks_id, BlockStreamMeta block_meta);

    void load_index_block_meta();
};

}
}

#endif //TON_TLB_BLOCKS_INDEX_HPP
