//
// Created by user on 10/24/19.
//

#include <td/utils/buffer.h>
#include <vm/boc.h>
#include <crypto/tl/tlblib.hpp>
#include <crypto/block/block-auto.h>
#include <common/checksum.h>
#include <crypto/vm/dict.h>
#include <crypto/block/block-parse.h>

#ifndef TON_BLOCK_PARSER_HPP
#define TON_BLOCK_PARSER_HPP

namespace ton {
namespace ext {

class BlockConverter {
public:
    static std::basic_string<char> bin_to_pretty_custom(td::BufferSlice block_data);
    static ton::BlockId bin_to_block_id(td::BufferSlice block_data);
    static std::basic_string<char> state_to_pretty_custom(td::BufferSlice header_data, td::BufferSlice state_data);
};

}
}

#endif //TON_BLOCK_PARSER_HPP
