//
// Created by user on 12/19/19.
//

#ifndef TON_ACCOUNT_EXTRACTOR_HPP
#define TON_ACCOUNT_EXTRACTOR_HPP

#include <crypto/common/refcnt.hpp>
#include <crypto/vm/cells/MerkleProof.h>
#include <crypto/block/block-auto.h>
#include <crypto/vm/dict.h>
#include <crypto/block/block-parse.h>
#include <validator/interfaces/block.h>
#include "stream-writer.hpp"

namespace ton {
namespace ext {

class AccountExtractor {
public:
    static td::Status extract_and_write_shard_state(td::Ref<vm::Cell> &root, td::Ref<ton::validator::BlockData> &block) {
        // accounts state snapshot and write to stream
        vm::MerkleProofBuilder pb{root};
        block::gen::ShardStateUnsplit::Record state;
        if (!tlb::unpack_cell(pb.root(), state)) {
            std::cout << "cannot unpack state header" << std::endl;
            return td::Status::Error("Write_BlockStream_Failed");
        }

        block::gen::ShardStateUnsplit state_unsplit;
        block::gen::ShardStateUnsplit::Record state_copy;

        state_copy.global_id = state.global_id;
        state_copy.shard_id = state.shard_id;
        state_copy.seq_no = state.seq_no;
        state_copy.vert_seq_no = state.vert_seq_no;
        state_copy.gen_utime = state.gen_utime;
        state_copy.gen_lt = state.gen_lt;
        state_copy.min_ref_mc_seqno = state.min_ref_mc_seqno;
        state_copy.out_msg_queue_info = state.out_msg_queue_info;
        td::Ref<vm::Cell> accounts;
        state_copy.accounts = accounts;
        state_copy.before_split = state.before_split;
        state_copy.r1 = state.r1;
        state_copy.custom = state.custom;

        td::Ref<vm::Cell> state_cell;
        if (!state_unsplit.cell_pack(state_cell, state_copy)) {
            return td::Status::Error(-666,  "ERROR state_copy CELL PACK");
        }

        return td::Status::OK();
    }

    static td::Status extract_and_write_accounts_state(td::Ref<vm::Cell> &root, td::Ref<ton::validator::BlockData> &block) {
        std::map<std::string, td::BitArray<256>> uniq_accounts_addr;

        // extract accounts addr
        block::gen::BlockExtra::Record extra;
        block::gen::BlockInfo::Record blk_info_rec;
        block::gen::Block::Record blk;

        if (tlb::unpack_cell( block->root_cell(), blk) &&
            tlb::unpack_cell(blk.extra, extra) &&
            tlb::unpack_cell(blk.info, blk_info_rec)) {

            vm::AugmentedDictionary account_blocks_dict{vm::load_cell_slice_ref(extra.account_blocks), 256,
                                                        block::tlb::aug_ShardAccountBlocks, false};

            bool eof = false;
            bool allow_same = true;
            ton::LogicalTime reverse = 0;
            td::Bits256 cur_addr;
            cur_addr.set_zero_s();

            while (!eof) {
                td::Ref<vm::CellSlice> value;
                try {
                    value = account_blocks_dict.extract_value(
                            account_blocks_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, !reverse, allow_same));
                } catch (vm::VmError &err) {
                    std::cout << "error while traversing account block dictionary: "s + err.get_msg() << "\n";
                    break;
                }
                if (value.is_null()) {
                    if (allow_same) {
                        allow_same = false;
                        continue;
                    }

                    eof = true;
                    break;
                }
                allow_same = false;

                block::gen::AccountBlock::Record acc_blk;
                if (!(tlb::csr_unpack(std::move(value), acc_blk))) {
                    std::cout << "invalid AccountBlock for account "s + cur_addr.to_hex() << "\n";
                } else {
                    uniq_accounts_addr[acc_blk.account_addr.to_hex()] = acc_blk.account_addr;
                    cur_addr = acc_blk.account_addr;
                }
            }
        } else {
            std::cout << "UNPACK ERROR\n";
        }

        // accounts state snapshot and write to stream
        vm::MerkleProofBuilder pb{root};
        block::gen::ShardStateUnsplit::Record sstate;
        if (!tlb::unpack_cell(pb.root(), sstate)) {
            std::cout << "cannot unpack state header" << std::endl;
            return td::Status::Error("Write_BlockStream_Failed");
        }
        vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts, false};

        for (auto & itr : uniq_accounts_addr) {
            auto acc_csr = accounts_dict.lookup(itr.second);

            if (acc_csr.not_null()) {
                block::gen::ShardAccount acc_info;
                block::gen::ShardAccount::Record acc_info_rec;
                if (!tlb::csr_unpack(std::move(acc_csr), acc_info_rec)) {
                    std::cout << "ERROR CELL UNPACK ShardAccount \n";
                    break;
                }

                // state serialize
                td::Ref<vm::Cell> state_cell;
                if (!acc_info.cell_pack(state_cell, acc_info_rec)) {
                    std::cout << "ERROR acc_info CELL PACK\n";
                    break;
                }

                auto state_boc = vm::std_boc_serialize(std::move(state_cell));
                if (state_boc.is_error()) {
                    std::cout << "ERROR state_boc std_boc_serialize\n";
                    std::cout << state_boc.move_as_error().to_string() << std::endl;
                    break;
                }

                // block header serialize
                block::gen::BlockInfo blk_info;
                td::Ref<vm::Cell> block_header_cell;
                if (!blk_info.cell_pack(block_header_cell, blk_info_rec)) {
                    std::cout << "ERROR blk_info CELL PACK\n";
                    break;
                }
                auto block_header_boc = vm::std_boc_serialize(block_header_cell);
                if (block_header_boc.is_error()) {
                    std::cout << "ERROR blk_info std_boc_serialize\n";
                    std::cout << block_header_boc.move_as_error().to_string() << std::endl;
                    break;
                }

                // Write data to stream
                auto block_header = block_header_boc.move_as_ok().as_slice().str();
                auto state_str = state_boc.move_as_ok().as_slice().str();
                auto block_header_length = uint32_t(block_header.length());

                std::ostringstream state_buffer;

                state_buffer.write(reinterpret_cast<const char *>(&block_header_length), sizeof(block_header_length));
                state_buffer << block_header << state_str;

                if (FileStreamWriter::get_instance_state().write(state_buffer.str())) {
                    //                        std::cout << "ShardStateQ apply_block (" << state_str.length() << ")" << block->block_id().id.to_str() << std::endl;
                } else {
                    std::cout << "Write blockStreamShardQ fail" << std::endl;
                    state_buffer.clear();
                    return td::Status::Error("Write_BlockStream_Failed");
                }
                state_buffer.clear();
            } else {
                std::cout << "ACC IS NULL: "s + itr.second.to_hex() << " skip\n";
            }
        }

        return td::Status::OK();
    }
};

}
}

#endif //TON_ACCOUNT_EXTRACTOR_HPP
