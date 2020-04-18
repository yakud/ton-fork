//
// Created by user on 4/18/20.
//

#ifndef TON_SHARD_STATE_DIFF_CPP
#define TON_SHARD_STATE_DIFF_CPP

#include <crypto/block/block-auto.h>
#include <validator/interfaces/block.h>
#include <crypto/vm/dict.h>
#include <crypto/block/block-parse.h>
#include <crypto/vm/cells/MerkleProof.h>

#include <utility>


namespace streamdb {

void foreach_block_uniq_account_addr(
        const td::Ref<ton::validator::BlockData>& block_data,
        std::function<void(td::Bits256)> each_account
    ) {
        std::map<std::string, td::BitArray<256>> uniq_accounts_addr;

        // extract accounts addr
        block::gen::BlockExtra::Record extra;
        block::gen::BlockInfo::Record info_rec;
        block::gen::Block::Record block_record;

        if (!tlb::unpack_cell(block_data->root_cell(), block_record) ||
            !tlb::unpack_cell(block_record.extra, extra) ||
            !tlb::unpack_cell(block_record.info, info_rec)) {
            throw std::runtime_error("error unpack block_data");
        }

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
                throw std::runtime_error("error while traversing account block dictionary: " + std::string(err.get_msg()));
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
                throw std::runtime_error("invalid AccountBlock for account " + std::string( cur_addr.to_hex()));
            }

            auto acc_hex = acc_blk.account_addr.to_hex();
            if (uniq_accounts_addr.find(acc_hex) == uniq_accounts_addr.end()) {
                each_account(acc_blk.account_addr);
            }

            uniq_accounts_addr[acc_hex] = acc_blk.account_addr;
            cur_addr = acc_blk.account_addr;
        }
    }

    block::gen::ShardStateUnsplit::Record make_shard_state_unsplit_diff(const block::gen::ShardStateUnsplit::Record& shard_state, const td::Ref<ton::validator::BlockData> &block_data) {
        block::gen::ShardStateUnsplit::Record shard_state_diff;

        vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(shard_state.accounts), 256, block::tlb::aug_ShardAccounts, false};
        vm::AugmentedDictionary accounts_diff{256, block::tlb::aug_ShardAccounts, false};

        foreach_block_uniq_account_addr(block_data, [&accounts_dict, &accounts_diff](td::BitArray<256> addr) {
            auto acc_csr = accounts_dict.lookup(addr);
            if (acc_csr.is_null()) {
                return;
            }

            accounts_diff.set(addr, acc_csr);
        });

        shard_state_diff.global_id = shard_state.global_id;
        shard_state_diff.shard_id = shard_state.shard_id;
        shard_state_diff.seq_no = shard_state.seq_no;
        shard_state_diff.vert_seq_no = shard_state.vert_seq_no;
        shard_state_diff.gen_utime = shard_state.gen_utime;
        shard_state_diff.gen_lt = shard_state.gen_lt;
        shard_state_diff.min_ref_mc_seqno = shard_state.min_ref_mc_seqno;
        shard_state_diff.out_msg_queue_info = shard_state.out_msg_queue_info;
        shard_state_diff.accounts = accounts_diff.get_root_cell();
        shard_state_diff.before_split = shard_state.before_split;
        shard_state_diff.r1 = shard_state.r1;
        shard_state_diff.custom = shard_state.custom;

        return shard_state_diff;
    }

}

#endif //TON_SHARD_STATE_DIFF_CPP
