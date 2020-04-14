//
// Created by user on 10/24/19.
//

#include <crypto/block/mc-config.h>
#include "block-converter.hpp"

std::basic_string<char> ton::ext::BlockConverter::bin_to_pretty_custom(td::BufferSlice block_data) {
    auto res = vm::std_boc_deserialize(block_data);
    if (res.is_error()) {
        std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
        return "";
    }

    // buffer
    std::ostringstream outp;
    tlb::PrettyPrinter pp{outp, 0};
    pp.limit = 409600;

    auto root = res.move_as_ok();

    // root
    bool is_special;
    auto cs = vm::load_cell_slice_special(root, is_special);
    if ( cs.fetch_ulong(32) != 0x11ef55aa ) {
        std::cout << "error load cell slice" << std::endl;
        return "";
    }

    outp << "(block";

    // global_id
    if (cs.have(32)) {
        auto block_global_id = cs.fetch_long(32);
        outp << " global_id:" << block_global_id << "\n";
    } else {
        std::cout << "error load global_id" << std::endl;
        return "";
    }

    // block info
    auto ref_info = cs.fetch_ref();
    block::gen::BlockInfo info;
    pp << " info:";
    info.print_ref(pp, ref_info);
    pp << "\n";

//    block::gen::BlockInfo::Record info_record;
//    if (info.cell_unpack(ref_info, info_record) ) {
//        pp << " info:";
//        info.print_ref(pp, ref_info);
//        pp << "\n";
//    } else {
//        std::cout << "error load block info" << std::endl;
//        return "";
//    }

    // block header
    auto root_hash = root->get_hash().bits();
    auto file_hash = td::sha256_bits256(block_data);
    pp << " header:(block_header"<<
       " root_hash:" << root_hash.to_hex(256) <<
       " file_hash:" << file_hash.to_hex() << ")\n";

    // value flow
    auto ref_value_flow = cs.fetch_ref();
    block::gen::ValueFlow value_flow;
    pp << " value_flow:";
    value_flow.print_ref(pp, ref_value_flow);
    pp << "\n";

//    block::gen::ValueFlow::Record value_flow_record;
//    if ( value_flow.cell_unpack(ref_value_flow, value_flow_record) ) {
//        pp << " value_flow:";
//        value_flow.print_ref(pp, ref_value_flow);
//        pp << "\n";
//    } else {
//        std::cout << "error load value flow" << std::endl;
//        return "";
//    }

    // state update
    auto ref_state_update = cs.fetch_ref();
    // skip state update

    // block extra
    auto ref_block_extra = cs.fetch_ref();
    block::gen::BlockExtra block_extra;
    block::gen::BlockExtra::Record block_extra_record;
    if ( block_extra.cell_unpack(ref_block_extra, block_extra_record) ) {
        pp << " extra:(block_extra\n";

        {
            pp << "   account_blocks:";
            block::gen::ShardAccountBlocks shard_account_blocks;
//            block::gen::ShardAccountBlocks::Record shard_account_blocks_record;
//            if (!shard_account_blocks.cell_unpack(block_extra_record.account_blocks, shard_account_blocks_record) ) {
//                std::cout << "error unpack account_blocks" << std::endl;
//                return "";
//            }
            shard_account_blocks.print_ref(pp, block_extra_record.account_blocks);
            pp << "\n";
        }

        pp << "   rand_seed:" << block_extra_record.rand_seed.to_hex();
        pp << "   created_by:" << block_extra_record.created_by.to_hex();

        {
            if (block_extra_record.custom.not_null()) {
                auto custom_ref = block_extra_record.custom->prefetch_ref();

                if (custom_ref.not_null()) {
                    block::gen::McBlockExtra mc_block_extra;
                    pp << "\n  custom:(just value:^";
                    mc_block_extra.print_ref(pp, custom_ref);
                    pp << ")";
                } else {
                    pp << "\n  custom:nothing";
                }
            } else {
                pp << "\n  custom:nothing";
            }
        }

        pp << ")";
    } else {
        std::cout << "error load block extra" << std::endl;
        return "";
    }

    // transactions_hash
    pp << "\n  transactions_hash:(transactions_hash\n";
    vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(block_extra_record.account_blocks), 256,
                                     block::tlb::aug_ShardAccountBlocks};

    bool allow_same = true;
    bool reverse = false;
    td::Bits256 cur_addr{};
    while (true) {
        td::Ref<vm::CellSlice> value;
        try {
            value = acc_dict.extract_value(
                    acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, !reverse, allow_same));
        } catch (vm::VmError err) {
            std::cout << "error while traversing account block dictionary: " << err.get_msg() << std::endl;
            return "";
        }
        if (value.is_null()) {
            break;
        }
        allow_same = false;
        block::gen::AccountBlock::Record acc_blk;
        if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
            std::cout << "invalid AccountBlock for account " << cur_addr.to_hex() << std::endl;
            return "";
        }
        pp << "    " << cur_addr << ":(lthash\n";

        vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 64,
                                           block::tlb::aug_AccountTransactions};
        td::BitArray<64> cur_trans{};
        while (true) {
            td::Ref<vm::Cell> tvalue;
            try {
                tvalue = trans_dict.extract_value_ref(
                        trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64, !reverse));
            } catch (vm::VmError err) {
                std::cout << "error while traversing transaction dictionary of an AccountBlock: "<<  err.get_msg() << std::endl;
                return "";
            }
            if (tvalue.is_null()) {
//                    trans_lt_ = reverse;
                break;
            }

            pp << "      " << cur_trans.to_long() << ":" << tvalue->get_hash().bits().to_hex(256) << "\n";
        }

        pp << "  )\n";
    }

    pp << ")";
    /////////////////////////////////////////////////////////////////////////////////

    pp << ")";



//    auto config_res = block::ConfigInfo::extract_config(
//            root, block::ConfigInfo::needStateRoot | block::ConfigInfo::needValidatorSet |
//                  block::ConfigInfo::needShardHashes | block::ConfigInfo::needPrevBlocks);
//    if (config_res.is_error()) {
//        throw std::exception();
//    }
//    auto config_ = config_res.move_as_ok();
//    auto cv_root = config_->get_config_param(35, 34);
//    if (cv_root.not_null()) {
//        auto validators_ = block::Config::unpack_validator_set(std::move(cv_root));
//        auto validators = validators_.move_as_ok();
//
//        auto i = 0;
//        for (auto v : validators->list) {
//            std::cout << "validator: " << i++
//                << " pub_key: " << v.pubkey.as_slice().str()
//                << " weight: " << v.cum_weight;
//        }
//    }


    return outp.str();
}

ton::BlockId ton::ext::BlockConverter::bin_to_block_id(td::BufferSlice block_data) {
    auto res = vm::std_boc_deserialize(block_data);
    if (res.is_error()) {
        throw std::runtime_error("CANNOT DESERIALIZE BLOCK: " + res.move_as_error().error().public_message() );
    }

    auto root = res.move_as_ok();

    // root
    bool is_special;
    auto cs = vm::load_cell_slice_special(root, is_special);
    if ( cs.fetch_ulong(32) != 0x11ef55aa ) {
        throw std::runtime_error("error load cell slice");
    }

    // global_id
    if (!cs.have(32)) {
        throw std::runtime_error("error load global_id");
    }

    // block info
    auto ref_info = cs.fetch_ref();
    block::gen::BlockInfo info;
    block::gen::BlockInfo::Record info_record;
    if (info.cell_unpack(ref_info, info_record) ) {
        block::gen::ShardIdent shard_ident;
        block::gen::ShardIdent::Record shard_ident_record{};
        if (shard_ident.unpack(info_record.shard.write(), shard_ident_record)) {
            ton::BlockId block_id_td(
                    shard_ident_record.workchain_id,
                    (shard_ident_record.shard_prefix | (1ULL << (63 - shard_ident_record.shard_pfx_bits))),
                    info_record.seq_no
            );
            return block_id_td;
        } else {
            throw std::runtime_error("error load shard ident");
        }
    } else {
        throw std::runtime_error("error load block info");
    }
}

std::basic_string<char> ton::ext::BlockConverter::state_to_pretty_custom(td::BufferSlice header_data, td::BufferSlice state_data) {
    // buffer
    std::ostringstream outp;
    tlb::PrettyPrinter pp{outp, 0};
    pp.limit = 409600;

    // header_data
    auto block_header_res = vm::std_boc_deserialize(header_data);
    if (block_header_res.is_error()) {
        std::cout << "CANNOT DESERIALIZE STATE: " << block_header_res.move_as_error().error().public_message() << std::endl;
        return "";
    }

    auto block_header_root = block_header_res.move_as_ok();
    auto block_header_cs = vm::load_cell_slice(block_header_root);

    block::gen::BlockInfo blk_info;
    block::gen::BlockInfo::Record blk_info_rec;
    if (!blk_info.unpack(block_header_cs, blk_info_rec)) {
        std::cout << "cannot unpack BlockInfo" << std::endl;
        return "";
    }

    outp << "(account_state \n info:";

    td::Ref<vm::Cell> cc_block_info;
    blk_info.cell_pack(cc_block_info, blk_info_rec);
    blk_info.print_ref(pp, cc_block_info);

    // State data
    auto state_res = vm::std_boc_deserialize(state_data);
    if (state_res.is_error()) {
        std::cout << "CANNOT DESERIALIZE STATE: " << state_res.move_as_error().error().public_message() << std::endl;
        return "";
    }

    auto state_root = state_res.move_as_ok();
    auto state_cs = vm::load_cell_slice(state_root);

    block::gen::ShardAccount acc_info;
    block::gen::ShardAccount::Record acc_info_rec;
    if (!acc_info.unpack(state_cs, acc_info_rec)) {
        std::cout << "cannot unpack ShardAccount" << std::endl;
        return "";
    }

    outp << "\n state:";

    td::Ref<vm::Cell> cc;
    acc_info.cell_pack(cc, acc_info_rec);
    acc_info.print_ref(pp, cc);

    outp << ")";

    return outp.str();
}
