//
// Created by user on 9/24/19.
//

#include <fstream>
#include <iostream>
#include <vector>
#include <td/utils/buffer.h>
#include <vm/boc.h>
#include <crypto/block/block-auto.h>

#include <queue>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <condition_variable>
#include <string>
#include <thread>
#include <mutex>
#include <boost/asio.hpp>
#include <cstdlib>
#include <crypto/vm/cells/MerkleUpdate.h>
#include <crypto/vm/dict.h>
#include <crypto/block/block-parse.h>
#include <validator/impl/block.hpp>
#include <common/checksum.h>
#include <blocks-stream/src/blocks-reader.hpp>

void parse_block(const td::Ref<vm::Cell>& root) {
    std::ostringstream outp;
    tlb::PrettyPrinter pp{std::cout, 0};

//    root->get_hash()

    // root
    bool is_special;
    auto cs = vm::load_cell_slice_special(root, is_special);
    if ( cs.fetch_ulong(32) == 0x11ef55aa ) {
        std::cout << "cs.fetch_ulong(32) == 0x11ef55aa" << std::endl;
    }

    // (block global_id:-17
    if (cs.have(32)) {
        auto block_global_id = cs.fetch_long(32);
        std::cout << "Block global_id: " << block_global_id << std::endl;
    }

    // block info
    auto ref_info = cs.fetch_ref();
    block::gen::BlockInfo info;
    block::gen::BlockInfo::Record info_record;
    block::gen::ShardIdent shard_ident;
//    block::gen::ShardIdent::Record shard_ident_record;
    if ( info.cell_unpack(ref_info, info_record) ) {
        std::cout << "Cell info unpack success:" << std::endl;
//        info.print_ref(pp, ref_info);
        std::cout << std::endl;

        block::ShardId shard;
        if (shard.deserialize(info_record.shard.write())) {
            std::cout << "workchain_id: " << shard.workchain_id << std::endl;
            std::cout << "shard: " << shard.shard_pfx << std::endl;
            std::cout << "seqNo: " << info_record.seq_no << std::endl;
        } else {
            std::cout << "ERROR deserialize shard" << std::endl;
        }
    }

    // value flow
    auto ref_value_flow = cs.fetch_ref();
    block::gen::ValueFlow value_flow;
    block::gen::ValueFlow::Record value_flow_record;
    if ( value_flow.cell_unpack(ref_value_flow, value_flow_record) ) {
        std::cout << "Value flow unpack success" << std::endl;
//        value_flow.print_ref(pp, ref_info);
    }

    // state update
    auto ref_state_update = cs.fetch_ref();

    // Block extra
    auto ref_block_extra = cs.fetch_ref();
    block::gen::BlockExtra block_extra;
    block::gen::BlockExtra::Record block_extra_record;
    if ( block_extra.cell_unpack(ref_block_extra, block_extra_record) ) {
        std::cout << "Block extra unpack success" << std::endl;

        std::cout << std::endl;

        // account_blocks
        /////////////////////////////////////////////////////////////////////////////////
        vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(block_extra_record.account_blocks), 256,
                                         block::tlb::aug_ShardAccountBlocks};

        int count = 0;
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
                return;
            }
            if (value.is_null()) {
                break;
            }
            allow_same = false;
            block::gen::AccountBlock::Record acc_blk;
            if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
                std::cout << "invalid AccountBlock for account " << cur_addr.to_hex() << std::endl;
                return;
            }
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
                    return;
                }
                if (tvalue.is_null()) {
//                    trans_lt_ = reverse;
                    break;
                }
                std::cout << "cur adr: " << cur_addr << " cur_trans: " << cur_trans.to_long() << " tvalue:" << tvalue->get_hash().bits().to_hex(256) << std::endl;
//                result.push_back(create_tl_object<lite_api::liteServer_transactionId>(mode, cur_addr, cur_trans.to_long(),
//                                                                                      tvalue->get_hash().bits()));
                ++count;
            }
        }
        /////////////////////////////////////////////////////////////////////////////////

        // In message
        block::gen::InMsgDescr in_msg_descr;
        block::gen::InMsgDescr::Record in_msg_descr_record;
        if ( in_msg_descr.cell_unpack(block_extra_record.in_msg_descr, in_msg_descr_record) ) {
            std::cout << "Block extra InMsgDescr unpack success" << std::endl;
        }

        // Out message
        block::gen::OutMsgDescr out_msg_descr;
        block::gen::OutMsgDescr::Record out_msg_descr_record;
        if ( out_msg_descr.cell_unpack(block_extra_record.out_msg_descr, out_msg_descr_record) ) {
            std::cout << "Block extra OutMsgDescr unpack success" << std::endl;

            block::gen::OutMsg out_msg;
            block::gen::CurrencyCollection currency_collection;
            block::gen::CurrencyCollection::Record currency_collection_record;
            block::gen::HashmapAugE out_msg_HashmapAugE(256, out_msg, currency_collection);
            block::gen::HashmapAugE::Record_ahme_empty ahme_empty;
            block::gen::HashmapAugE::Record_ahme_root ahme_root;

            auto cs_out_msg_descr = vm::load_cell_slice_special(block_extra_record.out_msg_descr, is_special);

            auto tag = out_msg_HashmapAugE.get_tag(cs_out_msg_descr);
            switch (tag) {
                case block::gen::HashmapAugE::ahme_empty:
                    if ( out_msg_HashmapAugE.unpack_ahme_empty(cs_out_msg_descr, ahme_empty.extra ) ) {
                        std::cout << "OutMsgDescr HashmapAugE is ahme_empty success" << std::endl;
                    } else {
                        std::cout << "OutMsgDescr HashmapAugE is ahme_empty failed" << std::endl;
                    }

                    break;

                case block::gen::HashmapAugE::ahme_root:
//                    std::cout << "Block extra OutMsgDescr HashmapAugE is ahme_root" << std::endl;
//
//                    if (out_msg_HashmapAugE.unpack(cs_out_msg_descr, ahme_root)) {
//                        std::cout << "OutMsgDescr HashmapAugE is ahme_root success" << std::endl;
//
//                        out_msg_HashmapAugE.print_ref(pp, cs_out_msg_descr.fetch_ref());
//                    } else {
//                        std::cout << "OutMsgDescr HashmapAugE is ahme_root failed" << std::endl;
//                    }


//                    out_msg_descr.print_ref(pp, block_extra_record.out_msg_descr);

                    break;

                default:
                    std::cout << "Block extra OutMsgDescr HashmapAugE is default:" << tag << std::endl;
            }

//            out_msg_descr.print_ref(pp, block_extra_record.out_msg_descr);
        }

    }
    /**
     * switch (get_tag(cs)) {
  case ahme_empty:
    return cs.advance(1)
        && pp.open("ahme_empty")
        && pp.field("extra")
        && Y_.print_skip(pp, cs)
        && pp.close();
  case ahme_root:
    return cs.advance(1)
        && pp.open("ahme_root")
        && pp.field("root")
        && HashmapAug{m_, X_, Y_}.print_ref(pp, cs.fetch_ref())
        && pp.field("extra")
        && Y_.print_skip(pp, cs)
        && pp.close();
  }
     */


    std::cout << std::endl;
}

void print_block(const char *data, uint32_t size) {
    auto res = vm::std_boc_deserialize(td::BufferSlice(data, std::size_t(size)));
    if (res.is_error()) {
        std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
        return;
    }

    auto root = res.move_as_ok();
    block::gen::Block::Record blk;
    block::gen::BlockExtra::Record extra;
    block::gen::BlockInfo::Record info;
    block::gen::ShardStateUnsplit::Record state;
    block::gen::ShardIdent::Record shardIdent{};

    if (!(tlb::unpack_cell(root, blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
        std::cout <<"CANNOT UNPACK HEADER FOR BLOCK " << std::endl;
    } else {

//        info->
//        auto blckId = ton::BlockIdExt{0, 0, 0, root->get_hash().bits(), td::sha256_bits256(td::BufferSlice(data, std::size_t(size)))};
//        std::cout << blckId.to_str() << "\n";

        /*
        std::cout << "BLOCK\n";
        vm::AugmentedDictionary account_blocks_dict{vm::load_cell_slice_ref(extra.account_blocks), 256,
                                                    block::tlb::aug_ShardAccountBlocks};

        bool eof = false;
        bool allow_same = false;
        bool reverse = false;
        td::Bits256 cur_addr;
        while (!eof) {
            td::Ref<vm::CellSlice> value;
            try {
                value = account_blocks_dict.extract_value(
                        account_blocks_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, !reverse,
                                                                                    allow_same));
            } catch (vm::VmError err) {
                std::cout << "error while traversing account block dictionary: \n";
            }
            if (value.is_null()) {
                eof = true;
                break;
            }

            block::gen::AccountBlock acc_blk;
            block::gen::AccountBlock::Record acc_blk_rec;
            if (!(tlb::csr_unpack(std::move(value), acc_blk_rec))) {
                std::cout << "invalid AccountBlock for account \n";
            } else {
                std::cout << "found account in block:" << acc_blk_rec.account_addr.to_hex() << "\n";
                cur_addr = acc_blk_rec.account_addr;
                td::Ref<vm::Cell> c;
                acc_blk.cell_pack(c, acc_blk_rec);
                acc_blk.print_ref(std::cout, c);
//                acc_blk.transactions
            }
        }
*/
//        BlockIdEx
//        auto blockQ = ton::validator::BlockQ(ton::BlockIdExt(), td::BufferSlice(data, std::size_t(size)));
//        auto blockQRoot = blockQ.root_cell();
//        vm::CellSlice cs{vm::NoVmOrd{}, blockQRoot};
//        if (cs.prefetch_ulong(32) != 0x11ef55aa || !cs.have_refs(4)) {
//            std::cout << "invalid shardchain block header for block\n";
//        } else {
//            std::cout << "success shardchain block header for block\n";
//        }
//        td::Ref<vm::Cell> update = cs.prefetch_ref(2);  // Merkle update
//        auto next_state_root = vm::MerkleUpdate::apply(blockQRoot, update);
//        if (next_state_root.is_null()) {
//            std::cout << "cannot apply Merkle update from block to previous state\n";
//        } else {
//            std::cout << "done apply Merkle update from block to previous state\n";
//        }

        std::cout << "block contents is " << std::endl;

        auto rhash = root->get_hash().bits();
        auto fhash = td::sha256_bits256(td::BufferSlice(data, std::size_t(size))).bits();
        std::cout << "rhash: " << rhash.to_hex(256) << " fhash: " << fhash.to_hex(256) << std::endl;
        parse_block(root);

//        std::ostringstream outp;
//        block::gen::t_Block.print_ref(std::cout, root);
//        vm::load_cell_slice(root).print_rec(outp);
//        std::cout << outp.str();
    }


}


/*
NEED TO HAVE LIKE:
(block
  global_id:...
  info:(...)
  header:(block_header
    root_hash:...
    file_hash:...
  )
  value_flow:(...)
  extra:(block_extra
    account_blocks:(...)
    custom:(...)
  )
  transactions_hash:(transactions_hash
    account1:(lthash lt:hash lt:hash)
    account2:(lthash lt:hash lt:hash)
  )
)

 */
void print_block_custom(const char *data, uint32_t size) {
    auto bs = td::BufferSlice(data, std::size_t(size));
    auto res = vm::std_boc_deserialize(bs);
    if (res.is_error()) {
        std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
        return;
    }

    // buffer
    std::ostringstream outp;
    tlb::PrettyPrinter pp{outp, 0};

    auto root = res.move_as_ok();

    // root
    bool is_special;
    auto cs = vm::load_cell_slice_special(root, is_special);
    if ( cs.fetch_ulong(32) != 0x11ef55aa ) {
        std::cout << "error load cell slice" << std::endl;
        return;
    }

    outp << "(block";

    // global_id
    if (cs.have(32)) {
        auto block_global_id = cs.fetch_long(32);
        outp << " global_id:" << block_global_id << "\n";
    } else {
        std::cout << "error load global_id" << std::endl;
        return;
    }

    // block info
    auto ref_info = cs.fetch_ref();
    block::gen::BlockInfo info;
    block::gen::BlockInfo::Record info_record;
    if (info.cell_unpack(ref_info, info_record) ) {
        pp << " info:";
        info.print_ref(pp, ref_info);
        pp << "\n";
    } else {
        std::cout << "error load block info" << std::endl;
        return;
    }

    // block header
    auto root_hash = root->get_hash().bits();
    auto file_hash = td::sha256_bits256(bs);
    pp << " header:(block_header"<<
        " root_hash:" << root_hash.to_hex(256) <<
        " file_hash:" << file_hash.to_hex() << ")\n";

    // value flow
    auto ref_value_flow = cs.fetch_ref();
    block::gen::ValueFlow value_flow;
    block::gen::ValueFlow::Record value_flow_record;
    if ( value_flow.cell_unpack(ref_value_flow, value_flow_record) ) {
        pp << " value_flow:";
        value_flow.print_ref(pp, ref_value_flow);
        pp << "\n";
    } else {
        std::cout << "error load value flow" << std::endl;
        return;
    }

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
            block::gen::ShardAccountBlocks::Record shard_account_blocks_record;
            if (!shard_account_blocks.cell_unpack(block_extra_record.account_blocks, shard_account_blocks_record) ) {
                std::cout << "error unpack account_blocks" << std::endl;
                return;
            }
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
                    pp << "\n  custom:";
                    mc_block_extra.print_ref(pp, custom_ref);
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
        return;
    }

    // transactions_hash
    pp << "\n  transactions_hash:(transactions_hash\n";
    vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(block_extra_record.account_blocks), 256,
                                     block::tlb::aug_ShardAccountBlocks};

    int count = 0;
    bool allow_same = false;
    bool reverse = false;
    td::Bits256 cur_addr{};
    while (true) {
        td::Ref<vm::CellSlice> value;
        try {
            value = acc_dict.extract_value(
                    acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, !reverse, allow_same));
        } catch (vm::VmError err) {
            std::cout << "error while traversing account block dictionary: " << err.get_msg() << std::endl;
            return;
        }
        if (value.is_null()) {
            break;
        }
        allow_same = false;
        block::gen::AccountBlock::Record acc_blk;
        if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
            std::cout << "invalid AccountBlock for account " << cur_addr.to_hex() << std::endl;
            return;
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
                return;
            }
            if (tvalue.is_null()) {
//                    trans_lt_ = reverse;
                break;
            }

            pp << "      " << cur_trans.to_long() << ":" << tvalue->get_hash().bits().to_hex(256) << "\n";

//            std::cout << "cur adr: " << cur_addr << " cur_trans: " << cur_trans.to_long() << " tvalue:" << tvalue->get_hash().bits().to_hex(256) << std::endl;
            ++count;
        }

        pp << "  )\n";
    }

    pp << ")";
    /////////////////////////////////////////////////////////////////////////////////

    pp << ")";

    if (info_record.seq_no == 1123366) {
        std::cout << outp.str() << std::endl;
    }
}


void run(ton::ext::BlockingQueue<std::string>* q) {
    std::string m;
    while (1) {
        if (!q->pop(m)) {
            std::cout << "CANT POP return\n";
            return;
        }

        std::cout << m << std::endl;
    }
}

int main(int argc, char *argv[]) {
    std::string  logFile = "/tmp/blocks.log.part";
    std::string logFileIndex = "/tmp/blocks.log.index.part";

    ton::ext::BlockingQueue<std::string> queue(100);

    std::thread t1(run, &queue);
    std::thread t2(run, &queue);
    std::thread t3(run, &queue);
    queue.push("1");
    queue.push("2");
    queue.push("3");
    queue.push("4");
    queue.push("5");

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    queue.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    t3.join();
    t1.join();
    t2.join();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "DONE!\n";

//    ton::ext::BlocksReaderConfig conf = {
//            .log_filename = logFile,
//            .index_filename = logFileIndex
//    };
//
//    ton::ext::BlocksReader blocksReader(&conf, &queue);
//
//    try {
//        blocksReader.load_seek();
//        blocksReader.open_files();
//    } catch (std::system_error &e) {
//        std::cout << "error load seek: " << e.what() << std::endl;
//    }
//
//    std::cout << "start reading index from: " << conf.index_seek << std::endl;
//    std::cout << "start reading log from: " << conf.log_seek << std::endl;
//
//    auto reader_thread = blocksReader.spawn();
//
//    int c = 10;
//    std::string msg;
//    while (--c > 0) {
//        if (!queue.pop(msg)) {
//            break;
//        }
//        print_block_custom(msg.c_str(), msg.size());
//        std::cout << "msg size: " << msg.size() << std::endl;
//    }
//    std::cout << "stopping..." << std::endl;
//    queue.close();
//    blocksReader.stop();
//
//    std::cout << "waiting reader..." << std::endl;
//    reader_thread.join();
//    std::cout << "stoped!" << std::endl;


//    blocksReader.Run


//    std::cout << "Start reading log: " << logFile << std::endl;
//    std::cout << "Start reading index: " << logFileIndex << std::endl;
//
//    std::ifstream ifs (logFile, std::ifstream::in | std::ifstream::binary);
//    if (!ifs.good()) {
//        ifs.close();
//        std::cout << "can not open file " << std::endl;
//        return 1;
//    }
//
//    std::ifstream ifsIndex (logFileIndex, std::ifstream::in | std::ifstream::binary);
//    if (!ifsIndex.good()) {
//        ifsIndex.close();
//        std::cout << "can not open file " << std::endl;
//        return 1;
//    }
//
//    std::ofstream ifsIndexSkip (logFileIndexSkip, std::ifstream::out | std::ifstream::binary);
//    if (!ifsIndex.good()) {
//        ifsIndex.close();
//        std::cout << "can not open file logFileIndexSkip" << std::endl;
//        return 1;
//    }
//
//
//
//    std::vector<char> header_buffer(sizeof(uint32_t)); // create next block size buffer
//    std::vector<char> block_buffer(50 * 1024 * 1024); // create next block size buffer
//
//    uint32_t data_size;
//    std::ostringstream outp;
//
//    vm::Ref<vm::Cell> root;
//    td::Result<vm::Ref<vm::Cell>> res;
//    long int rows = 0;
//    long int index_seek = 0;
//    long int index_seek_last = 0;
//
//    while (true) {
//        if (!ifsIndex.read(&header_buffer[0], sizeof(uint32_t))) {
//            ifsIndex.close();
//            sleep(1);
//            std::cout << "sleep after read index" << std::endl;
//
//            // reopen file and seek
//            ifsIndex.open(logFileIndex, std::ifstream::in | std::ifstream::binary);
//            if (!ifsIndex.good()) {
//                ifsIndex.close();
//                std::cout << "can not open file " << std::endl;
//                return 1;
//            }
//            ifsIndex.seekg(index_seek_last, std::ios::beg);
//            continue;
//        }
//        index_seek_last = ifsIndex.tellg();
//        data_size = *(reinterpret_cast<uint32_t *>(header_buffer.data()));
//
//        if (index_seek > index_seek_last) {
//            ifs.seekg(data_size, std::ios::cur);
//            rows ++;
//            if (rows % 100 == 0) {
//                std::cout << "skip last seek:" << index_seek_last << std::endl;
//            }
//            continue;
//        }
//
//        try {
//            if (!ifs.read(&block_buffer[0], data_size)) {
//                std::cout << "CANNOT READ BLOCK: " << std::endl;
//                exit(1);
//                continue;
//            }
////            std::string buff(&block_buffer[0], data_size);
////            blocksQueue.push(buff);
//            print_block_custom(&block_buffer[0], data_size);
//        } catch (std::exception &e) {
//            std::cout << "EXCEPTION catch" << std::endl;
//            break;
//        }
//        header_buffer.clear();
//        block_buffer.clear();
//
//        rows ++;
//        if (rows % 50 == 0) {
//            std::cout << "last seek:" << index_seek_last << std::endl;
//        }
//
//        ifsIndexSkip << index_seek_last;
//        ifsIndexSkip.flush();
//        ifsIndexSkip.clear();
//        ifsIndexSkip.seekp(0, std::ios::beg);
//        std::cout <<  index_seek_last << std::endl;
//    }
//
//    std::cout << "Read end" << std::endl;
//    ifs.close();
//
//
//    return 0;
}