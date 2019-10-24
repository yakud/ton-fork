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
#include <chrono>
#include <common/checksum.h>
#include <crypto/vm/dict.h>

#include <fstream>
#include <iostream>
#include <vector>
#include <td/utils/buffer.h>
#include <vm/boc.h>
#include <crypto/block/block-auto.h>

#include <crypto/block/block-parse.h>
#include <validator/impl/block.hpp>
#include <csignal>
#include <blocks-stream/src/blocks-reader.hpp>

using boost::asio::ip::tcp;

std::atomic<bool> sig_caught;
void signal_handler( int signal_num ) {
    std::cout << "The interrupt signal is (" << signal_num << "). \n";
    sig_caught.store(true);
}

std::basic_string<char> deserialize_block(const std::string& block_data, int size) {
    auto bs = td::BufferSlice(block_data.data(), std::size_t(size));
    auto res = vm::std_boc_deserialize(bs);
    if (res.is_error()) {
        std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
        return "";
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
    auto file_hash = td::sha256_bits256(bs);
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
                } else {
                    pp << "\n  custom:nothing";
                }
            } else {
                pp << "\n  custom:nothing";
            }
        }

        pp << "))";
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


    return outp.str();
}

int runSocket(ton::ext::BlockingQueue<std::string> *blocksQueue, std::string ws_host, std::string ws_port, int id) {

    try
    {
        boost::asio::io_service io_service;

        std::cout << "Start resolve " << ws_host << ":" << ws_port << "\n";
        tcp::resolver resolver(io_service);
        tcp::resolver::query query(tcp::v4(), ws_host, ws_port);
        tcp::resolver::iterator iterator = resolver.resolve(query);
        std::cout << "End resolve\n";
        std::cout << "Create socket\n";

        tcp::socket s(io_service);
        std::cout << "Socket connect\n";
        boost::asio::connect(s, iterator);
        std::cout << "Socket connected\n";

        uint32_t num;
        std::ostringstream msg_buffer;

        unsigned long int message_count = 0;
        unsigned int buffer_size = 0;
        std::string next_message;

        while (true) {
            next_message = blocksQueue->pop();
            auto next_message_pretty = deserialize_block(next_message, int(next_message.size()));
            if (next_message_pretty.empty()) {
                continue;
            }

            num = uint32_t(next_message_pretty.size());
            msg_buffer.write(reinterpret_cast<const char *>(&num), sizeof(num));
            msg_buffer.write(next_message_pretty.data(), num);
            buffer_size += sizeof(num) + num;
            message_count++;

            next_message.clear();

            // TODO: dynamic bulk buffer
//            if (message_count % 1 == 0 && buffer_size > 0) {
            if (!boost::asio::write(s, boost::asio::buffer( msg_buffer.str(), buffer_size))) {
                std::cout << "Send message error!" <<  "\n";
                return 1;
            }

            buffer_size = 0;
            msg_buffer.str("");
            msg_buffer.clear();

            if (message_count % 100 == 0) {
                std::cout << id << "] Total messages send:" << message_count << "\n";
            }
//            }
        }
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
        std::exit(1);
    }

    return 0;
}

// https://github.com/jupp0r/prometheus-cpp needed
int main(int argc, char *argv[]) {
    auto logFile = argv[1];
    auto logFileIndex = argv[2];
    auto ws_host = argv[3];
    auto ws_port = argv[4];

    ton::ext::BlockingQueue<std::string> queue(1000);
    ton::ext::BlocksReaderConfig conf = {
            .log_filename = logFile,
            .index_filename = logFileIndex
    };

    ton::ext::BlocksReader blocksReader(&conf, &queue);

    try {
        blocksReader.LoadSeek();
    } catch (std::system_error &e) {
        std::cout << "error load seek: " << e.what() << std::endl;
    }
    blocksReader.OpenFiles();

    std::cout << "start reading index from: " << conf.index_seek << std::endl;
    std::cout << "start reading log from: " << conf.log_seek << std::endl;
    std::cout << "log: " << logFile << std::endl;
    std::cout << "index: " << logFileIndex << std::endl;

    std::thread t1(runSocket, &queue, ws_host, ws_port, 1);
    std::thread t2(runSocket, &queue, ws_host, ws_port, 2);
    std::thread t3(runSocket, &queue, ws_host, ws_port, 3);
    auto reader_thread = blocksReader.Spawn();

    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
    while(true) {
        if (sig_caught.load()) {
            std::cout << "caught a SIG.\n";
            std::cout << "Waiting for sending all data from buffers\n";
            queue.Close();
            blocksReader.Stop();
            reader_thread.join();

            while (queue.Size() > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            std::cout << "Done!\n";

            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::terminate();

    return 0;
}