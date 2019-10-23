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

const uint32_t BUFFER_SIZE_BLOCK = 50 * 1024 * 1024;

template <class T> class BlockingQueue: public std::queue<T> {
public:
    BlockingQueue(int size) {
        maxSize = size;
    }

    void push(T item) {
        std::unique_lock<std::mutex> wlck(writerMutex);
        while(Full())
            isFull.wait(wlck);
        std::queue<T>::push(item);
        isEmpty.notify_all();
    }

    bool notEmpty() {
        return !std::queue<T>::empty();
    }

    bool Full(){
        return std::queue<T>::size() >= (uint64_t)maxSize;
    }

    T pop() {
        std::unique_lock<std::mutex> lck(readerMutex);
        while(std::queue<T>::empty()) {
            isEmpty.wait(lck);
        }
        T value = std::queue<T>::front();
        std::queue<T>::pop();
        if(!Full())
            isFull.notify_all();
        return value;
    }

private:
    int maxSize;
    std::mutex readerMutex;
    std::mutex writerMutex;
    std::condition_variable isFull;
    std::condition_variable isEmpty;
};

using boost::asio::ip::tcp;
//enum { max_length = 1000 * 1024 };


static volatile sig_atomic_t sig_caught = 0;
void signal_handler( int signal_num ) {
    std::cout << "The interrupt signal is (" << signal_num << "). \n";
//    auto sig = sig_caught.get();
//    sig = 1;

    sig_caught = 1;
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

int runSocket(BlockingQueue<std::string> *blocksQueue, std::string ws_host, std::string ws_port, int id) {

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

            if (message_count % 1000 == 0) {
                std::cout << id << "] Total messages send:" << message_count <<  "; queue size: " << blocksQueue->size() << "\n";
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

int runReader(BlockingQueue<std::string> *blocksQueue, std::ifstream *ifsLog, std::ifstream *ifsIndex, std::string logFileIndex, std::ofstream *ofsIndexSeek, long int index_seek) {
    std::vector<char> header_buffer(sizeof(uint32_t)); // create next block size buffer
    std::vector<char> block_buffer(BUFFER_SIZE_BLOCK); // create next block size buffer

    uint32_t data_size;
    std::ostringstream outp;

    vm::Ref<vm::Cell> root;
    td::Result<vm::Ref<vm::Cell>> res;
    long int rows = 0;
    long int index_seek_last = 0;

    while (sig_caught == 0) {
        if (!ifsIndex->read(&header_buffer[0], sizeof(uint32_t))) {
            ifsIndex->close();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            std::cout << "-";

            // reopen file and seek
            ifsIndex->open(logFileIndex, std::ifstream::in | std::ifstream::binary);
            if (!ifsIndex->good()) {
                ifsIndex->close();
                std::cout << "can not open file " << std::endl;
                return 1;
            }
            ifsIndex->seekg(index_seek_last, std::ios::beg);
            continue;
        }
        index_seek_last = ifsIndex->tellg();
        data_size = *(reinterpret_cast<uint32_t *>(header_buffer.data()));

        if (index_seek > index_seek_last) {
            ifsLog->seekg(data_size, std::ios::cur);
            rows ++;
//            if (rows % 50 == 0) {
//                std::cout << "skip last seek:" << index_seek_last << std::endl;
//            }
            continue;
        }

        try {
            if (!ifsLog->read(&block_buffer[0], data_size)) {
                std::cout << "CANNOT READ BLOCK: " << std::endl;
                continue;
            }
            std::string buff(&block_buffer[0], data_size);
            blocksQueue->push(buff);
        } catch (std::exception &e) {
            std::cout << "EXCEPTION catch" << std::endl;
            return 1;
        }
        header_buffer.clear();
        block_buffer.clear();

        ofsIndexSeek->seekp(0, std::ios::beg);
        ofsIndexSeek->write(reinterpret_cast<const char *>(&index_seek_last), sizeof(index_seek_last));
        ofsIndexSeek->flush();
//        ofsIndexSeek.clear();

        rows ++;
        if (rows % 50 == 0) {
            std::cout << "last seek:" << index_seek_last << std::endl;
        }
    }

    std::cout << "Reader end" << std::endl;
//    ifsLog->close();
//    ifsIndex->close();

    return 0;
}

// https://github.com/jupp0r/prometheus-cpp needed
int main(int argc, char *argv[]) {
    auto logFile = argv[1];
    auto logFileIndex = argv[2];
    std::string logFileIndexSeek = std::string(logFileIndex) + ".seek";

    auto ws_host = argv[3];
    auto ws_port = argv[4];

    std::cout << "log: " << logFile << std::endl;
    std::cout << "index: " << logFileIndex << std::endl;
    std::cout << "index seek file: " << logFileIndexSeek << std::endl;

    std::ifstream ifs (logFile, std::ifstream::in | std::ifstream::binary);
    if (!ifs.good()) {
        ifs.close();
        std::cout << "can not open file " << std::endl;
        return 1;
    }

    std::ifstream ifsIndex (logFileIndex, std::ifstream::in | std::ifstream::binary);
    if (!ifsIndex.good()) {
        ifsIndex.close();
        std::cout << "can not open file " << std::endl;
        return 1;
    }

    long int index_seek = 0;
    std::ifstream ifsIndexSeek (logFileIndexSeek, std::ifstream::in | std::ifstream::binary);
    if (ifsIndexSeek.good()) {
        // read index skip from file
        std::vector<char> index_seek_file(sizeof(long int));
        if (ifsIndexSeek.read(&index_seek_file[0], sizeof(long int))) {
            index_seek = *(reinterpret_cast<uint32_t *>(index_seek_file.data()));
            std::cout << "read seek from file: " << index_seek << std::endl;
        } else {
            std::cout << "error read ifsIndexSeek" << std::endl;
        }
        ifsIndexSeek.close();
    } else {
        ifsIndexSeek.close();
        std::cout << "Index seek file is empty, continue" << std::endl;
    }

    std::ofstream ofsIndexSeek (logFileIndexSeek, std::ofstream::in|std::ofstream::out|std::ofstream::binary| std::ofstream::ate);
    if (!ofsIndexSeek.good()) {
        ofsIndexSeek.close();
        std::cout << "can not open file ofsIndexSeek: " << logFileIndexSeek << std::endl;
        return 1;
    }

    //todo: refac
    BlockingQueue<std::string> blocksQueue(1000);
    std::thread t1(runSocket, &blocksQueue, ws_host, ws_port, 1);
    std::thread t2(runSocket, &blocksQueue, ws_host, ws_port, 2);
    std::thread t3(runSocket, &blocksQueue, ws_host, ws_port, 3);
    std::thread t4(runReader, &blocksQueue, &ifs, &ifsIndex, std::string(logFileIndex), &ofsIndexSeek, index_seek);

    void (*prev_handler)(int);
    prev_handler = signal(SIGTERM, signal_handler);
    while(1) {
        if (sig_caught) {
            std::cout << "caught a SIGTERM.\n";

            while (!blocksQueue.empty()) {
                std::cout << "Waiting for sending all data from buffers\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    t4.join();

    t1.detach();
    t2.detach();
    t3.detach();
    std::terminate();

    return 0;
}