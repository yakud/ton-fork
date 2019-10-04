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
#include <boost/asio.hpp>

void parse_block(const td::Ref<vm::Cell>& root) {
    std::ostringstream outp;
    tlb::PrettyPrinter pp{std::cout, 0};

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
    if ( info.cell_unpack(ref_info, info_record) ) {
        std::cout << "Cell info unpack success:" << std::endl;
        info.print_ref(pp, ref_info);
        std::cout << std::endl;
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
    // do not interesting

    // Block extra
    auto ref_block_extra = cs.fetch_ref();
    block::gen::BlockExtra block_extra;
    block::gen::BlockExtra::Record block_extra_record;
    if ( block_extra.cell_unpack(ref_block_extra, block_extra_record) ) {
        std::cout << "Block extra unpack success" << std::endl;
//        block_extra.print_ref(pp, ref_block_extra);
//        std::cout << std::endl;

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


                    out_msg_descr.print_ref(pp, block_extra_record.out_msg_descr);

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
//
//    t_BlockExtra.print_ref(pp, cs.fetch_ref())

//    pp.field("extra")
//    && t_BlockExtra.print_ref(pp, cs.fetch_ref())


    // field info
//    block::gen::t_BlockInfo.print_ref(pp, )
//    auto cs_info = load_cell_slice_special(ref_info, is_special);
//
//
//
//    t_BlockInfo.print_ref(pp, cs.fetch_ref())
////
//    pp.field("info")
//    &&

//    pp.open("block") &&
//    pp.fetch_int_field(cs, 32, "global_id");

//    print_skip(pp, cs) && (cs.empty_ext() || pp.fail("extra data in cell"));

    /*
       *
       * bool Block::print_skip(PrettyPrinter& pp, vm::CellSlice& cs) const {
    return cs.fetch_ulong(32) == 0x11ef55aa
        && pp.open("block")
        && pp.fetch_int_field(cs, 32, "global_id")
        && pp.field("info")
        && t_BlockInfo.print_ref(pp, cs.fetch_ref())
        && pp.field("value_flow")
        && t_ValueFlow.print_ref(pp, cs.fetch_ref())
        && pp.field("state_update")
        && t_MERKLE_UPDATE_ShardState.print_ref(pp, cs.fetch_ref())
        && pp.field("extra")
        && t_BlockExtra.print_ref(pp, cs.fetch_ref())
        && pp.close();
  }
       */

//        cs.fetch_ulong(32) == 0x11ef55aa
//        && pp.open("block")
//        && pp.fetch_int_field(cs, 32, "global_id")
//        && pp.field("info")
//        && t_BlockInfo.print_ref(pp, cs.fetch_ref())
//        && pp.field("value_flow")
//        && t_ValueFlow.print_ref(pp, cs.fetch_ref())
//        && pp.field("state_update")
//        && t_MERKLE_UPDATE_ShardState.print_ref(pp, cs.fetch_ref())
//        && pp.field("extra")
//        && t_BlockExtra.print_ref(pp, cs.fetch_ref())
//        && pp.close();



//        if (is_special) {
//            return print_special(pp, cs);
//        } else {
//
//        }
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

    if (!(tlb::unpack_cell(root, blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
        std::cout <<"CANNOT UNPACK HEADER FOR BLOCK " << std::endl;
    } else {
        std::cout << "block contents is " << std::endl;

        parse_block(root);

//        std::ostringstream outp;
//        block::gen::t_Block.print_ref(outp, root);
//        vm::load_cell_slice(root).print_rec(outp);
//        std::cout << outp.str();
    }


}

const uint32_t BUFFER_SIZE_BLOCK = 10485760;

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
enum { max_length = 1000 * 1024 };

int runSocket(BlockingQueue<std::string> *blocksQueue) {
    std::string ws_host = "0.0.0.0";
    std::string ws_port = "7315";

    std::string request = "Hello my GO!";

    try
    {
        boost::asio::io_service io_service;

        std::cout << "Start resolve\n";
        tcp::resolver resolver(io_service);
        tcp::resolver::query query(tcp::v4(), ws_host, ws_port);
        tcp::resolver::iterator iterator = resolver.resolve(query);
        std::cout << "End resolve\n";
        std::cout << "Create socket\n";

        tcp::socket s(io_service);
        std::cout << "Socket connect\n";
        boost::asio::connect(s, iterator);
        std::cout << "Socket connected\n";

//        std::string nextMessage;
        while (1) {
            std::ostringstream msg_buffer;
            auto nextMessage = blocksQueue->pop();

            auto num = uint32_t(nextMessage.size());
            msg_buffer.write(reinterpret_cast<const char *>(&num), sizeof(num));
            msg_buffer << nextMessage;

            if (auto writed = boost::asio::write(s,
                    boost::asio::buffer(
                            msg_buffer.str(), sizeof(num) + nextMessage.size() * sizeof(std::string::value_type))
            ) == 0) {
                break;
            } else {
                std::cout << "Send message: " <<  nextMessage.size() <<  "\n";
            }
        }

//        boost::asio::write(s, boost::asio::buffer(request, request.size() * sizeof(std::string::value_type)));
//
//        std::cout << "Done write\n";
//        char data[max_length];
//        boost::system::error_code error;
//        size_t length = s.read_some(boost::asio::buffer(data), error);
//        if (error == boost::asio::error::eof) {
//            s.close();
//            std::cout << "Connection closed cleanly by peer." << std::endl;
//            return 0;
//        }
//
//        std::cout << "Reply is: ";
//        std::cout.write(data, length);
//        std::cout << "\n";
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}


int main(int argc, char *argv[]) {
    BlockingQueue<std::string> blocksQueue(1000);

//    runSocket(blocksQueue);

//    blocksQueue.push("hello");
//    blocksQueue.push("world");
//    blocksQueue.push("from C++");

    std::thread t1(runSocket, &blocksQueue);

//    t1.join();

    // scp akisilev@46.4.4.150:/tmp/testlog.log .
//    std::ifstream ifs ("/Users/user/ton-src/ton/blocks-stream/testlog.log", std::ifstream::in | std::ifstream::binary);
    std::ifstream ifs ("/tmp/testlog.log", std::ifstream::in | std::ifstream::binary);
    if (!ifs.good()) {
        ifs.close();
        std::cout << "can not open file " << std::endl;
        return 1;
    }

    std::ifstream ifsIndex ("/tmp/testlog.log.index", std::ifstream::in | std::ifstream::binary);
    if (!ifsIndex.good()) {
        ifsIndex.close();
        std::cout << "can not open file " << std::endl;
        return 1;
    }

    std::vector<char> header_buffer(sizeof(uint32_t)); // create next block size buffer
    std::vector<char> block_buffer(BUFFER_SIZE_BLOCK); // create next block size buffer

//    while (!ifsIndex.eof()) {
    while (true) {
        if (!ifsIndex.read(&header_buffer[0], sizeof(uint32_t))) {
            usleep(5*1000);
            continue;
        }
        auto data_size = *(reinterpret_cast<uint32_t *>(header_buffer.data()));
//        std::cout << "Data size:" << data_size << std::endl;

        try {
            if (!ifs.read(&block_buffer[0], data_size)) {
                std::cout << "CANNOT READ BLOCK: " << std::endl;
                continue;
            }

            auto res = vm::std_boc_deserialize(td::BufferSlice(block_buffer.data(), std::size_t(data_size)));
            if (res.is_error()) {
                std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
                continue;
//                return 0;
            }

            auto root = res.move_as_ok();
            block::gen::Block::Record blk;
            block::gen::BlockExtra::Record extra;
            block::gen::BlockInfo::Record info;

            if (!(tlb::unpack_cell(root, blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
                std::cout <<"CANNOT UNPACK HEADER FOR BLOCK " << std::endl;
            } else {
//                std::cout << "block contents is " << std::endl;
                std::ostringstream outp;
                block::gen::t_Block.print_ref(outp, root);
                blocksQueue.push(outp.str());
//                std::cout << outp.str();
//                break;
            }

//            print_block(block_buffer.data(), data_size);
        } catch (std::exception &e) {
            std::cout << "EXCEPTION catch" << std::endl;
            break;
        }
        header_buffer.clear();
        block_buffer.clear();
    }

    std::cout << "Read end" << std::endl;
    ifs.close();

    t1.join();

    return 0;
}