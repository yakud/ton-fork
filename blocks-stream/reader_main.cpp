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
enum { max_length = 1000 * 1024 };

std::basic_string<char> deserialize_block(std::string block_data, int size) {
    auto res = vm::std_boc_deserialize(td::BufferSlice(block_data.data(), std::size_t(size)));
    if (res.is_error()) {
        std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
        return "";
    }

    auto root = res.move_as_ok();
    std::ostringstream outp;

//    now = std::chrono::system_clock::now();
    block::gen::t_Block.print_ref(outp, root);
//    mu = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - now);
//    meanPrintPretty += mu.count();
//    countPrintPretty++;

    return outp.str();
}

int runSocket(BlockingQueue<std::string> *blocksQueue, std::string ws_host, std::string ws_port, int id) {

    try
    {
        boost::asio::io_service io_service;

        std::cout << "Start resolve" << ws_host << ":" << ws_port << "\n";
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
        uint32_t num;
        std::ostringstream msg_buffer;

        unsigned long int message_count = 0;
        unsigned int buffer_size = 0;
        std::string next_message;

//        std::basic_string<char>
//        std::vector<char> msg_buffer(104857600);

        while (true) {
            next_message = blocksQueue->pop();


//            auto now = std::chrono::system_clock::now();
            auto next_message_pretty = deserialize_block(next_message, int(next_message.size()));
//            auto diffMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - now);
//            std::cout << id << "] deserialize_block for:  " << diffMs.count() << " ms; " << int(next_message_pretty.size()) << "; rate: "
//                << float(next_message.size()) / float(diffMs.count()) << "\n";

            /*
            num = uint32_t(next_message.size());
            std::ostringstream msg_buffer;
            msg_buffer.write(reinterpret_cast<const char *>(&num), sizeof(num));

            if (!boost::asio::write(s, boost::asio::buffer(msg_buffer.str(), sizeof(num) ))) {
                std::cout << "Send message error index!" <<  "\n";
                return 1;
            }
            msg_buffer.str("");
            msg_buffer.clear();
            if (!boost::asio::write(s, boost::asio::buffer( next_message, num))) {
                std::cout << "Send message error data!" <<  "\n";
                return 1;
            }
            next_message.clear();

            message_count++;
            if (message_count % 100 == 0) {
                std::cout  << id << "] Total messages send:" << message_count <<  "; queue size: " << blocksQueue->size() << "\n";
            }

            -----*/
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

template <unsigned N>
double approxRollingAverage (double avg, double input) {
    avg -= avg/N;
    avg += input/N;
    return avg;
}

// https://github.com/jupp0r/prometheus-cpp needed
int main(int argc, char *argv[]) {
    auto logFile = argv[1];
    auto logFileIndex = argv[2];

    errno = 0;
    char *endptr;
    long int index_seek = strtol(argv[3], &endptr, 10);
    if (endptr == argv[3]) {
        std::cerr << "Invalid number seek: " << argv[3] << '\n';
    } else if (*endptr) {
        std::cerr << "Trailing characters after number: " << argv[3] << '\n';
    } else if (errno == ERANGE) {
        std::cerr << "Number out of range: " << argv[3] << '\n';
    }

    auto ws_host = argv[4];
    auto ws_port = argv[5];

    std::cout << "Index seek: " << index_seek << std::endl;
    std::cout << "Start reading log: " << logFile << std::endl;
    std::cout << "Start reading index: " << logFileIndex << std::endl;

    BlockingQueue<std::string> blocksQueue(1000);
//    for (auto i = 0; i < 5; i ++) {
//        std::thread(runSocket, &blocksQueue, ws_host, ws_port, i);
//    }
    std::thread t1(runSocket, &blocksQueue, ws_host, ws_port, 1);
    std::thread t2(runSocket, &blocksQueue, ws_host, ws_port, 2);
    std::thread t3(runSocket, &blocksQueue, ws_host, ws_port, 3);
//    std::thread t4(runSocket, &blocksQueue, ws_host, ws_port, 4);
//    std::thread t5(runSocket, &blocksQueue, ws_host, ws_port, 5);
//    std::thread t6(runSocket, &blocksQueue, ws_host, ws_port, 6);
//    std::thread t7(runSocket, &blocksQueue, ws_host, ws_port, 7);
//    std::thread t8(runSocket, &blocksQueue, ws_host, ws_port, 8);
//    std::thread t9(runSocket, &blocksQueue, ws_host, ws_port, 9);
//    std::thread t10(runSocket, &blocksQueue, ws_host, ws_port, 10);
//    std::thread t11(runSocket, &blocksQueue, ws_host, ws_port, 11);
//    std::thread t12(runSocket, &blocksQueue, ws_host, ws_port, 12);
//    std::thread t13(runSocket, &blocksQueue, ws_host, ws_port, 13);
//    std::thread t14(runSocket, &blocksQueue, ws_host, ws_port, 14);
//    std::thread t15(runSocket, &blocksQueue, ws_host, ws_port, 15);

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

    std::vector<char> header_buffer(sizeof(uint32_t)); // create next block size buffer
    std::vector<char> block_buffer(BUFFER_SIZE_BLOCK); // create next block size buffer

//    if (index_seek > 0) {
//        ifsIndex.seekg(index_seek-sizeof(uint32_t), std::ios::beg);
//
//        if (!ifsIndex.read(&header_buffer[0], sizeof(uint32_t))) {
//            std::cout << "can not open file " << std::endl;
//            return 1;
//        }
//        index_seek = ifsIndex.tellg();
//        auto data_size = *(reinterpret_cast<uint32_t *>(header_buffer.data()));
//    }


    uint32_t data_size;
    std::ostringstream outp;

    vm::Ref<vm::Cell> root;
    td::Result<vm::Ref<vm::Cell>> res;
    long int rows = 0;

    std::chrono::time_point<std::chrono::system_clock> now;
    long meanPrintPretty = 0;
    long countPrintPretty = 0;
    std::chrono::duration<int64_t, std::ratio<1,1000>> mu;
    long int index_seek_last = 0;

    while (true) {
        if (!ifsIndex.read(&header_buffer[0], sizeof(uint32_t))) {
            ifsIndex.close();
            sleep(1);
            std::cout << "sleep after read index" << std::endl;

            // reopen file and seek
            ifsIndex.open(logFileIndex, std::ifstream::in | std::ifstream::binary);
            if (!ifsIndex.good()) {
                ifsIndex.close();
                std::cout << "can not open file " << std::endl;
                return 1;
            }
            ifsIndex.seekg(index_seek_last, std::ios::beg);
            continue;
        }
        index_seek_last = ifsIndex.tellg();
        data_size = *(reinterpret_cast<uint32_t *>(header_buffer.data()));

        if (index_seek > index_seek_last) {
            ifs.seekg(data_size, std::ios::cur);
            rows ++;
            if (rows % 100 == 0) {
                std::cout << "skip last seek:" << index_seek_last << std::endl;
            }
            continue;
        }

        try {
            if (!ifs.read(&block_buffer[0], data_size)) {
                std::cout << "CANNOT READ BLOCK: " << std::endl;
                continue;
            }
            /*
            res = vm::std_boc_deserialize(td::BufferSlice(block_buffer.data(), std::size_t(data_size)));
            if (res.is_error()) {
                std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
                continue;
            }

            root = res.move_as_ok();

            now = std::chrono::system_clock::now();
            block::gen::t_Block.print_ref(outp, root);
            mu = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - now);
            meanPrintPretty += mu.count();
            countPrintPretty++;
             */

            std::string buff(&block_buffer[0], data_size);
            blocksQueue.push(buff);

            /*
            outp.str("");
            outp.clear();
            root.release();
            res.clear();
             */

//            print_block(block_buffer.data(), data_size);
        } catch (std::exception &e) {
            std::cout << "EXCEPTION catch" << std::endl;
            break;
        }
        header_buffer.clear();
        block_buffer.clear();

        rows ++;
        if (rows % 50 == 0) {
            std::cout << "last seek:" << index_seek_last << std::endl;
        }
    }

    std::cout << "Read end" << std::endl;
    ifs.close();

//    t1.join();
//    t2.join();

    return 0;
}