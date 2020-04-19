//
// Created by user on 4/19/20.
//

#ifndef TON_STREAM_WRITER_HPP
#define TON_STREAM_WRITER_HPP

#include "block_bucket.hpp"
#include "blocking_queue.hpp"
#include "streamdb.hpp"

namespace streamdb {

const auto BUFFER_SIZE = 50 * 1024 * 1024; // 50 MiB

class StreamWriter {
public:
    StreamWriter(): db(nullptr){}

    ~StreamWriter() {
        close();
    };

    void init(streamdb::DB *_db) {
        db = _db;
    }

    bool write(streamdb::BlockBucket *block_bucket) {
        queue->push(block_bucket);
        return true;
    }

    void writer() {
        std::cout << "Starting stream writer\n";
        ton::ext::BlockingQueue<streamdb::BlockBucket*> bq(10000);
        queue = &bq;

        streamdb::BlockBucket *block_bucket = nullptr;
        char buffer[BUFFER_SIZE];

        while (true) {
            if (need_stop) {
                break;
            }
            if (!queue->pop(block_bucket)) {
                if (queue->closed()) {
                    break;
                }
                continue;
            }

            std::cout << "Poped new value!\n";

            if (block_bucket == nullptr) {
                continue;
            }

            try {
                auto size = db->write_bucket(std::move(*block_bucket), &buffer[0], BUFFER_SIZE);
                if (size == 0) {
                    throw std::runtime_error("failed to write_bucket block_bucket");
                }

                std::cout << "bucket size " << size << " writed\n";
            } catch (std::exception& e) {
                std::cerr << e.what() << "\n";
                continue;
            }
        }

        bq.close();
    }

    ton::ext::BlockingQueue<streamdb::BlockBucket*>* get_queue() {
        return queue;
    }

    void close() {
        need_stop.store(true);
    }

protected:
    ton::ext::BlockingQueue<streamdb::BlockBucket*> *queue{};
    std::atomic<bool> need_stop{false};

    streamdb::DB *db;
};


class StreamWriterGlobal {
public:
    static StreamWriter& get_instance() {
        static StreamWriter instanceState{};
        return instanceState;
    };
};

}


#endif //TON_STREAM_WRITER_HPP
