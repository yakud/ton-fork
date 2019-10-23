//
// Created by user on 9/23/19.
//

#include <fstream>
#include <mutex>
#include <iostream>
#include <queue>

#ifndef TON_BLOCKSSTREAM_H
#define TON_BLOCKSSTREAM_H

using namespace std;

namespace ton {
namespace ext {

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

class BlocksStream {
public:
    static BlocksStream& GetInstance() {
        static BlocksStream instance{};
        return instance;
    };

    bool Init(const std::string& f) {
        outfileIndex.open(f + ".index", std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
        outfile.open(f, std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary | std::ios_base::app) ;
        return true;
    }

    BlocksStream() = default;
    ~BlocksStream() {
        outfile.close();
        outfileIndex.close();
        delete(blocksQueue);
    }

    bool WriteData(const std::string &stream) {
        blocksQueue->push(stream);
        return true;
    }

    void Writer() {
        // todo: params
        BlockingQueue<std::string> bq(10000);
        blocksQueue = &bq;

        std::ostringstream header_buffer;
        std::ostringstream body_buffer;

        // todo: graceful shutdown
        while (true) {
            if (need_stop) {
                break;
            }
            auto nextMessage = blocksQueue->pop();

            body_buffer << nextMessage;

            uint32_t num = uint32_t(nextMessage.length());
            header_buffer.write(reinterpret_cast<const char *>(&num), sizeof(num));

            outfile << body_buffer.str();
            outfile.flush();
            outfileIndex << header_buffer.str();
            outfileIndex.flush();

            body_buffer.str("");
            body_buffer.clear();
            header_buffer.str("");
            header_buffer.clear();
        }
    }

protected:
    BlockingQueue<std::string> *blocksQueue{};
    std::ofstream outfile;
    std::ofstream outfileIndex;
    bool need_stop = false;

};

}
}

#endif //TON_BLOCKSSTREAM_H
