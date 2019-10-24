//
// Created by user on 10/23/19.
//

#include <queue>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <atomic>

#ifndef TON_BLOCKING_QUEUE_HPP
#define TON_BLOCKING_QUEUE_HPP

namespace ton {
namespace ext {

template <typename T>
class BlockingQueue
{
protected:
    int                     max_size;
    std::mutex              d_mutex;
    std::condition_variable is_empty;
    std::condition_variable is_full;
    std::deque<T>           d_queue;
    std::atomic<bool>       is_closed;

public:
    BlockingQueue(int size): max_size(size){
        is_closed.store(false);
    }

    bool push(T const& value) {
        {
            std::unique_lock<std::mutex> lock(this->d_mutex);
            if (is_closed.load())
                return false;
            while (Full())
                is_full.wait(lock);
            d_queue.push_front(value);
        }
        this->is_empty.notify_one();
        return true;
    }

    T pop() {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->is_empty.wait(lock, [=]{ return !this->d_queue.empty() || this->is_closed.load(); });
        if (this->d_queue.size() == 0 && is_closed.load()) {
            return T();
        }

        T rc(std::move(this->d_queue.back()));
        this->d_queue.pop_back();
        return rc;
    }

    bool Full() {
        return d_queue.size() >= (uint64_t) max_size;
    }

    void Close() {
        is_closed.store(true);
    }

    int Size() {
        return this->d_queue.size();
    }
};

}
}
#endif //TON_BLOCKING_QUEUE_HPP
