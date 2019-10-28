//
// Created by user on 10/23/19.
//

#include <mutex>
#include <condition_variable>
#include <deque>
#include <atomic>

#include <queue>
#include <assert.h>

#ifndef TON_BLOCKING_QUEUE_HPP
#define TON_BLOCKING_QUEUE_HPP

namespace ton {
namespace ext {

// Based on https://www.justsoftwaresolutions.co.uk/threading/implementing-a-thread-safe-queue-using-condition-variables.html
template<typename Data>
class BlockingQueue {
private:
    std::queue<Data>            queue;
    mutable std::mutex        queue_mutex;
    const size_t                queue_limit;

    bool                        is_closed = false;

    std::condition_variable   new_item_or_closed_event;
    std::condition_variable   item_removed_event;

//#ifndef NDEBUG
//    size_t                      pushes_in_progress = 0;
//#endif

public:
    explicit BlockingQueue(size_t size_limit=0) : queue_limit(size_limit)
    {}

    void push(const Data& data)
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
//#ifndef NDEBUG
//        ++pushes_in_progress;
//#endif
        if (queue_limit > 0) {
            while (queue.size() >= queue_limit) {
                item_removed_event.wait(lock);
            }
        }
        assert (!is_closed);
        queue.push(data);
//#ifndef NDEBUG
//        --pushes_in_progress;
//#endif
        lock.unlock();

        new_item_or_closed_event.notify_one();
    }

    bool try_push(const Data& data)
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if (queue_limit > 0) {
            if (queue.size() >= queue_limit) {
                return false;
            }
        }
        assert (!is_closed);
        queue.push(data);
        lock.unlock();

        new_item_or_closed_event.notify_one();
        return true;
    }

    void close()
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        assert (!is_closed);
//#ifndef NDEBUG
//        assert (pushes_in_progress == 0);
//#endif
        is_closed = true;
        lock.unlock();

        new_item_or_closed_event.notify_all();
    }

    bool pop(Data &popped_value)
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        while (queue.empty()) {
            if (is_closed) {
                return false;
            }
            new_item_or_closed_event.wait(lock);
        }

        popped_value = queue.front();
        queue.pop();
        item_removed_event.notify_one();
        return true;
    }

    bool try_pop(Data &popped_value)
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if (queue.empty()) {
            return false;
        }

        popped_value = queue.front();
        queue.pop();
        item_removed_event.notify_one();
        return true;
    }

    bool empty() const
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        return queue.empty();
    }

    bool closed() const
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        return is_closed;
    }

    size_t limit() const
    {
        return queue_limit;
    }

    size_t size() const
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        return queue.size();
    }

};

/*

template <typename T>
class BlockingQueue
{
protected:
    int                     max_size;
    std::mutex              d_mutex;
    std::condition_variable is_empty;
    std::condition_variable is_full;
    std::deque<T>           d_queue;
    std::atomic<bool>       is_closed_flag;

public:
    BlockingQueue(int size): max_size(size){
        is_closed_flag.store(false);
    }

    bool push(T const& value) {
        {
            std::unique_lock<std::mutex> lock(d_mutex);

            if (is_closed())
                return false;

            while (full())
                is_full.wait(lock);

            if (is_closed())
                return false;

            d_queue.push_front(value);
        }
        this->is_empty.notify_one();
        return true;
    }

    bool pop(T &t) {
        std::unique_lock<std::mutex> lock(d_mutex);
//        if (d_queue.size() == 0 && is_closed()) {
//            return false;
//        }

        std::cout << "W";
        is_empty.wait_for(lock, std::chrono::milliseconds(200), [this]{ return !this->d_queue.empty() || this->is_closed(); });
        std::cout << "D";

        if (d_queue.size() == 0) {
            return false;
        }

        t = std::move(d_queue.back());
        d_queue.pop_back();
        lock.unlock();

        is_full.notify_one();

        return true;
    }

    bool full() {
        return (uint64_t)d_queue.size() >= (uint64_t) max_size;
    }

    void close() {
        is_closed_flag.store(true);
        this->is_empty.notify_all();
    }

    void notify() {
        this->is_empty.notify_all();
    }

    bool is_closed() {
        return is_closed_flag.load();
    }

    uint64_t size() {
        return (uint64_t)this->d_queue.size();
    }
};*/

}
}
#endif //TON_BLOCKING_QUEUE_HPP
