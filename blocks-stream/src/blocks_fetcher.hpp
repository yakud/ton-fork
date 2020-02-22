//
// Created by user on 2/22/20.
//

#ifndef TON_BLOCKS_FETCHER_HPP
#define TON_BLOCKS_FETCHER_HPP

#include <boost/asio.hpp>
#include <iostream>
#include <utility>
#include "tlb_blocks_index.hpp"

using boost::asio::ip::tcp;

namespace ton {
namespace ext {

void session_start(tcp::socket sock, TlbBlocksIndex *blocks_index);

class BlocksFetcher {
protected:
    std::string host;
    unsigned short port;
    TlbBlocksIndex *blocks_index;

public:
    BlocksFetcher(std::string host, unsigned short port, TlbBlocksIndex *blocks_index);
    void run_server(boost::asio::io_context &io_context);
    void session(tcp::socket &&sock);
};

}
}


#endif //TON_BLOCKS_FETCHER_HPP
