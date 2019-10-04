//
// Created by user on 9/23/19.
//

#include <sstream>
#include "src/blocks-stream.hpp"

int main(int argc, char *argv[]) {
    auto blocks_stream = new ton::ext::BlocksStream("/tmp/testlog.log");

    if ( !blocks_stream->WriteData("header", "super logn body") ) {
        cout << "write FAIL" << std::endl;
    } else {
        cout << "write success" << std::endl;
    }

    return 0;
}