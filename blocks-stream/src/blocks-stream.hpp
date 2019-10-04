//
// Created by user on 9/23/19.
//

#include <fstream>
#include <mutex>
#include <iostream>

#ifndef TON_BLOCKSSTREAM_H
#define TON_BLOCKSSTREAM_H

using namespace std;

namespace ton {
namespace ext {

class BlocksStream {

public:
    static BlocksStream& GetInstance() {
        static BlocksStream instance("/tmp/testlog.log");
        return instance;
    };

    explicit BlocksStream(const std::string& fname)
        :fname(fname),fnameIndex(fname + ".index"){
        outfile.open(fname, std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary) ;
        outfileIndex.open(fnameIndex, std::ofstream::out | std::ofstream::app | std::ofstream::ate | std::ofstream::binary) ;
    };
    bool WriteData(const std::string &stream) {
        m.lock();
        try {
            outfile << stream;

            uint32_t num = uint32_t(stream.length());
            outfileIndex.write(reinterpret_cast<const char *>(&num), sizeof(num));
        } catch (const std::exception& e) {
            m.unlock();
            std::cout << e.what() << std::endl;
            return false;
        }
        m.unlock();

        return true;
    }

protected:
    const std::string fname;
    const std::string fnameIndex;
    std::ofstream outfile;
    std::ofstream outfileIndex;
    std::mutex m;

};

}
}

#endif //TON_BLOCKSSTREAM_H
