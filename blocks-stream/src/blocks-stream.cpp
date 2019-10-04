//
// Created by user on 9/23/19.
//

#include "blocks-stream.hpp"

//bool ton::ext::BlocksStream::WriteData(const std::string& stream) {
//    m.lock();
//    try {
//        auto num = uint32_t(stream.length());
//        outfile.write(reinterpret_cast<const char *>(&num), sizeof(num));
//        outfile << stream << std::endl;
//    } catch (const std::exception& e) {
//        m.unlock();
//        std::cout << e.what() << std::endl;
//        return false;
//    }
//    m.unlock();
//
//    return true;
//}
