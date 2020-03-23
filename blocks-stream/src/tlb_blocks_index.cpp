//
// Created by user on 2/22/20.
//

#include "tlb_blocks_index.hpp"
#include "stream-reader.hpp"

ton::ext::TlbBlocksIndex::TlbBlocksIndex(const std::string &_stream_filename, const std::string& _tlb_blocks_index_filename) :
        stream_filename(_stream_filename), tlb_blocks_index_filename(_tlb_blocks_index_filename) {
    open_files();
    blocks_index = std::map<ton::BlockId, BlockStreamMeta>();
}

void ton::ext::TlbBlocksIndex::open_files() {
    // Open stream log
    if (ifs_log.is_open()) {
        ifs_log.close();
    }
    ifs_log.open(stream_filename, std::ios::in | std::ios::binary);
    if (ifs_log.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to open "+stream_filename);
    }
    ifs_log.seekg(0,std::ios::beg);

    // Open tlb index
    if (ifs_tlb_index.is_open()) {
        ifs_tlb_index.close();
    }
    ifs_tlb_index.open(tlb_blocks_index_filename, std::ios::in | std::ios::binary);
    if (ifs_tlb_index.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to open "+tlb_blocks_index_filename);
    }

//    ifs_tlb_index.seekg(0,std::ios::beg);

    // Open tlb index
    if (ofs_tlb_index.is_open()) {
        ofs_tlb_index.close();
    }
    ofs_tlb_index.open(tlb_blocks_index_filename, std::ios::out|std::ios::binary|std::ios::app);
    if (ofs_tlb_index.fail()) {
        throw std::system_error(errno, std::system_category(), "failed to open "+tlb_blocks_index_filename);
    }
}

void ton::ext::TlbBlocksIndex::add_block_meta(ton::BlockId blocks_id, ton::ext::BlockStreamMeta block_meta) {
    blocks_index[blocks_id] = block_meta;
    write_index_block_meta(blocks_id, block_meta);
}

void ton::ext::TlbBlocksIndex::write_index_block_meta(ton::BlockId blocks_id, ton::ext::BlockStreamMeta block_meta) {
    auto buffer_size = sizeof(ton::BlockId) + sizeof(ton::ext::BlockStreamMeta);
    char buffer[sizeof(ton::BlockId) + sizeof(ton::ext::BlockStreamMeta)];
    auto offset = 0;

    // write block_id
    std::memcpy(&buffer[0], reinterpret_cast<const char *>(&blocks_id.workchain), sizeof(blocks_id.workchain));
    offset += sizeof(blocks_id.workchain);
    std::memcpy(&buffer[offset], reinterpret_cast<const char *>(&blocks_id.seqno), sizeof(blocks_id.seqno));
    offset += sizeof(blocks_id.seqno);
    std::memcpy(&buffer[offset], reinterpret_cast<const char *>(&blocks_id.shard), sizeof(blocks_id.shard));
    offset += sizeof(blocks_id.shard);

    // write block_meta
    std::memcpy(&buffer[offset], reinterpret_cast<const char *>(&block_meta.offset), sizeof(block_meta.offset));
    offset += sizeof(block_meta.offset);
    std::memcpy(&buffer[offset], reinterpret_cast<const char *>(&block_meta.size), sizeof(block_meta.size));
//    offset += sizeof(block_meta.size);

    if (!ofs_tlb_index.write(&buffer[0], buffer_size)) {
        throw std::system_error(errno, std::system_category(), "failed to write "+tlb_blocks_index_filename);
    }
    ofs_tlb_index.flush();
}

void ton::ext::TlbBlocksIndex::load_index_block_meta() {
    auto buffer_size = sizeof(ton::BlockId) + sizeof(ton::ext::BlockStreamMeta);
    char buffer[sizeof(ton::BlockId) + sizeof(ton::ext::BlockStreamMeta)];
    auto count_loadaed = 0;

    while(!ifs_tlb_index.eof()) {
        if (!ifs_tlb_index.read(buffer, buffer_size) && ifs_tlb_index.fail()  && ifs_tlb_index.gcount() != 0) {
            throw std::system_error(errno, std::system_category(), "failed to read "+tlb_blocks_index_filename);
        }
        if (ifs_tlb_index.gcount() == 0) {
            break;
        }
        if (ifs_tlb_index.gcount() < int(buffer_size)) {
            throw std::runtime_error("failed to read "+tlb_blocks_index_filename + "expected exactly buffer size got less");
        }

        ton::BlockId blocks_id{};
        ton::ext::BlockStreamMeta block_meta{};

        auto offset = 0;
        blocks_id.workchain =  *(reinterpret_cast<ton::WorkchainId *>(&buffer[offset]));
        offset += sizeof(blocks_id.workchain);
        blocks_id.seqno =  *(reinterpret_cast<ton::BlockSeqno *>(&buffer[offset]));
        offset += sizeof(blocks_id.seqno);
        blocks_id.shard =  *(reinterpret_cast<ton::ShardId *>(&buffer[offset]));
        offset += sizeof(blocks_id.shard);

        block_meta.offset =  *(reinterpret_cast<int64_t *>(&buffer[offset]));
        offset += sizeof(block_meta.offset);
        block_meta.size =  *(reinterpret_cast<int64_t *>(&buffer[offset]));

        blocks_index[blocks_id] = block_meta;

        if (count_loadaed == 0) {
            std::cout << "First block in index: " << blocks_id.to_str() << "\n";
        }
        count_loadaed++;
    }

    std::cout << "Total loaded rows from TLB index: " << count_loadaed << "\n";
}

td::BufferSlice ton::ext::TlbBlocksIndex::get_block_tlb(ton::BlockId blocks_id,std::vector<char> &block_buffer) {
//    auto block_meta = blocks_index[blocks_id];
    auto it = blocks_index.find(blocks_id);
    if( it == blocks_index.end() ) {
        throw std::runtime_error("not found block_id");
    }
    auto block_meta = it->second;

    ifs_log.seekg(block_meta.offset,std::ios::beg);

    if (!ifs_log.read(&block_buffer[0], block_meta.size) || ifs_log.gcount() != block_meta.size) {
        throw std::runtime_error("Error read block tlb. Loaded chunk is smaller then expected.");
    }

    return td::BufferSlice(&block_buffer[0], block_meta.size);
}

std::basic_string<char> ton::ext::TlbBlocksIndex::get_block_pretty_custom(ton::BlockId blocks_id,std::vector<char> &block_buffer) {
    return BlockConverter::bin_to_pretty_custom(get_block_tlb(blocks_id, block_buffer));
}

