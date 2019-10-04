/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2017-2019 Telegram Systems LLP
*/
#include <crypto/block/block-auto.h>
#include <crypto/block/block-parse.h>
#include "download-block.hpp"
#include "ton/ton-tl.hpp"
#include "adnl/utils.hpp"
#include "ton/ton-shard.h"
#include "td/utils/overloaded.h"
#include "ton/ton-io.hpp"
#include "validator/full-node.h"
#include "blocks-stream/src/blocks-stream.hpp"

namespace ton {

namespace validator {

namespace fullnode {

DownloadBlock::DownloadBlock(BlockIdExt block_id, adnl::AdnlNodeIdShort local_id, overlay::OverlayIdShort overlay_id,
                             adnl::AdnlNodeIdShort download_from, td::uint32 priority, td::Timestamp timeout,
                             td::actor::ActorId<ValidatorManagerInterface> validator_manager,
                             td::actor::ActorId<rldp::Rldp> rldp, td::actor::ActorId<overlay::Overlays> overlays,
                             td::actor::ActorId<adnl::Adnl> adnl, td::actor::ActorId<adnl::AdnlExtClient> client,
                             td::Promise<ReceivedBlock> promise)
    : block_id_(block_id)
    , local_id_(local_id)
    , overlay_id_(overlay_id)
    , download_from_(download_from)
    , priority_(priority)
    , timeout_(timeout)
    , validator_manager_(validator_manager)
    , rldp_(rldp)
    , overlays_(overlays)
    , adnl_(adnl)
    , client_(client)
    , promise_(std::move(promise))
    , block_{block_id, td::BufferSlice()}
    , allow_partial_proof_{!block_id_.is_masterchain()} {
}

DownloadBlock::DownloadBlock(BlockIdExt block_id, adnl::AdnlNodeIdShort local_id, overlay::OverlayIdShort overlay_id,
                             BlockHandle prev, adnl::AdnlNodeIdShort download_from, td::uint32 priority,
                             td::Timestamp timeout, td::actor::ActorId<ValidatorManagerInterface> validator_manager,
                             td::actor::ActorId<rldp::Rldp> rldp, td::actor::ActorId<overlay::Overlays> overlays,
                             td::actor::ActorId<adnl::Adnl> adnl, td::actor::ActorId<adnl::AdnlExtClient> client,
                             td::Promise<ReceivedBlock> promise)
    : block_id_(block_id)
    , local_id_(local_id)
    , overlay_id_(overlay_id)
    , prev_(prev)
    , download_from_(download_from)
    , priority_(priority)
    , timeout_(timeout)
    , validator_manager_(validator_manager)
    , rldp_(rldp)
    , overlays_(overlays)
    , adnl_(adnl)
    , client_(client)
    , promise_(std::move(promise))
    , block_{block_id, td::BufferSlice()} {
}

void DownloadBlock::abort_query(td::Status reason) {
  if (promise_) {
    if (reason.code() == ErrorCode::notready || reason.code() == ErrorCode::timeout) {
      VLOG(FULL_NODE_DEBUG) << "failed to download block " << block_id_ << "from " << download_from_ << ": " << reason;
    } else {
      VLOG(FULL_NODE_NOTICE) << "failed to download block " << block_id_ << " from " << download_from_ << ": "
                             << reason;
    }
    promise_.set_error(std::move(reason));
  }
  stop();
}

void DownloadBlock::alarm() {
  abort_query(td::Status::Error(ErrorCode::timeout, "timeout"));
}

void DownloadBlock::finish_query() {
  if (promise_) {
    promise_.set_value(std::move(block_));
  }
  stop();
}

void DownloadBlock::start_up() {
  alarm_timestamp() = timeout_;

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<BlockHandle> R) {
    if (R.is_error()) {
      auto S = R.move_as_error();
      if (S.code() == ErrorCode::notready) {
        td::actor::send_closure(SelfId, &DownloadBlock::got_block_handle, nullptr);
      } else {
        td::actor::send_closure(SelfId, &DownloadBlock::abort_query, std::move(S));
      }
    } else {
      td::actor::send_closure(SelfId, &DownloadBlock::got_block_handle, R.move_as_ok());
    }
  });

  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle, block_id_, false,
                          std::move(P));
}

void DownloadBlock::got_block_handle(BlockHandle handle) {
  handle_ = std::move(handle);

  if (handle_ && (handle_->inited_proof() || (handle_->inited_proof_link() && allow_partial_proof_) || skip_proof_) &&
      handle_->received()) {
    short_ = true;
    got_download_token(nullptr);
    return;
  }

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::unique_ptr<DownloadToken>> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &DownloadBlock::abort_query,
                              R.move_as_error_prefix("failed to get download token: "));
    } else {
      td::actor::send_closure(SelfId, &DownloadBlock::got_download_token, R.move_as_ok());
    }
  });
  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_download_token, 1, priority_, timeout_,
                          std::move(P));
}

void DownloadBlock::got_download_token(std::unique_ptr<DownloadToken> token) {
  token_ = std::move(token);

  if (download_from_.is_zero() && !short_ && client_.empty()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<adnl::AdnlNodeIdShort>> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
      } else {
        auto vec = R.move_as_ok();
        if (vec.size() == 0) {
          td::actor::send_closure(SelfId, &DownloadBlock::abort_query,
                                  td::Status::Error(ErrorCode::notready, "no nodes"));
        } else {
          td::actor::send_closure(SelfId, &DownloadBlock::got_node_to_download, vec[0]);
        }
      }
    });

    td::actor::send_closure(overlays_, &overlay::Overlays::get_overlay_random_peers, local_id_, overlay_id_, 1,
                            std::move(P));
  } else {
    got_node_to_download(download_from_);
  }
}

void DownloadBlock::got_node_to_download(adnl::AdnlNodeIdShort node) {
  download_from_ = node;
  if (skip_proof_ || (handle_ && (handle_->inited_proof() || (handle_->inited_proof_link() && allow_partial_proof_)))) {
    checked_block_proof();
    return;
  }

  VLOG(FULL_NODE_DEBUG) << "downloading proof for " << block_id_;

  CHECK(!short_);
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::BufferSlice> R) mutable {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
    } else {
      td::actor::send_closure(SelfId, &DownloadBlock::got_block_proof_description, R.move_as_ok());
    }
  });

  auto q = create_serialize_tl_object<ton_api::tonNode_prepareBlockProof>(create_tl_block_id(block_id_),
                                                                          allow_partial_proof_);
  if (client_.empty()) {
    td::actor::send_closure(overlays_, &overlay::Overlays::send_query, download_from_, local_id_, overlay_id_,
                            "get_prepare", std::move(P), td::Timestamp::in(1.0), std::move(q));
  } else {
    td::actor::send_closure(client_, &adnl::AdnlExtClient::send_query, "get_prepare",
                            create_serialize_tl_object_suffix<ton_api::tonNode_query>(std::move(q)),
                            td::Timestamp::in(1.0), std::move(P));
  }
}

void DownloadBlock::got_block_proof_description(td::BufferSlice proof_description) {
  VLOG(FULL_NODE_DEBUG) << "downloaded proof description for " << block_id_;

  auto F = fetch_tl_object<ton_api::tonNode_PreparedProof>(std::move(proof_description), true);
  if (F.is_error()) {
    abort_query(F.move_as_error());
    return;
  }

  auto self = this;
  ton_api::downcast_call(
      *F.move_as_ok().get(),
      td::overloaded(
          [&](ton_api::tonNode_preparedProof &obj) {
            auto P = td::PromiseCreator::lambda([SelfId = actor_id(self)](td::Result<td::BufferSlice> R) {
              if (R.is_error()) {
                td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
              } else {
                td::actor::send_closure(SelfId, &DownloadBlock::got_block_proof, R.move_as_ok());
              }
            });

            auto q = create_serialize_tl_object<ton_api::tonNode_downloadBlockProof>(create_tl_block_id(block_id_));
            if (client_.empty()) {
              td::actor::send_closure(overlays_, &overlay::Overlays::send_query_via, download_from_, local_id_,
                                      overlay_id_, "get_proof", std::move(P), td::Timestamp::in(3.0), std::move(q),
                                      FullNode::max_proof_size(), rldp_);
            } else {
              td::actor::send_closure(client_, &adnl::AdnlExtClient::send_query, "get_proof",
                                      create_serialize_tl_object_suffix<ton_api::tonNode_query>(std::move(q)),
                                      td::Timestamp::in(3.0), std::move(P));
            }
          },
          [&](ton_api::tonNode_preparedProofLink &obj) {
            if (!allow_partial_proof_) {
              abort_query(td::Status::Error(ErrorCode::protoviolation, "received partial proof, though did not allow"));
              return;
            }
            auto P = td::PromiseCreator::lambda([SelfId = actor_id(self)](td::Result<td::BufferSlice> R) {
              if (R.is_error()) {
                td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
              } else {
                td::actor::send_closure(SelfId, &DownloadBlock::got_block_partial_proof, R.move_as_ok());
              }
            });

            auto q = create_serialize_tl_object<ton_api::tonNode_downloadBlockProofLink>(create_tl_block_id(block_id_));
            if (client_.empty()) {
              td::actor::send_closure(overlays_, &overlay::Overlays::send_query_via, download_from_, local_id_,
                                      overlay_id_, "get_proof_link", std::move(P), td::Timestamp::in(3.0), std::move(q),
                                      FullNode::max_proof_size(), rldp_);
            } else {
              td::actor::send_closure(client_, &adnl::AdnlExtClient::send_query, "get_proof_link",
                                      create_serialize_tl_object_suffix<ton_api::tonNode_query>(std::move(q)),
                                      td::Timestamp::in(3.0), std::move(P));
            }
          },
          [&](ton_api::tonNode_preparedProofEmpty &obj) {
            abort_query(td::Status::Error(ErrorCode::notready, "proof not found"));
          }));
}

void DownloadBlock::got_block_proof(td::BufferSlice proof) {
  VLOG(FULL_NODE_DEBUG) << "downloaded proof for " << block_id_;

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
    } else {
      td::actor::send_closure(SelfId, &DownloadBlock::checked_block_proof);
    }
  });

  if (!prev_) {
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::validate_block_proof, block_id_,
                            std::move(proof), std::move(P));
  } else {
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::validate_block_is_next_proof, prev_->id(),
                            block_id_, std::move(proof), std::move(P));
  }
}

void DownloadBlock::got_block_partial_proof(td::BufferSlice proof) {
  CHECK(allow_partial_proof_);
  CHECK(!prev_);

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
    } else {
      td::actor::send_closure(SelfId, &DownloadBlock::checked_block_proof);
    }
  });

  td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::validate_block_proof_link, block_id_,
                          std::move(proof), std::move(P));
}

void DownloadBlock::checked_block_proof() {
  VLOG(FULL_NODE_DEBUG) << "checked proof for " << block_id_;

  if (!handle_) {
    CHECK(!short_);
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<BlockHandle> R) {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &DownloadBlock::got_block_handle_2, R.move_as_ok());
      }
    });
    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_handle, block_id_, true,
                            std::move(P));
  } else {
    got_block_handle_2(handle_);
  }
}

void DownloadBlock::got_block_handle_2(BlockHandle handle) {
  handle_ = std::move(handle);
  LOG_CHECK(skip_proof_ || handle_->inited_proof() || (allow_partial_proof_ && handle_->inited_proof_link()))
      << handle_->id() << " allowpartial=" << allow_partial_proof_;

  if (handle_->received()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::BufferSlice> R) mutable {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &DownloadBlock::got_block_data, R.move_as_ok());
      }
    });

    td::actor::send_closure(validator_manager_, &ValidatorManagerInterface::get_block_data, handle_, std::move(P));
  } else {
    CHECK(!short_);
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::BufferSlice> R) mutable {
      if (R.is_error()) {
        td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
      } else {
        td::actor::send_closure(SelfId, &DownloadBlock::got_block_data_description, R.move_as_ok());
      }
    });

    auto q = create_serialize_tl_object<ton_api::tonNode_prepareBlock>(create_tl_block_id(block_id_));
    if (client_.empty()) {
      td::actor::send_closure(overlays_, &overlay::Overlays::send_query, download_from_, local_id_, overlay_id_,
                              "get_prepare_block", std::move(P), td::Timestamp::in(1.0), std::move(q));
    } else {
      td::actor::send_closure(client_, &adnl::AdnlExtClient::send_query, "get_prepare_block",
                              create_serialize_tl_object_suffix<ton_api::tonNode_query>(std::move(q)),
                              td::Timestamp::in(1.0), std::move(P));
    }
  }
}

void DownloadBlock::got_block_data_description(td::BufferSlice data_description) {
  VLOG(FULL_NODE_DEBUG) << "downloaded data description for " << block_id_;
  auto F = fetch_tl_object<ton_api::tonNode_Prepared>(std::move(data_description), true);
  if (F.is_error()) {
    abort_query(F.move_as_error());
    return;
  }
  auto f = F.move_as_ok();

  ton_api::downcast_call(
      *f.get(),
      td::overloaded(
          [&, self = this](ton_api::tonNode_prepared &val) {
            auto P = td::PromiseCreator::lambda([SelfId = actor_id(self)](td::Result<td::BufferSlice> R) {
              if (R.is_error()) {
                td::actor::send_closure(SelfId, &DownloadBlock::abort_query, R.move_as_error());
              } else {
                td::actor::send_closure(SelfId, &DownloadBlock::got_block_data, R.move_as_ok());
              }
            });

            auto q = create_serialize_tl_object<ton_api::tonNode_downloadBlock>(create_tl_block_id(block_id_));
            if (client_.empty()) {
              td::actor::send_closure(overlays_, &overlay::Overlays::send_query_via, download_from_, local_id_,
                                      overlay_id_, "get_block", std::move(P), td::Timestamp::in(3.0), std::move(q),
                                      FullNode::max_block_size(), rldp_);
            } else {
              td::actor::send_closure(client_, &adnl::AdnlExtClient::send_query, "get_block",
                                      create_serialize_tl_object_suffix<ton_api::tonNode_query>(std::move(q)),
                                      td::Timestamp::in(3.0), std::move(P));
            }
          },
          [&](ton_api::tonNode_notFound &val) {
            abort_query(td::Status::Error(ErrorCode::notready, "dst node does not have block"));
          }));
}

//    void LiteQuery::continue_getState(BlockIdExt blkid, Ref<ton::validator::ShardState> state) {
//        LOG(INFO) << "obtained data for getState(" << blkid.to_str() << ")";
//        CHECK(state.not_null());
//        auto res = state->serialize();
//        if (res.is_error()) {
//            abort_query(res.move_as_error());
//            return;
//        }
//        auto data = res.move_as_ok();
//        FileHash file_hash;
//        td::sha256(data, file_hash.as_slice());
//        auto b = ton::create_serialize_tl_object<ton::lite_api::liteServer_blockState>(
//                ton::create_tl_lite_block_id(blkid), state->root_hash(), file_hash, std::move(data));
//        finish_query(std::move(b));
//    }


//void DownloadBlock::catchBlock(td::BufferSlice data) {


    /*
    auto res = vm::std_boc_deserialize(data);
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
        std::ostringstream outp;
//        block::gen::t_Block.print_ref(outp, root);

        blk.extra

        block::gen::t_Block.print_ref(outp, std::move(extra.out_msg_descr));
        if (blocks_stream->WriteData(outp.str())) {
            std::cout << ".";
        } else {
            std::cout << "Write blockStream fail" << std::endl;
        }
    }
*/
//    auto res = vm::std_boc_deserialize(data);
//    if (res.is_error()) {
//        std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
//        return;
//    }
//
//    auto root = res.move_as_ok();
//    block::gen::Block::Record blk;
//    block::gen::BlockExtra::Record extra;
//    block::gen::BlockInfo::Record info;
//
//    if (!(tlb::unpack_cell(root, blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
//        std::cout <<"CANNOT UNPACK HEADER FOR BLOCK " << std::endl;
////        return
//    } else {
//        std::cout << "block:" << block_.id.to_str() << std::endl;
//        std::cout << "roothash:" << root->get_hash(0).to_hex() << std::endl;
//
//        auto outmsg_cs = vm::load_cell_slice_ref(std::move(extra.out_msg_descr));
//        auto inmsg_cs = vm::load_cell_slice_ref(std::move(extra.in_msg_descr));
//
//        if (!block::tlb::t_OutMsgDescr.validate(*outmsg_cs)) {
//            std::cout << "OutMsgDescr of the new block failed to pass handwritten validity tests" << std::endl;
//        } else {
//            auto out_msg_dict_ = std::make_unique<vm::AugmentedDictionary>(std::move(outmsg_cs), 256, block::tlb::aug_OutMsgDescr);
//            if (!out_msg_dict_->validate_all()) {
//                std::cout << "OutMsgDescr dictionary is invalid" << std::endl;
//            } else {
//                std::cout << "YAAAAHHH OutMsg IS VALID!!!" << std::endl;
//            }
//
//            auto in_msg_dict_ = std::make_unique<vm::AugmentedDictionary>(std::move(inmsg_cs), 256, block::tlb::aug_InMsgDescr);
//            if (!in_msg_dict_->validate_all()) {
//                std::cout << "in_msg_dict_ dictionary is invalid" << std::endl;
//            } else {
//                std::cout << "YAAAAHHH in_msg_dict_ IS VALID!!!" << std::endl;
//            }
//
////            out_msg_dict_->check_for_each_extra()
//
//
//            in_msg_dict_->traverse_extra(out_msg_dict_->get_root_cell(), 64,
//                    [](td::ConstBitPtr key_prefix, int key_pfx_len, td::Ref<vm::CellSlice> extra,
//                    td::Ref<vm::CellSlice> value)int {
//                std::cout << "in_msg_dict_ with key " << key_prefix.to_hex(256) << std::endl;
//                return 1;
//            });
////            out_msg_dict_->validate_all(
////                    [](td::Ref<vm::CellSlice> value, td::Ref<vm::CellSlice> extra, td::ConstBitPtr key, int key_len) {
////                std::cout << "OutMsg with key " << key.to_hex(256) << std::endl;
////                return true;
////
//////                return check_out_msg(key, std::move(value)) ||
//////                       reject_query("invalid OutMsg with key "s + key.to_hex(256) + " in the new block "s + id_.to_str());
////            });
//
////            out_msg_dict_->check_for_each_extra(
////                    [this](td::Ref<vm::CellSlice> value, td::Ref<vm::CellSlice> extra, td::ConstBitPtr key, int key_len) {
////                        std::cout << "CHECK key_len:" << key_len << std::endl;
////                    }, false
////            );
////            std::function<bool(Ref<CellSlice>, Ref<CellSlice>, td::ConstBitPtr, int)>
////            std::function<bool(Ref<CellSlice>, Ref<CellSlice>, td::ConstBitPtr, int)> foreach_extra_func_t;
////
////            if (!out_msg_dict_->validate_check_extra(
////                    [this](Ref<vm::CellSlice> value, Ref<vm::CellSlice> extra, td::ConstBitPtr key, int key_len) {
////                        CHECK(key_len == 256);
////                        return check_out_msg(key, std::move(value)) ||
////                               reject_query("invalid OutMsg with key "s + key.to_hex(256) + " in the new block "s + id_.to_str());
////                    })) {
////                return reject_query("invalid OutMsgDescr dictionary in the new block "s + id_.to_str());
////            }
//        }
//
////        block::gen::OutMsgDescr::Record outMsgDescr;
////        if (!(tlb::unpack_cell(std::move(extra.out_msg_descr), outMsgDescr) )) {
////            std::cout <<"CANNOT UNPACK OutMsgDescr" << std::endl;
////        } else {
//
////            vm::load_cell_slice_ref(extra.out_msg_descr)
////            vm::AugmentedDictionary out_msg_dict{std::move(outMsgDescr.x), 256,
////                                                 block::tlb::CurrencyCollection};
////        }
////        auto outmsg_cs = vm::load_cell_slice_ref(std::move(extra.out_msg_descr));
////        auto out_msg_dict_ = std::make_unique<vm::AugmentedDictionary>(std::move(outmsg_cs), 256, block::tlb::aug_OutMsgDescr);
////        if (!out_msg_dict_->validate_all()) {
////            std::cout << "OutMsgDescr dictionary is invalid" << std::endl;
////        } else {
////            std::cout << "outmsg_cs->data(): " << out_msg_dict_->get_root_cell() << std::endl;
////            std::ostringstream outp;
////            block::gen::t_Block.print_ref(outp, out_msg_dict_->get_root_cell());
////            std::cout << outp.str();
////        }
//
//
////        block::gen::OutMsgDescr::Record outMsgDescr;
////        if (!(tlb::unpack_cell(root, outMsgDescr) )) {
////            std::cout <<"CANNOT UNPACK OutMsgDescr" << std::endl;
////        } else {
////            vm::AugmentedDictionary out_msg_dict{outMsgDescr.x, 256,
////                                                 block::tlb::CurrencyCollection};
////            outMsgDescr->x
//// HashmapAugE 256 OutMsg CurrencyCollection
//
////        std::cout << "filehash:" << info-> << std::endl;
//
////        std::cout << "block contents is " << std::endl;
//        std::ostringstream outp;
//        block::gen::t_Block.print_ref(outp, root);
//        std::cout << outp.str();
//    }
//

    /*
    auto copy = block_.data.clone();

    auto res = vm::std_boc_deserialize(copy);
    if (res.is_error()) {
        std::cout << "CANNOT DESERIALIZE BLOCK: " << res.move_as_error().error().public_message() << std::endl;
    } else {
        auto root = res.move_as_ok();
        block::gen::Block::Record blk;
        block::gen::BlockInfo::Record info;

        if (!(tlb::unpack_cell(root, blk) && tlb::unpack_cell(blk.info, info))) {
            std::cout <<"CANNOT UNPACK HEADER FOR BLOCK " << res.move_as_error().error().public_message() << std::endl;
        } else {
            std::cout << "block contents is " << std::endl;
//            std::ostringstream outp;
//            block::gen::t_Block.print_ref(outp, root);
//            vm::load_cell_slice(root).print_rec(outp);
//            std::cout << outp.str();


//            std::cout << "BLOCK GLOBAL ID: " << blk.global_id << " " << info->
        }

        try {

            std::cout << "unpack BlockExtra..." << std::endl;
            block::gen::BlockExtra::Record extra;
            if (!(tlb::unpack_cell(std::move(blk.extra), extra))) {
                std::cout << "cannot unpack extra header of key masterchain block " << block_.id.to_str() << std::endl;
            } else {
                std::cout << "unpack BlockExtra success" << std::endl;
//            vm::AugmentedDictionary out_msg_dict{vm::load_cell_slice_ref(extra.out_msg_descr), 1024,
//                                                 block::tlb::aug_OutMsgDescr};

                std::ostringstream outp;
                block::gen::t_Block.print_ref(outp, extra.account_blocks);
                std::cout << "BLOCK ACCOUNT ~BLOCKS PRINT: " << outp.str() << std::endl;

                vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256,
                                                 block::tlb::aug_ShardAccountBlocks};


                bool eof = false;
                bool allow_same = true;
                bool reverse = true;
                td::Bits256 cur_addr = td::Bits256::zero();
                while (!eof) {
                    td::Ref<vm::CellSlice> value;
                    try {
                        std::cout << "acc_dict.extract_value start" << std::endl;
                        value = acc_dict.extract_value(
                                acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, !reverse,
                                                                                 allow_same));
                    } catch (vm::VmError err) {
                        std::cout << "error while traversing account block dictionary: " << err.get_msg() << std::endl;
                        break;
                    }
                    if (value.is_null()) {
                        std::cout << "acc_dict.vm::DictionaryFixed is empty. break loop" << std::endl;
                        eof = true;
                        break;
                    }
                    std::cout << "acc_dict.extract_value SUCCESS" << std::endl;

                    block::gen::AccountBlock::Record acc_blk;
                    if (!(tlb::csr_unpack(std::move(value), acc_blk))) {
                        std::cout << "invalid AccountBlock for account " << cur_addr.to_hex() << std::endl;
                        break;
                    }

                    vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 256,
                                                       block::tlb::aug_AccountTransactions};
                    td::BitArray<64> cur_trans{(long long) 0};
                    std::cout << "TRANSACTIONS LIST:" << std::endl;
                    while (true) {
                        td::Ref<vm::Cell> tvalue;
                        try {
                            tvalue = trans_dict.extract_value_ref(
                                    trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64));
                        } catch (vm::VmError err) {
                            std::cout << "error while traversing transaction dictionary of an AccountBlock: "
                                      << err.get_msg() << std::endl;
                            break;
                        }

                        if (tvalue.is_null()) {
                            std::cout << "END TRANSACTIONS LIST" << std::endl;
                            break;
                        }

                        std::cout << "ADDR:" << cur_addr.data() << " LT:" << cur_trans.to_long() <<
                                  " HASH: " << tvalue->get_hash().to_hex() << std::endl;

//                    tvalue->get_hash().as_slice().str()
//                    result.push_back(create_tl_object<lite_api::liteServer_transactionId>(mode, cur_addr, cur_trans.to_long(),
//                                                                                          tvalue->get_hash().bits()));
                    }
                }
            }
        } catch (vm::VmError err) {
            std::cout << "error while parsing AccountBlocks of block " + block_.id.to_str() + " : " + err.get_msg() << std::endl;
        }
    }*/

//}

void DownloadBlock::got_block_data(td::BufferSlice data) {
  VLOG(FULL_NODE_DEBUG) << "downloaded data for " << block_id_;
  block_.data = std::move(data);

    std::cout << "DownloadBlock got_block_data (" << block_.data.length() << ")" << block_.id.to_str() << std::endl;

    if (ton::ext::BlocksStream::GetInstance().WriteData(block_.data.as_slice().str())) {
        std::cout << ".";
    } else {
        std::cout << "Write blockStream fail" << std::endl;
    }
//    catchBlock(block_.data.clone());

  finish_query();
}

}  // namespace fullnode

}  // namespace validator

}  // namespace ton
