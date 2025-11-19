//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  explicit ClockReplacer(size_t num_pages);

  ~ClockReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  size_t num_pages_;
  size_t clock_hand_;
  std::vector<bool> reference_bits_;
  // `pinned_` here records whether a frame is currently tracked by the replacer
  // (i.e. is a candidate for replacement). Despite the name, when
  // `pinned_[i] == true` the frame is in the replacer and thus eligible for
  // victimization; when `pinned_[i] == false` the frame is not tracked.
  std::vector<bool> pinned_;
  // 当前 replacer 中可被替换的元素数量
  size_t current_size_;
  std::mutex latch_;
};

}  // namespace bustub
