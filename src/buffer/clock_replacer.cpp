//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

/**
 * Create a new ClockReplacer.
 * @param num_pages the maximum number of pages the ClockReplacer will be required to store
 */
ClockReplacer::ClockReplacer(size_t num_pages) {
  LOG_INFO("ClockReplacer created with num_pages: %zu", num_pages);
  num_pages_ = num_pages;
  clock_hand_ = 0;
  reference_bits_ = std::vector<bool>(num_pages, false);
  pinned_ = std::vector<bool>(num_pages, false);
  current_size_ = 0;
}

/**
 * Destroys the ClockReplacer.
 */
ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (current_size_ == 0) {
    return false;
  }

  // Keep scanning until we find a victim. Since `current_size_ > 0`, there is
  // at least one candidate, so this loop will eventually return.
  while (true) {
    // If this slot is not a candidate, skip it.
    if (!pinned_[clock_hand_]) {
      clock_hand_ = (clock_hand_ + 1) % num_pages_;
      continue;
    }

    // Give a second chance if referenced recently.
    if (reference_bits_[clock_hand_]) {
      reference_bits_[clock_hand_] = false;
      clock_hand_ = (clock_hand_ + 1) % num_pages_;
      continue;
    }

    // Found victim
    *frame_id = static_cast<frame_id_t>(clock_hand_);
    pinned_[clock_hand_] = false;
    reference_bits_[clock_hand_] = false;
    current_size_--;
    clock_hand_ = (clock_hand_ + 1) % num_pages_;
    LOG_INFO("Victim selected: frame_id %d", *frame_id);
    return true;
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= num_pages_) {
    return;
  }
  if (pinned_[frame_id]) {
    pinned_[frame_id] = false;
    reference_bits_[frame_id] = false;
    if (current_size_ > 0) {
      current_size_--;
    }
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  LOG_INFO("Unpin called with frame_id: %d", frame_id);
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= num_pages_) {
    return;
  }
  if (!pinned_[frame_id]) {
    pinned_[frame_id] = true;
    reference_bits_[frame_id] = true;
    current_size_++;
  }
}

auto ClockReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return current_size_;
}

}  // namespace bustub
