// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <mutex>
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> { 
    std::lock_guard<std::mutex> guard(latch_);
    // if mru_ size < target size, evict from mfu_
    if (mru_.size() < mru_target_size_) {
        auto evicted_frame = EvictInternal(mfu_, ArcStatus::MFU_GHOST, mfu_ghost_);
        if (evicted_frame.has_value()) {
            return evicted_frame;
        } else {
            return EvictInternal(mru_, ArcStatus::MRU_GHOST, mru_ghost_);
        }
    } else {
        auto evicted_frame = EvictInternal(mru_, ArcStatus::MRU_GHOST, mru_ghost_);
        if (evicted_frame.has_value()) {
            return evicted_frame;
        } else {
            return EvictInternal(mfu_, ArcStatus::MFU_GHOST, mfu_ghost_);
        }
    }
    return std::nullopt;
 }

auto ArcReplacer::EvictInternal(std::list<frame_id_t> &from_list, ArcStatus ghost_status,
                       std::list<page_id_t> &to_ghost_list) -> std::optional<frame_id_t> {
    for (auto riter = from_list.rbegin(); riter != from_list.rend(); ++riter) {
        auto frame_id = *riter;
        auto alive_iter = alive_map_.find(frame_id);
        if (alive_iter != alive_map_.end()) {
            auto frame_status = alive_iter->second;
            if (frame_status->evictable_) {
                // erase the element using the stored iterator (O(1))
                auto base_it = std::prev(riter.base());
                from_list.erase(base_it);
                // move into ghost list
                to_ghost_list.push_front(frame_status->page_id_);
                auto ghost_it = to_ghost_list.begin();
                ghost_map_[frame_status->page_id_] = std::make_shared<FrameStatus>(
                    frame_status->page_id_, frame_id, false, ghost_status);
                ghost_map_[frame_status->page_id_]->ghost_iter_ = ghost_it;
                // remove from alive map and update size
                alive_map_.erase(alive_iter);
                curr_size_--;
                return frame_id;
            }
        }
    }
    return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> guard(latch_);
    // case 1: hit in mru_ or mfu_
    auto alive_iter = alive_map_.find(frame_id);
    if (alive_iter != alive_map_.end()) {
        auto frame_status = alive_iter->second;
        if (frame_status->arc_status_ == ArcStatus::MRU) {
            // move from mru_ to mfu_
            mru_.erase(frame_status->list_iter_);
            mfu_.push_front(frame_id);
            frame_status->list_iter_ = mfu_.begin();
            frame_status->arc_status_ = ArcStatus::MFU;
        } else if (frame_status->arc_status_ == ArcStatus::MFU) {
            // move to front of mfu_ using splice (O(1))
            mfu_.splice(mfu_.begin(), mfu_, frame_status->list_iter_);
            frame_status->list_iter_ = mfu_.begin();
        }
    }
    // case 2/3 : hit in mru_ghost_ / mfu_ghost_
    else {
        auto ghost_iter = ghost_map_.find(page_id);
        if (ghost_iter != ghost_map_.end()) {
            // hit in ghost lists
            auto frame_status = ghost_iter->second;
            if (frame_status->arc_status_ == ArcStatus::MRU_GHOST) {
                // increase target size
                auto delta = (mru_ghost_.size() >= mfu_ghost_.size()) 
                                    ? 1 : mfu_ghost_.size() / mru_ghost_.size();
                mru_target_size_ += delta;
                // move from mru_ghost_ to mfu_
                mru_ghost_.erase(frame_status->ghost_iter_);
                ghost_map_.erase(ghost_iter);
                mfu_.push_front(frame_id);
                auto new_status = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
                new_status->list_iter_ = mfu_.begin();
                alive_map_[frame_id] = new_status;
                curr_size_++;
            } 
            // hit in mfu_ghost_
            else if (frame_status->arc_status_ == ArcStatus::MFU_GHOST) {
                // decrease target size
                auto delta = mfu_ghost_.size() >= mru_ghost_.size() 
                                    ? 1 : mru_ghost_.size() / mfu_ghost_.size();
                mru_target_size_ -= delta;
                // move from mfu_ghost_ to mfu_
                mfu_ghost_.erase(frame_status->ghost_iter_);
                ghost_map_.erase(ghost_iter);
                mfu_.push_front(frame_id);
                auto new_status2 = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
                new_status2->list_iter_ = mfu_.begin();
                alive_map_[frame_id] = new_status2;
                curr_size_++;
            }
        }
        // case 4: miss all lists
        else {
            // case 4a: L1 overflow
            if (mru_.size() + mru_ghost_.size() == replacer_size_) {
                    // kill last item in MRU Ghost
                    auto last_page_id = mru_ghost_.back();
                    mru_ghost_.pop_back();
                    ghost_map_.erase(last_page_id);
                    // move new page to the front of MRU
                    mru_.push_front(frame_id);
                    auto new_status3 = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MRU);
                    new_status3->list_iter_ = mru_.begin();
                    alive_map_[frame_id] = new_status3;
                    curr_size_++;
            }
            // case 4b: L2 overflow
            else if (mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
                // kill last item in MFU Ghost
                auto last_page_id = mfu_ghost_.back();
                mfu_ghost_.pop_back();
                ghost_map_.erase(last_page_id);
                // move new page to the front of MRU
                mru_.push_front(frame_id);
                auto new_status4 = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MRU);
                new_status4->list_iter_ = mru_.begin();
                alive_map_[frame_id] = new_status4;
                curr_size_++;
            } else {
                // move new page to the front of MRU
                mru_.push_front(frame_id);
                auto new_status5 = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MRU);
                new_status5->list_iter_ = mru_.begin();
                alive_map_[frame_id] = new_status5;
                curr_size_++;
            }
        }
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> guard(latch_);
    auto alive_iter = alive_map_.find(frame_id);
    if (alive_iter == alive_map_.end()) {
        throw std::runtime_error("Frame id is invalid");
    }
    auto frame_status = alive_iter->second;
    // no-op if status is already the same
    if (frame_status->evictable_ == set_evictable) {
        return;
    }
    frame_status->evictable_ = set_evictable;
    if (set_evictable) {
        curr_size_++;
    } else {
        curr_size_--;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    auto alive_iter = alive_map_.find(frame_id);
    if (alive_iter == alive_map_.end()) {
        return;
    }
    auto frame_status = alive_iter->second;
    if (!frame_status->evictable_) {
        throw std::runtime_error("Cannot remove a non-evictable frame");
    }
    // remove from corresponding list using stored iterator (O(1))
    if (frame_status->arc_status_ == ArcStatus::MRU) {
        mru_.erase(frame_status->list_iter_);
    } else if (frame_status->arc_status_ == ArcStatus::MFU) {
        mfu_.erase(frame_status->list_iter_);
    }
    alive_map_.erase(alive_iter);
    curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
    std::lock_guard<std::mutex> guard(latch_);
    return curr_size_;
}

}  // namespace bustub
