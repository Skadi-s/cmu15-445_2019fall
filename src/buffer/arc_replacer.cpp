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
	std::scoped_lock guard(latch_);

	// If no evictable entries, cannot evict
	if (curr_size_ == 0) {
		return std::nullopt;
	}

	// Decide which side to evict from: prefer MRU if its size > target, otherwise MFU
	auto evict_from_mru = (mru_.size() > mru_target_size_);

	// Try to evict from desired side, if none evictable there, try the other.
	for (int attempt = 0; attempt < 2; attempt++) {
		if (evict_from_mru) {
			// Evict from back of mru_ (least recently used in MRU)
			for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
				frame_id_t fid = *it;
				auto it_alive = alive_map_.find(fid);
				if (it_alive == alive_map_.end()) {
					continue;
				}
				if (!it_alive->second->evictable_) {
					continue;
				}
				// Found victim
				page_id_t pid = it_alive->second->page_id_;
				// remove from mru_
				auto rit = std::find(mru_.begin(), mru_.end(), fid);
				if (rit != mru_.end()) {
					mru_.erase(rit);
				}
				// move to mru_ghost_
				mru_ghost_.push_front(pid);
				ghost_map_.erase(pid);
				ghost_map_.emplace(pid, std::make_shared<FrameStatus>(pid, -1, false, ArcStatus::MRU_GHOST));

				// remove from alive_map_
				alive_map_.erase(it_alive);
				curr_size_--;
				// shrink ghost if needed
				if (mru_ghost_.size() + mfu_ghost_.size() > replacer_size_ * 2) {
					// evict oldest from mfu_ghost_
					if (!mfu_ghost_.empty()) {
						page_id_t old = mfu_ghost_.back();
						mfu_ghost_.pop_back();
						ghost_map_.erase(old);
					}
				}
				return fid;
			}
		} else {
			// Evict from back of mfu_
			for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
				frame_id_t fid = *it;
				auto it_alive = alive_map_.find(fid);
				if (it_alive == alive_map_.end()) {
					continue;
				}
				if (!it_alive->second->evictable_) {
					continue;
				}
				page_id_t pid = it_alive->second->page_id_;
				auto rit = std::find(mfu_.begin(), mfu_.end(), fid);
				if (rit != mfu_.end()) {
					mfu_.erase(rit);
				}
				mfu_ghost_.push_front(pid);
				ghost_map_.erase(pid);
				ghost_map_.emplace(pid, std::make_shared<FrameStatus>(pid, -1, false, ArcStatus::MFU_GHOST));

				alive_map_.erase(it_alive);
				curr_size_--;
				if (mru_ghost_.size() + mfu_ghost_.size() > replacer_size_ * 2) {
					if (!mru_ghost_.empty()) {
						page_id_t old = mru_ghost_.back();
						mru_ghost_.pop_back();
						ghost_map_.erase(old);
					}
				}
				return fid;
			}
		}

		// nothing evictable on desired side, flip and retry
		evict_from_mru = !evict_from_mru;
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
	std::scoped_lock guard(latch_);

	// Case 1: frame already alive
	auto it_alive = alive_map_.find(frame_id);
	if (it_alive != alive_map_.end()) {
		auto st = it_alive->second;
		if (st->arc_status_ == ArcStatus::MRU) {
			// promote to MFU
			// remove from mru_
			auto it = std::find(mru_.begin(), mru_.end(), frame_id);
			if (it != mru_.end()) {
				mru_.erase(it);
			}
			// place at front of mfu_
			st->arc_status_ = ArcStatus::MFU;
			mfu_.push_front(frame_id);
		} else if (st->arc_status_ == ArcStatus::MFU) {
			// refresh position in mfu_
			auto it = std::find(mfu_.begin(), mfu_.end(), frame_id);
			if (it != mfu_.end()) {
				mfu_.erase(it);
			}
			mfu_.push_front(frame_id);
		}
		return;
	}

	// Case 2/3: hit ghost
	auto it_ghost = ghost_map_.find(page_id);
	if (it_ghost != ghost_map_.end()) {
		// determine which ghost
		auto existing = it_ghost->second;
		if (existing->arc_status_ == ArcStatus::MRU_GHOST) {
			// bump target size up
			mru_target_size_ = std::min(replacer_size_, mru_target_size_ + 1);
		} else if (existing->arc_status_ == ArcStatus::MFU_GHOST) {
			if (mru_target_size_ > 0) {
				mru_target_size_ = mru_target_size_ - 1;
			}
		}
		// remove from ghost lists
		auto itg = std::find(mru_ghost_.begin(), mru_ghost_.end(), page_id);
		if (itg != mru_ghost_.end()) {
			mru_ghost_.erase(itg);
		}
		itg = std::find(mfu_ghost_.begin(), mfu_ghost_.end(), page_id);
		if (itg != mfu_ghost_.end()) {
			mfu_ghost_.erase(itg);
		}
		ghost_map_.erase(it_ghost);

		// place frame into MFU
		auto st = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
		alive_map_.emplace(frame_id, st);
		mfu_.push_front(frame_id);
		return;
	}

	// Case 4: cold miss, insert into MRU
	auto st = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
	alive_map_.emplace(frame_id, st);
	mru_.push_front(frame_id);
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
	std::scoped_lock guard(latch_);
	auto it = alive_map_.find(frame_id);
	if (it == alive_map_.end()) {
		return;
	}
	auto st = it->second;
	if (st->evictable_ == set_evictable) {
		return;
	}
	if (set_evictable) {
		st->evictable_ = true;
		curr_size_++;
	} else {
		if (!st->evictable_) {
			// already non-evictable
			return;
		}
		st->evictable_ = false;
		if (curr_size_ > 0) {
			curr_size_--;
		}
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
	std::scoped_lock guard(latch_);
	auto it = alive_map_.find(frame_id);
	if (it == alive_map_.end()) {
		return;
	}
	auto st = it->second;
	if (!st->evictable_) {
		// removal called on non-evictable frame => abort
		BUSTUB_ENSURE(false, "Remove called on non-evictable frame");
	}
	// remove from corresponding list
	if (st->arc_status_ == ArcStatus::MRU) {
		auto itl = std::find(mru_.begin(), mru_.end(), frame_id);
		if (itl != mru_.end()) {
			mru_.erase(itl);
		}
	} else if (st->arc_status_ == ArcStatus::MFU) {
		auto itl = std::find(mfu_.begin(), mfu_.end(), frame_id);
		if (itl != mfu_.end()) {
			mfu_.erase(itl);
		}
	}
	alive_map_.erase(it);
	if (curr_size_ > 0) {
		curr_size_--;
	}
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t { std::scoped_lock guard(latch_); return curr_size_; }

}  // namespace bustub
