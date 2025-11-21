//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::scoped_lock latch(*bpm_latch_);

  // Allocate a new page and bring it into the buffer pool. If no frame is
  // available (no free frames and replacer can't evict), return INVALID_PAGE_ID.
  
  frame_id_t fid = INVALID_FRAME_ID;

  // Use a free frame if available
  if (!free_frames_.empty()) {
    fid = free_frames_.front();
    free_frames_.pop_front();
  } else {
    // Try to evict a victim
    auto victim = replacer_->Evict();
    if (!victim.has_value()) {
      // no frame available - return early without allocating a page_id
      return INVALID_PAGE_ID;
    }
    fid = victim.value();

    // find the page id currently mapped to this frame
    page_id_t old_pid = INVALID_PAGE_ID;
    for (auto it = page_table_.begin(); it != page_table_.end(); ++it) {
      if (it->second == fid) {
        old_pid = it->first;
        page_table_.erase(it);
        break;
      }
    }

    // If frame contained a page, write it back if dirty
    auto &frame = frames_[fid];
    if (old_pid != INVALID_PAGE_ID && frame->is_dirty_) {
      // Schedule a write to disk and wait for completion before reusing frame.
      std::vector<DiskRequest> reqs;
      DiskRequest req;
      req.is_write_ = true;
      req.data_ = frame->GetDataMut();
      req.page_id_ = old_pid;
      // Create a promise so we can wait until the write completes.
      auto promise = disk_scheduler_->CreatePromise();
      req.callback_ = std::move(promise);
      auto future = req.callback_.get_future();
      reqs.push_back(std::move(req));
      disk_scheduler_->Schedule(reqs);
      future.get();
    }

    // reset frame metadata
    frame->Reset();
  }

  // Now that we have a frame, allocate a fresh page id
  page_id_t new_pid = static_cast<page_id_t>(next_page_id_.fetch_add(1));

  // Install new page into frame
  page_table_.emplace(new_pid, fid);
  auto &frame = frames_[fid];
  frame->Reset();
  frame->pin_count_.store(0);
  frame->is_dirty_ = false;  // newly allocated page not dirty yet

  // Tell replacer about the access (it will insert the alive entry). Page is not
  // pinned after NewPage, so it is evictable.
  replacer_->RecordAccess(fid, new_pid);
  replacer_->SetEvictable(fid, true);

  return new_pid;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock latch(*bpm_latch_);

  // If page is in page_table_, ensure it's not pinned
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;
    auto &frame = frames_[fid];
    if (frame->pin_count_.load() != 0) {
      // cannot delete pinned page
      return false;
    }

    // Remove mapping and reset frame
    page_table_.erase(it);

    // If dirty, best effort: schedule a write (optional); then deallocate on disk
    if (frame->is_dirty_) {
      std::vector<DiskRequest> reqs;
      DiskRequest req;
      req.is_write_ = true;
      req.data_ = frame->GetDataMut();
      req.page_id_ = page_id;
      auto promise = disk_scheduler_->CreatePromise();
      req.callback_ = std::move(promise);
      auto future = req.callback_.get_future();
      reqs.push_back(std::move(req));
      disk_scheduler_->Schedule(reqs);
      // Wait for write to finish before deallocating and resetting frame
      future.get();
    }

    // Reset frame and add back to free list
    frame->Reset();
    free_frames_.push_back(fid);

    // Ensure replacer knows this frame is removed
    replacer_->Remove(fid);

    // Finally deallocate page on disk
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  // Page not present in memory: still deallocate on disk and return true
  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::shared_ptr<FrameHeader> frame = nullptr;
  frame_id_t fid = INVALID_FRAME_ID;

  // First, check if the page is already resident.
  {
    std::scoped_lock latch(*bpm_latch_);
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
      fid = it->second;
      replacer_->RecordAccess(fid, page_id);
      frame = frames_[fid];
    }
  }

  if (frame != nullptr) {
    // Construct and return WritePageGuard (do not hold bpm_latch_ while constructing)
    return std::optional<WritePageGuard>(WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
  }

  // Page not resident: allocate a frame (may evict)
  {
    std::scoped_lock latch(*bpm_latch_);
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
    } else {
      auto victim = replacer_->Evict();
      if (!victim.has_value()) {
        return std::nullopt;
      }
      fid = victim.value();

      // remove old mapping for this frame
      page_id_t old_pid = INVALID_PAGE_ID;
      for (auto it2 = page_table_.begin(); it2 != page_table_.end(); ++it2) {
        if (it2->second == fid) {
          old_pid = it2->first;
          page_table_.erase(it2);
          break;
        }
      }

      // If frame contained a page and is dirty, write it back
      auto &fref = frames_[fid];
        if (old_pid != INVALID_PAGE_ID && fref->is_dirty_) {
          std::vector<DiskRequest> reqs;
          DiskRequest req;
          req.is_write_ = true;
          req.data_ = fref->GetDataMut();
          req.page_id_ = old_pid;
          auto promise = disk_scheduler_->CreatePromise();
          req.callback_ = std::move(promise);
          auto future = req.callback_.get_future();
          reqs.push_back(std::move(req));
          disk_scheduler_->Schedule(reqs);
          future.get();
        }

      // Reset frame metadata to blank
      frames_[fid]->Reset();
    }

    // Install new mapping
    page_table_.emplace(page_id, fid);
    replacer_->RecordAccess(fid, page_id);
    frame = frames_[fid];
  }

  // Read page from disk into frame
  {
    std::vector<DiskRequest> reqs;
    DiskRequest req;
    req.is_write_ = false;
    req.data_ = frame->GetDataMut();
    req.page_id_ = page_id;
    reqs.push_back(std::move(req));
    auto promise = disk_scheduler_->CreatePromise();
    reqs[0].callback_ = std::move(promise);
    auto future = reqs[0].callback_.get_future();
    disk_scheduler_->Schedule(reqs);
    future.get();
  }

  return std::optional<WritePageGuard>(WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::shared_ptr<FrameHeader> frame = nullptr;
  frame_id_t fid = INVALID_FRAME_ID;

  // First, check if the page is already resident.
  {
    std::scoped_lock latch(*bpm_latch_);
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
      fid = it->second;
      // Inform replacer about the access while holding bpm_latch_
      replacer_->RecordAccess(fid, page_id);
      frame = frames_[fid];
    }
  }

  if (frame != nullptr) {
    // Construct and return guard (bpm_latch_ not held to avoid deadlock).
    return std::optional<ReadPageGuard>(ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
  }

  // Page not resident: try to allocate a frame (may evict)
  {
    std::scoped_lock latch(*bpm_latch_);
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
    } else {
      auto victim = replacer_->Evict();
      if (!victim.has_value()) {
        return std::nullopt;
      }
      fid = victim.value();

      // remove old mapping for this frame
      page_id_t old_pid = INVALID_PAGE_ID;
      for (auto it2 = page_table_.begin(); it2 != page_table_.end(); ++it2) {
        if (it2->second == fid) {
          old_pid = it2->first;
          page_table_.erase(it2);
          break;
        }
      }

      // If frame contained a page and is dirty, write it back
      auto &fref = frames_[fid];
      if (old_pid != INVALID_PAGE_ID && fref->is_dirty_) {
        std::vector<DiskRequest> reqs;
        DiskRequest req;
        req.is_write_ = true;
        req.data_ = fref->GetDataMut();
        req.page_id_ = old_pid;
        auto promise = disk_scheduler_->CreatePromise();
        req.callback_ = std::move(promise);
        auto future = req.callback_.get_future();
        reqs.push_back(std::move(req));
        disk_scheduler_->Schedule(reqs);
        future.get();
      }

      // Reset frame metadata to blank
      frames_[fid]->Reset();
    }

    // Install new mapping for the requested page
    page_table_.emplace(page_id, fid);

    // Tell replacer about the access (it will insert alive entry). We do this while holding bpm_latch_.
    replacer_->RecordAccess(fid, page_id);

    frame = frames_[fid];
  }

  // Read page from disk into frame (schedule request)
  {
    std::vector<DiskRequest> reqs;
    DiskRequest req;
    req.is_write_ = false;
    req.data_ = frame->GetDataMut();
    req.page_id_ = page_id;
    reqs.push_back(std::move(req));
    auto promise = disk_scheduler_->CreatePromise();
    reqs[0].callback_ = std::move(promise);
    auto future = reqs[0].callback_.get_future();
    disk_scheduler_->Schedule(reqs);
    future.get();
  }

  // Construct and return a ReadPageGuard which will pin and latch the frame
  return std::optional<ReadPageGuard>(ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_));
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // Do not take the page latch; just look up the frame and schedule write if dirty.
  std::scoped_lock latch(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t fid = it->second;
  auto &frame = frames_[fid];
  if (!frame->is_dirty_) {
    return true;
  }

  std::vector<DiskRequest> reqs;
  DiskRequest req;
  req.is_write_ = true;
  req.data_ = frame->GetDataMut();
  req.page_id_ = page_id;
  reqs.push_back(std::move(req));
  // Best-effort: schedule write and clear dirty flag
  disk_scheduler_->Schedule(reqs);
  frame->is_dirty_ = false;
  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // Safe flush: take the page latch to ensure consistent state while writing
  std::shared_ptr<FrameHeader> frame = nullptr;
  {
    std::scoped_lock latch(*bpm_latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }
    frame = frames_[it->second];
  }

  // Lock the frame exclusively to get a consistent snapshot
  frame->rwlatch_.lock();
  if (frame->is_dirty_) {
    std::vector<DiskRequest> reqs;
    DiskRequest req;
    req.is_write_ = true;
    req.data_ = frame->GetDataMut();
    req.page_id_ = page_id;
    reqs.push_back(std::move(req));
    disk_scheduler_->Schedule(reqs);
    frame->is_dirty_ = false;
  }
  frame->rwlatch_.unlock();
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  std::vector<DiskRequest> reqs;
  {
    std::scoped_lock latch(*bpm_latch_);
    for (const auto &entry : page_table_) {
      page_id_t pid = entry.first;
      frame_id_t fid = entry.second;
      auto &frame = frames_[fid];
      if (frame->is_dirty_) {
        DiskRequest req;
        req.is_write_ = true;
        req.data_ = frame->GetDataMut();
        req.page_id_ = pid;
        reqs.push_back(std::move(req));
        frame->is_dirty_ = false;
      }
    }
  }
  if (!reqs.empty()) {
    disk_scheduler_->Schedule(reqs);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // Collect a snapshot of pages to flush to avoid holding bpm_latch_ while locking frames
  std::vector<std::pair<page_id_t, std::shared_ptr<FrameHeader>>> pages;
  {
    std::scoped_lock latch(*bpm_latch_);
    pages.reserve(page_table_.size());
    for (const auto &entry : page_table_) {
      pages.emplace_back(entry.first, frames_[entry.second]);
    }
  }

  // Flush each page while holding its exclusive latch
  for (auto &p : pages) {
    page_id_t pid = p.first;
    auto &frame = p.second;
    frame->rwlatch_.lock();
    if (frame->is_dirty_) {
      std::vector<DiskRequest> reqs;
      DiskRequest req;
      req.is_write_ = true;
      req.data_ = frame->GetDataMut();
      req.page_id_ = pid;
      reqs.push_back(std::move(req));
      disk_scheduler_->Schedule(reqs);
      frame->is_dirty_ = false;
    }
    frame->rwlatch_.unlock();
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock latch(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  frame_id_t fid = it->second;
  auto &frame = frames_[fid];
  // pin_count_ is atomic; safe to load without extra per-frame latch
  return static_cast<size_t>(frame->pin_count_.load());
}

}  // namespace bustub
