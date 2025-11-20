//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread which will process requests from the channel.
  // The worker will run until the destructor enqueues a `std::nullopt` sentinel.
  background_thread_.emplace([this] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  // Non-blocking enqueue of requests.
  // Each DiskRequest may optionally contain a promise in `callback_` that the
  // caller can wait on to observe completion. We move each request into the
  // shared channel so the background worker can pick it up and execute it.
  for (auto &req : requests) {
    request_queue_.Put(std::make_optional<DiskRequest>(std::move(req)));
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  // Background worker loop: consume DiskRequest objects from the request_queue_
  // Channel semantics: Get() blocks until an element is available. When the
  // destructor wants the worker to exit it will push a `std::nullopt` sentinel.
  while (true) {
    auto opt = request_queue_.Get();
    if (!opt.has_value()) {
      // Shutdown signal received; exit the worker loop.
      break;
    }

    // Take ownership of the request and process it.
    DiskRequest req = std::move(opt.value());

    // Execute the I/O using the DiskManager. The DiskManager implementations
    // are expected to be synchronous (blocking) calls that complete the I/O
    // before returning. This keeps the DiskScheduler simple: scheduling is
    // asynchronous, but completion can be observed via the optional promise.
    if (req.is_write_) {
      disk_manager_->WritePage(req.page_id_, req.data_);
    } else {
      disk_manager_->ReadPage(req.page_id_, req.data_);
    }

    // Fulfill the promise if the caller provided one. Use try/catch because
    // the future side may have been discarded by the caller.
    try {
      req.callback_.set_value(true);
    } catch (...) {
      // Ignore errors setting the promise (e.g., no consumer).
    }
  }
}

}  // namespace bustub
