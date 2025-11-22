//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <queue>
#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("Width and depth must be non-zero");
  }

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  // Allocate sketch matrix
  sketch_matrix_.resize(depth_);
  for (size_t i = 0; i < depth_; i++) {
    sketch_matrix_[i].resize(width_, 0);
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  // move constructor
  width_ = other.width_;
  depth_ = other.depth_;
  hash_functions_ = std::move(other.hash_functions_);
  sketch_matrix_ = std::move(other.sketch_matrix_);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  // move assignment
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    hash_functions_ = std::move(other.hash_functions_);
    sketch_matrix_ = std::move(other.sketch_matrix_);
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  std::lock_guard<std::mutex> lock(global_mutex_);
  for (size_t i = 0; i < depth_; i++) {
    size_t col = hash_functions_[i](item);
    sketch_matrix_[i][col]++;
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  std::lock_guard<std::mutex> lock(global_mutex_);
  std::lock_guard<std::mutex> other_lock(other.global_mutex_);
  for (size_t row = 0; row < depth_; row++) {
    for (size_t col = 0; col < width_; col++) {
      sketch_matrix_[row][col] += other.sketch_matrix_[row][col];
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  std::lock_guard<std::mutex> lock(global_mutex_);
  uint32_t min_count = UINT32_MAX;
  for (size_t i = 0; i < depth_; i++) {
    size_t col = hash_functions_[i](item);
    uint32_t count = sketch_matrix_[i][col];
    if (count < min_count) {
      min_count = count;
    }
  }
  return min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  std::lock_guard<std::mutex> lock(global_mutex_);
  for (size_t row = 0; row < depth_; row++) {
    for (size_t col = 0; col < width_; col++) {
      sketch_matrix_[row][col] = 0;
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  // Use a min-heap to keep the largest k elements
  // The heap stores (count, candidate), and we use greater to make it a min-heap
  std::priority_queue<std::pair<uint32_t, KeyType>, 
                      std::vector<std::pair<uint32_t, KeyType>>,
                      std::greater<>> min_heap;
  
  for (const auto &candidate : candidates) {
    uint32_t count = Count(candidate);
    min_heap.push({count, candidate});
    if (min_heap.size() > k) {
      min_heap.pop();  // Remove the smallest element
    }
  }

  // Extract elements and reverse to get descending order
  std::vector<std::pair<KeyType, uint32_t>> result;
  result.reserve(min_heap.size());
  while (!min_heap.empty()) {
    auto [count, candidate] = min_heap.top();
    min_heap.pop();
    result.emplace_back(candidate, count);
  }
  std::reverse(result.begin(), result.end());
  return result;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
