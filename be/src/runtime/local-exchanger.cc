// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/local-exchanger.h"
#include "runtime/row-batch.h"

namespace impala {

Status LocalExchanger::Push(std::unique_ptr<RowBatch> batch) {
  Cell* cell = new Cell();
  cell->batch = std::move(batch);
  cell->consumers_left = consumer_count_;

  std::lock_guard l(mutex_);
  tail_->next = cell;
  tail_ = cell;
  return Status::OK();
}

RowBatch* LocalExchanger::Pull(int32_t consumer_index, bool* eos) {
  DCHECK(consumer_index >= 0 && consumer_index < consumer_count_);
  std::lock_guard l(mutex_);
  // This skips the first dummy cell.
  Cell* cell = progress_[consumer_index]->next;
  if (cell == nullptr) {
    // No more batches currently available.
    *eos = eos_;
    return nullptr;
  }
  cell->consumers_left--;
  progress_[consumer_index] = cell;
  release_cells();
  *eos = false;
  return cell->batch.get();
}

void LocalExchanger::Close() {
  std::lock_guard l(mutex_);
  eos_ = true;
}

void LocalExchanger::Close(int32_t consumer_index) {
  DCHECK(consumer_index >= 0 && consumer_index < consumer_count_);
  std::lock_guard l(mutex_);
  while (progress_[consumer_index] != nullptr) {
    Cell* cell = progress_[consumer_index]->next;
    if (cell != nullptr) cell->consumers_left--;
    progress_[consumer_index] = cell;
  }
  release_cells();
  ++consumers_done_;
}

void LocalExchanger::release_cells() {
  // Advance head_ while all consumers have moved past the cell.
  while (head_ != nullptr && head_->consumers_left == 0) {
    Cell* next_cell = head_->next;
    // Halt if head_ is still referenced by progress_ and not safe to delete.
    if (next_cell == nullptr || next_cell->consumers_left > 0) break;
    // All consumers have moved past head_, can release it.
    delete head_;
    head_ = next_cell;
  }
}

} // namespace impala
