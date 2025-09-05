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

#include "exec/cte-producer-node.h"
#include "exec/exec-node-util.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/local-exchanger.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"

#include "common/names.h"

namespace impala {

Status CTEProducerPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  DCHECK(tnode.__isset.cte_producer);
  // Register the exchanger here while plan setup is single-threaded.
  // TODO: handle MT_DOP > 1, which would currently result in name collisions.
  exchanger_.reset(new LocalExchanger(tnode_->cte_producer.num_consumers));
  VLOG_QUERY << "Registering CTE exchange " << tnode_->cte_producer.name << " with "
             << tnode_->cte_producer.num_consumers << " consumers.";
  state->query_state()->RegisterExchanger(tnode_->cte_producer.name, exchanger_.get());
  return Status::OK();
}

Status CTEProducerPlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new CTEProducerNode(pool, *this, state, exchanger_.get()));
  return Status::OK();
}

CTEProducerNode::CTEProducerNode(ObjectPool* pool, const CTEProducerPlanNode& pnode,
    RuntimeState* state, LocalExchanger* exchanger)
  : ExecNode(pool, pnode, state->desc_tbl()), exchanger_(exchanger) {
}

Status CTEProducerNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(child(0)->Open(state));
  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement.
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }

  DCHECK(row_desc()->Equals(*child(0)->row_desc()));
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    // TODO: re-use row batch if not full.
    unique_ptr<RowBatch> child_batch(new RowBatch(
        child(0)->row_desc(), state->batch_size(), mem_tracker()));
    RETURN_IF_ERROR(children_[0]->GetNext(state, child_batch.get(), &eos));
    VLOG_PROGRESS << "Adding " << child_batch->num_rows()
                  << " rows to CTE exchange " << GetCTEName() << " in " << label();
    // Add all row batches, even if empty, to avoid freeing the tuple data pool.
    RETURN_IF_ERROR(exchanger_->Push(std::move(child_batch)));
  } while (!eos);

  exchanger_->Close();
  return Status::OK();
}

Status CTEProducerNode::GetNext(
    RuntimeState* state, RowBatch* output_row_batch, bool* eos) {
  RETURN_IF_CANCELLED(state);
  // Avoid busy waiting while readers finish.
  SleepForMs(10);
  *eos = exchanger_->ReadFinished();
  return Status::OK();
}

Status CTEProducerNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  // Reset() is not supported.
  const char* msg = "Internal error: CTE producer nodes should not appear in subplans.";
  DCHECK(false) << msg;
  return Status(msg);
}

void CTEProducerNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  VLOG_QUERY << "Releasing CTE exchange " << GetCTEName();
  exchanger_->Release();
  ExecNode::Close(state);
}

void CTEProducerNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ') << "CTEProducerNode(" << GetCTEName();
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
