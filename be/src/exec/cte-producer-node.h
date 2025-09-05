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

#pragma once

#include "exec/exec-node.h"
#include "runtime/local-exchanger.h"

namespace impala {

class LocalExchanger;

class CTEProducerPlanNode : public PlanNode {
 public:
  Status Init(const TPlanNode& tnode, FragmentState* state) override;
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~CTEProducerPlanNode() override = default;

 private:
  std::unique_ptr<LocalExchanger> exchanger_;
};

/// Node that buffers results produced by a Common Table Expression into a
/// LocalExchanger.

class CTEProducerNode : public ExecNode {
 public:
  CTEProducerNode(ObjectPool* pool, const CTEProducerPlanNode& pnode, RuntimeState* state,
      LocalExchanger* exchanger);

  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  void Close(RuntimeState* state) override;
  void DebugString(int indentation_level, std::stringstream* out) const override;

 private:
  const std::string& GetCTEName() const { return plan_node().tnode_->cte_producer.name; }
  LocalExchanger* exchanger_;
};

}
