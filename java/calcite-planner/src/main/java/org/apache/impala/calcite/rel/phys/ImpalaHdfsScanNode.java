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

package org.apache.impala.calcite.rel.phys;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.planner.HdfsScanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * ImpalaHdfsScanNode. Extends the HdfsScanNode to bypass processing of the
 * assignedConjuncts.
 */
public class ImpalaHdfsScanNode extends HdfsScanNode {
  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaHdfsScanNode.class.getName());

  private final List<Expr> assignedConjuncts_;

  private final TableRef hdfsTblRef_;

  private final Double calciteCardinality_;

  public ImpalaHdfsScanNode(PlanNodeId id, TupleDescriptor tupleDesc,
      List<? extends FeFsPartition> partitions,
      TableRef hdfsTblRef, MultiAggregateInfo aggInfo, List<Expr> partConjuncts,
      List<Expr> assignedConjuncts, SlotDescriptor countStarDescriptor,
      boolean isPartitionScanOnly) {
    this(id, tupleDesc, partitions, hdfsTblRef, aggInfo, partConjuncts, assignedConjuncts,
        countStarDescriptor, isPartitionScanOnly, null);
  }

  public ImpalaHdfsScanNode(PlanNodeId id, TupleDescriptor tupleDesc,
      List<? extends FeFsPartition> partitions,
      TableRef hdfsTblRef, MultiAggregateInfo aggInfo, List<Expr> partConjuncts,
      List<Expr> assignedConjuncts, SlotDescriptor countStarDescriptor,
      boolean isPartitionScanOnly, Double calciteCardinality) {
    super(id, tupleDesc, assignedConjuncts, partitions, hdfsTblRef, aggInfo,
        partConjuncts, isPartitionScanOnly);
    this.assignedConjuncts_ = assignedConjuncts;
    this.countStarSlot_ = countStarDescriptor;
    this.hdfsTblRef_ = hdfsTblRef;
    this.calciteCardinality_ = calciteCardinality;
  }

  public TableRef getTableRef() {
    return hdfsTblRef_;
  }

  @Override
  public void assignConjuncts(Analyzer analyzer) {
    // ignore analyzer and retrieve the processed Calcite conjuncts
    this.conjuncts_ = assignedConjuncts_;
  }

  @Override
  public String getCalciteCardinalityString() {
    String ret = ", calciteCardinality=";
    ret += (calciteCardinality_ == null) ? "unknown" : calciteCardinality_;
    return ret;
  }
}
