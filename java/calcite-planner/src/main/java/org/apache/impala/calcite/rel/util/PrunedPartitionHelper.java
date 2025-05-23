/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.rel.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter.CalciteImpalaConjunct;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.HdfsEstimatedMissingTableStats;
import org.apache.impala.planner.HdfsPartitionPruner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PrunedPartitionHelper is a wrapper around the Impala Planner's partition
 * pruner and holds the partitions remaining after pruning and the conjuncts
 * separated by the ones that pruned and the ones that did not prune.
 */
public class PrunedPartitionHelper {

  private final List<? extends FeFsPartition> prunedPartitions_;

  private final List<Expr> partitionedConjuncts_;

  private final List<Expr> nonPartitionedConjuncts_;

  private final RexNode nonPartitionedCalciteConjunct_;

  private final FeFsTable table_;

  private final Analyzer analyzer_;

  // not final because this is lazy loaded.
  private Double prunedRowCount_ = null;

  public PrunedPartitionHelper(CalciteTable table,
      ExprConjunctsConverter converter, TupleDescriptor tupleDesc,
      RexBuilder rexBuilder,
      Analyzer analyzer) throws ImpalaException {

    this.table_ = table.getFeFsTable();
    this.analyzer_ = analyzer;
    HdfsPartitionPruner pruner = new HdfsPartitionPruner(tupleDesc);

    List<CalciteImpalaConjunct> conjuncts = converter.getConjuncts();
    // IMPALA-13849: tblref is null.  Tablesampling is disabled.
    Pair<List<? extends FeFsPartition>, List<Expr>> impalaPair =
        pruner.prunePartitions(analyzer,
            new ArrayList<>(converter.getImpalaConjuncts()), true, false, null);

    prunedPartitions_ = impalaPair.first;

    ImmutableList.Builder<Expr> partitionedConjBuilder =
        new ImmutableList.Builder();
    ImmutableList.Builder<Expr> nonPartitionedConjBuilder =
        new ImmutableList.Builder();
    List<SlotId> partitionSlots = tupleDesc.getPartitionSlots();

    List<CalciteImpalaConjunct> allConjuncts = converter.getConjuncts();
    RexNode tmpCalciteConjunct = null;
    for (CalciteImpalaConjunct conjunct : allConjuncts) {
      if (HdfsPartitionPruner.isPartitionPrunedFilterConjunct(
          partitionSlots, conjunct.impalaConjunct_, false)) {
        partitionedConjBuilder.add(conjunct.impalaConjunct_);
      } else {
        nonPartitionedConjBuilder.add(conjunct.impalaConjunct_);
        tmpCalciteConjunct = (tmpCalciteConjunct == null)
            ? conjunct.calciteConjunct_
            : rexBuilder.makeCall(SqlStdOperatorTable.AND,
              ImmutableList.of(tmpCalciteConjunct, conjunct.calciteConjunct_));
      }
    }
    nonPartitionedCalciteConjunct_ = tmpCalciteConjunct;

    partitionedConjuncts_ = partitionedConjBuilder.build();
    nonPartitionedConjuncts_ = nonPartitionedConjBuilder.build();
  }

  public List<? extends FeFsPartition> getPrunedPartitions() {
    return prunedPartitions_;
  }

  public RexNode getNonPartitionedConjunct() {
    return nonPartitionedCalciteConjunct_;
  }

  public Double getPrunedRowCount() {
    if (prunedRowCount_ == null) {
      Double calculatedRowCount = calculatePartitionRows(table_, prunedPartitions_);
      if (calculatedRowCount == null) {
        HdfsEstimatedMissingTableStats estimatedMissingStats =
            new HdfsEstimatedMissingTableStats(analyzer_.getQueryOptions(), table_,
            prunedPartitions_, -1);
        calculatedRowCount = new Double(estimatedMissingStats.statsNumRows_);
        if (calculatedRowCount < 0.0) {
          calculatedRowCount = Double.MAX_VALUE;
        }
      }
      prunedRowCount_ = calculatedRowCount;
    }
    return prunedRowCount_;
  }

  private Double calculatePartitionRows(FeFsTable table,
      List<? extends FeFsPartition> partitions) {
    if (table_.getNumRows() < 0.0) {
      return null;
    }
    Double prunedRowCount = 0.0;
    for (FeFsPartition part : prunedPartitions_) {
      if (part.getNumRows() < 0.0) {
        return null;
      }
      prunedRowCount += part.getNumRows();
    }
    return prunedRowCount;
  }

  public List<Expr> getPartitionedConjuncts() {
    return partitionedConjuncts_;
  }

  public List<Expr> getNonPartitionedConjuncts() {
    return nonPartitionedConjuncts_;
  }
}
