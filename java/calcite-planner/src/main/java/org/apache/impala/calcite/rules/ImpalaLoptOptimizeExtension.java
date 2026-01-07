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
package org.apache.impala.calcite.rules;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.impala.calcite.schema.ImpalaRelMdRowCount;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptJoinTree;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.LoptSemiJoinOptimizer;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.schema.ImpalaCost;
import org.apache.impala.calcite.schema.ImpalaRelColumnOrigin;
import org.apache.impala.calcite.schema.ImpalaRelMdNonCumulativeCost;
import org.apache.impala.catalog.Column;
import org.apache.impala.thrift.TQueryOptions;

import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ImpalaLoptOptimizeExtension {
  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaLoptOptimizeExtension.class.getName());

  public static boolean swapInputs(RelMetadataQuery mq, LoptMultiJoin multiJoin,
      LoptJoinTree leftTree, LoptJoinTree rightTree, RexNode condition,
      RexBuilder rexBuilder, boolean adjust) {
    MultiJoin mjRel = multiJoin.getMultiJoinRel();
    RelNode leftSide = leftTree.getJoinTree();
    RelNode rightSide = rightTree.getJoinTree();
    RuntimeFilterInfo runtimeFilterInfo =
        mjRel.getCluster().getPlanner().getContext().unwrap(RuntimeFilterInfo.class);

    runtimeFilterInfo.clear();
    RelOptCost leftSideWithoutFilterCost =
        getCumulativeCost(unwrapHepRelVertex(leftSide), mq);
    RelOptCost rightSideWithoutFilterCost =
        getCumulativeCost(unwrapHepRelVertex(rightSide), mq);

    List<RexNode> andConditions = ExprConjunctsConverter.getAndConjuncts(condition);

    RuntimeFilterReductionContext leftContext = createRuntimeFilterReductionContext(condition, adjust,
        mjRel, leftSide, rightSide, rightTree, mq, true);
    if (leftContext != null) {
      runtimeFilterInfo.reductionMap_.put(leftContext.tableScan_, leftContext);
    }
    RuntimeFilterReductionContext rightContext = createRuntimeFilterReductionContext(condition, adjust,
        mjRel, leftSide, rightSide, rightTree, mq, false);
    if (rightContext != null) {
      runtimeFilterInfo.reductionMap_.put(rightContext.tableScan_, rightContext);
    }

    RelOptCost leftSideWithFilterCost =
        getCumulativeCost(unwrapHepRelVertex(leftSide), mq);
    RelOptCost rightSideWithFilterCost =
        getCumulativeCost(unwrapHepRelVertex(rightSide), mq);

    RelOptCost totalPreJoinNonSwapCost = leftSideWithFilterCost.plus(rightSideWithoutFilterCost);
    RelOptCost totalJoinNonSwapCost = totalPreJoinNonSwapCost.plus(ImpalaRelMdNonCumulativeCost.getJoinCost(leftSide, rightSide, mq));
    RelOptCost totalPreJoinSwapCost = leftSideWithoutFilterCost.plus(rightSideWithFilterCost);

    RelOptCost totalJoinSwapCost = totalPreJoinSwapCost.plus(ImpalaRelMdNonCumulativeCost.getJoinCost(rightSide, leftSide, mq));
    return totalJoinSwapCost.isLe(totalJoinNonSwapCost);
  }

  private static boolean swapBitSides(ImmutableBitSet bitSet, boolean adjust, LoptJoinTree rightTree, RelNode join) {
    if (!adjust || !(join instanceof MultiJoin)) {
      return false;
    }
    MultiJoin multiJoin = (MultiJoin) join;

    final List<Integer> joinOrder = new ArrayList<>();
    rightTree.getTreeOrder(joinOrder);
    Preconditions.checkState(joinOrder.size() == 1);

    int factor = joinOrder.get(0);

    int startOfRightSideFields = 0;
    for (int i = 0; i < factor; ++i) {
      startOfRightSideFields += multiJoin.getInputs().get(i).getRowType().getFieldList().size();
    }
    int endOfRightSideFields = startOfRightSideFields + multiJoin.getInputs().get(factor).getRowType().getFieldList().size();

    return bitSet.nth(0) >= startOfRightSideFields && bitSet.nth(0) < endOfRightSideFields;
  }

  private static RuntimeFilterReductionContext createRuntimeFilterReductionContext(
      RexNode condition, boolean adjust,
      RelNode joinRel, RelNode leftSide, RelNode rightSide, LoptJoinTree rightTree,
      RelMetadataQuery mq, boolean isLeft) {

    TableScan ts = null;
    Double reduction = 1.0;
    Map<TableScan, RuntimeFilterReductionContext> reductionMap = new HashMap<>();
    List<RexNode> andConditions = ExprConjunctsConverter.getAndConjuncts(condition);
    for (RexNode andCondition : andConditions) { 
      ImmutableBitSet bitSet = RelOptUtil.InputFinder.bits(andCondition);

      if (bitSet.cardinality() != 2) {
        continue;
      }

      boolean swapBits = swapBitSides(bitSet, adjust, rightTree, joinRel);
      int leftIndex = getLeftIndex(joinRel, bitSet, swapBits, adjust);
      int rightIndex = getRightIndex(joinRel, bitSet, swapBits, adjust, leftSide);

      ImpalaRelColumnOrigin leftOrigin = (ImpalaRelColumnOrigin) mq.getColumnOrigin(leftSide, leftIndex);
      ImpalaRelColumnOrigin rightOrigin = (ImpalaRelColumnOrigin) mq.getColumnOrigin(rightSide, rightIndex);
      if (leftOrigin == null || rightOrigin == null) {
        continue;
      }

      if (isLeft) {
        ts = leftOrigin.getTableScan();
        reduction *= getReduction(rightSide, rightOrigin, rightIndex, mq);
      } else {
        reduction *= getReduction(leftSide, leftOrigin, leftIndex, mq);
        ts = rightOrigin.getTableScan();
      }
    }
    return ts != null ? new RuntimeFilterReductionContext(ts, reduction) : null;
  }

  public static Double getReduction(RelNode input, ImpalaRelColumnOrigin originCol,
      int inputColumnIndex, RelMetadataQuery mq) {
    ImmutableBitSet bitSet = ImmutableBitSet.of(inputColumnIndex);
    Double distinctRowCount = mq.getDistinctRowCount(input, bitSet, null);
    if (distinctRowCount == null) {
      return 1.0;
    }
    CalciteTable table = (CalciteTable) originCol.getOriginTable();
    Column tableColumn = table.getColumn(originCol.getOriginColumnOrdinal());
    Double reduction = distinctRowCount / tableColumn.getStats().getNumDistinctValues();
    return Math.min(reduction, 1.0);

  }

  public static RelOptCost getCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    RuntimeFilterInfo runtimeFilterInfo = rel.getCluster().getPlanner().getContext().unwrap(RuntimeFilterInfo.class);
    runtimeFilterInfo.inputRefs_ = getInputRefsForContext(rel, runtimeFilterInfo.inputRefs_, 0);
    RelOptCost cost = getCumulativeCostInternal(rel, mq);
    runtimeFilterInfo.inputRefs_ = null;
    return cost;
  }

  public static RelOptCost getCumulativeCostInternal(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof Join) {
      Join join = (Join) rel;
      RuntimeFilterInfo runtimeFilterInfo = join.getCluster().getPlanner().getContext().unwrap(RuntimeFilterInfo.class);
      List<RexNode> andConditions = ExprConjunctsConverter.getAndConjuncts(join.getCondition());
      if (andConditions.size() <= 1 && join.getJoinType() != JoinRelType.INNER) {
        RuntimeFilterReductionContext context = createRuntimeFilterReductionContext(andConditions.get(0), false,
            join, join.getInput(0), join.getInput(1), null, mq, true);
        if (shouldUseContext(runtimeFilterInfo.reductionMap_, context)) {
          runtimeFilterInfo.reductionMap_.put(context.tableScan_, context);
        }
      }
    }
    ImpalaRelMdNonCumulativeCost noncumulativeCostHandler =
        new ImpalaRelMdNonCumulativeCost();
    RelOptCost cost = noncumulativeCostHandler.getNonCumulativeCost(rel, mq);
    RuntimeFilterInfo runtimeFilterInfo = rel.getCluster().getPlanner().getContext().unwrap(RuntimeFilterInfo.class);
    if (cost == null) {
      return null;
    }
    List<RelNode> inputs = rel.getInputs();
    for (int i = 0; i < inputs.size(); ++i) {
      runtimeFilterInfo.inputRefs_ = getInputRefsForContext(rel, runtimeFilterInfo.inputRefs_, i);
      RelNode realInput = inputs.get(i);
      if (realInput instanceof HepRelVertex) {
        realInput = ((HepRelVertex) realInput).getCurrentRel();
      }
      RelOptCost inputCost = getCumulativeCostInternal(realInput, mq);
      if (inputCost == null) {
        return null;
      }
      cost = cost.plus(inputCost);
    }
    return cost;
  }

  public static boolean shouldUseContext(
      Map<TableScan, RuntimeFilterReductionContext> map,
      RuntimeFilterReductionContext context) {
    if (context == null) {
      return false;
    }
    if (!map.containsKey(context.tableScan_)) {
      return true;
    }
    RuntimeFilterReductionContext currentContext = map.get(context.tableScan_);
    return context.reductionPercentage_ < currentContext.reductionPercentage_;
  }

  private static ImmutableBitSet getInputRefsForContext(RelNode rel, ImmutableBitSet currentSet, int i) {
    if (currentSet == null) {
      currentSet = ImmutableBitSet.of();
    }
    switch (ImpalaPlanRel.getRelNodeType(rel)) {
      case AGGREGATE:
      case SORT:
      case UNION:
        return ImmutableBitSet.range(rel.getInputs().get(0).getRowType().getFieldList().size());
      case HDFSSCAN:
      case VALUES:
        return ImmutableBitSet.range(rel.getRowType().getFieldList().size());
      case JOIN:
        int leftSize = rel.getInputs().get(0).getRowType().getFieldList().size();
        return (i == 0)
            ? ImmutableBitSet.range(leftSize)
            : ImmutableBitSet.range(rel.getRowType().getFieldList().size() - leftSize);
      case PROJECT:
        return RelOptUtil.InputFinder.bits(((Project) rel).getProjects(), null);
      case FILTER:
        return currentSet.union(RelOptUtil.InputFinder.bits(((Filter) rel).getCondition()));
      default:
        throw new RuntimeException("Unknown RelNodeType: " + ImpalaPlanRel.getRelNodeType(rel));
    }
  }

  private static int getLeftIndex(RelNode multiJoinRel, ImmutableBitSet bitSet,
      boolean swapBits, boolean adjust) {
    int leftBit = swapBits ? bitSet.nth(1) : bitSet.nth(0);
    return adjust ? getAdjustedIndex(multiJoinRel, leftBit) : leftBit;
  }

  private static int getRightIndex(RelNode multiJoinRel, ImmutableBitSet bitSet,
      boolean swapBits, boolean adjust, RelNode leftSide) {
    int rightBit = swapBits ? bitSet.nth(0) : bitSet.nth(1);
    return adjust
        ? getAdjustedIndex(multiJoinRel, rightBit)
        : rightBit - leftSide.getRowType().getFieldList().size();
  }

  private static int getAdjustedIndex(RelNode relNode, int index) {
    Preconditions.checkState(relNode instanceof MultiJoin);
    MultiJoin multiJoin = (MultiJoin) relNode;
    int totalFieldsSoFar = 0;
    for (RelNode r : multiJoin.getInputs()) {
      if (index - totalFieldsSoFar < r.getRowType().getFieldList().size()) {
        return index - totalFieldsSoFar;
      }
      totalFieldsSoFar += r.getRowType().getFieldList().size();
    }
    throw new RuntimeException("SJC: EXCEPTION");
  }

  private static RelNode unwrapHepRelVertex(RelNode relNode) {
    return relNode instanceof HepRelVertex
        ? ((HepRelVertex)relNode).getCurrentRel()
        : relNode;
  }

  public static class JoinRelNodes {
    public final RelNode left_;
    public final RelNode right_;
    public JoinRelNodes(RelNode left, RelNode right) {
      left_ = left;
      right_ = right;
    }

    @Override public int hashCode() {
      return Objects.hash(left_, right_);
    }
 
    @Override public boolean equals(Object obj) {
      if (!(obj instanceof JoinRelNodes)) {
        return false;
      }
      JoinRelNodes other = (JoinRelNodes) obj;
      return (this == other) ||
              (this.left_.equals(other.left_) && this.right_.equals(other.right_));
    }
  }

  public static class RuntimeFilterInfo implements Context {
    public final Map<TableScan, RuntimeFilterReductionContext> reductionMap_ = new HashMap<>();
    public ImmutableBitSet inputRefs_;
    public TQueryOptions queryOptions_;

    public RuntimeFilterInfo(TQueryOptions queryOptions) {
      queryOptions_ = queryOptions;
    }

    @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
      return clazz.isInstance(this) ? clazz.cast(this) : null;
    }

    public void clear() {
      reductionMap_.clear();
    }
  }

  public static class RuntimeFilterReductionContext {
    public final TableScan tableScan_;
    public final Double reductionPercentage_;
    //XXX: temp variable

    public RuntimeFilterReductionContext(TableScan tableScan,
        Double reductionPercentage) {
      this.tableScan_ = tableScan;
      this.reductionPercentage_ = reductionPercentage;
    }

    public static Double getTotalReductionPercentage(boolean useLeft,
        List<RuntimeFilterReductionContext> reductionList) {
      Double totalReduction = 1.0;
      for (RuntimeFilterReductionContext r : reductionList) {
        totalReduction = Math.min(totalReduction, r.reductionPercentage_);
      }
      return totalReduction;
    }
  }

}
