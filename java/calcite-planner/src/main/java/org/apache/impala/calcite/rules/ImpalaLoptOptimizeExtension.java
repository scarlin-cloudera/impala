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

  public static boolean swapInputs(RelMetadataQuery mq, LoptMultiJoin multiJoin, LoptJoinTree leftTree, LoptJoinTree rightTree, RexNode condition, RexBuilder rexBuilder, boolean adjust) {
    MultiJoin multiJoinRel = multiJoin.getMultiJoinRel();
    RuntimeFilterInfo runtimeFilterInfo = multiJoinRel.getCluster().getPlanner().getContext().unwrap(RuntimeFilterInfo.class);
    if (runtimeFilterInfo != null) {
      runtimeFilterInfo.clear();
    } else {
    }
    Context context = multiJoinRel.getCluster().getPlanner().getContext();


    int [] adjustments = new int[multiJoin.getNumTotalFields()];
    List<RexNode> andConditions = ExprConjunctsConverter.getAndConjuncts(condition);
    RelNode leftSide = leftTree.getJoinTree();
    RelNode rightSide = rightTree.getJoinTree();
    Map<CalciteTable, Double> leftReductionMap = new HashMap<>();
    Map<CalciteTable, Double> rightReductionMap = new HashMap<>();

    if (andConditions.size() > 1) {
      return false;
    }
    for (RexNode andCondition : andConditions) { 
      runtimeFilterInfo.reductionMap_.putAll(createRuntimeFilterReductionContext(andCondition, adjust,
          multiJoinRel, leftSide, rightSide, rightTree, mq));
    }

    //XXX: ugly, but will fix with cost model, hopefully
    /*
    List<RuntimeFilterReductionContext> leftReductionList = null;
    List<RuntimeFilterReductionContext> rightReductionList = null;
    for (TableScan ts : runtimeFilterInfo.reductionMap_.keySet()) {
      Preconditions.checkState(runtimeFilterInfo.reductionMap_.size() == 2);
      if (runtimeFilterInfo.reductionMap_.get(ts).get(0).isLeft_) {
        leftReductionList = runtimeFilterInfo.reductionMap_.get(ts);
      } else {
        rightReductionList = runtimeFilterInfo.reductionMap_.get(ts);
      }
    }

    Double leftTotalReduction = 0.0;
    Double rightTotalReduction = 0.0;
    if (leftReductionList != null) {
      leftTotalReduction = RuntimeFilterReductionContext.getTotalReduction(leftReductionList);
    } else {
    }
    if (rightReductionList != null) {
      rightTotalReduction = RuntimeFilterReductionContext.getTotalReduction(rightReductionList);
    } else {
    }


//    Double totalLeftReduction = leftTable.getRowCount() - leftTable.getRowCount() * leftReduction;
//    Double totalRightReduction = rightTable.getRowCount() - rightTable.getRowCount() * rightReduction;

*/
    RelNode relNode3 = leftSide;
    if (relNode3 instanceof HepRelVertex) {
      relNode3 = ((HepRelVertex)relNode3).getCurrentRel();
    }

    runtimeFilterInfo.useLeft_ = true;
    RelOptCost leftSideWithFilterCost = getCumulativeCost(relNode3, mq);
    runtimeFilterInfo.useLeft_ = false;
    RelOptCost leftSideWithoutFilterCost = getCumulativeCost(relNode3, mq);
    relNode3 = rightSide;
    if (relNode3 instanceof HepRelVertex) {
      relNode3 = ((HepRelVertex)relNode3).getCurrentRel();
    }
    runtimeFilterInfo.useLeft_ = false;
    RelOptCost rightSideWithFilterCost = getCumulativeCost(relNode3, mq);
    runtimeFilterInfo.useLeft_ = true;
    RelOptCost rightSideWithoutFilterCost = getCumulativeCost(relNode3, mq);
    RelOptCost totalPreJoinNonSwapCost = leftSideWithFilterCost.plus(rightSideWithoutFilterCost);
    RelOptCost totalPreJoinSwapCost = leftSideWithoutFilterCost.plus(rightSideWithFilterCost);

    RelOptCost totalJoinNonSwapCost = totalPreJoinNonSwapCost.plus(ImpalaRelMdNonCumulativeCost.getJoinCost(leftSide, rightSide, mq));
    RelOptCost totalJoinSwapCost = totalPreJoinSwapCost.plus(ImpalaRelMdNonCumulativeCost.getJoinCost(rightSide, leftSide, mq));
    return totalJoinSwapCost.isLe(totalJoinNonSwapCost);
    /*
    if (mq.getRowCount(leftSide) > mq.getRowCount(rightSide)) {
      if (leftSideCost.isLe(rightSideCost)) {
        return true;
      }
      if (rightReductionList == null) {
        return false;
      } else if (rightTotalReduction > mq.getRowCount(leftSide)) {
        return true;
      } else {
        return false;
      }
    } else {
      if (leftReductionList == null) {
        return true;
    } else if (leftTotalReduction > mq.getRowCount(rightSide)) {
        return false;
      } else {
        return true;
      }
    }
    */
  }

  private static boolean swapBitSides(ImmutableBitSet bitSet, boolean adjust, LoptJoinTree rightTree, MultiJoin multiJoin) {
    if (!adjust) {
      return false;
    }

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

  private static Map<TableScan, List<RuntimeFilterReductionContext>> createRuntimeFilterReductionContext(
      RexNode condition, boolean adjust,
      MultiJoin multiJoinRel, RelNode leftSide, RelNode rightSide, LoptJoinTree rightTree,
      RelMetadataQuery mq) {
    Map<TableScan, List<RuntimeFilterReductionContext>> reductionMap = new HashMap<>();
    JoinRelNodes joinRelNodes = new JoinRelNodes(leftSide, rightSide);
    ImmutableBitSet bitSet = RelOptUtil.InputFinder.bits(condition);

    if (bitSet.cardinality() != 2) {
      return reductionMap;
    }

    Double leftReduction = 1.0;
    Double rightReduction = 1.0;
    int leftBit = bitSet.nth(0);
    int rightBit = bitSet.nth(1);
    if (swapBitSides(bitSet, adjust, rightTree, multiJoinRel)) {
      leftBit = bitSet.nth(1);
      rightBit = bitSet.nth(0);
    }
    int leftIndex = adjust ? getAdjustedIndex(multiJoinRel, leftBit) : leftBit;
    int rightIndex = adjust ? getAdjustedIndex(multiJoinRel, rightBit) : rightBit - leftSide.getRowType().getFieldList().size();
//    Set<Integer> rightPreAdjustedIndexList = getRightPreAdjustedIndexList(bitSet, adjust, rightTree, multiJoinRel);
//    Set<Integer> leftPreAdjustedIndexList = getLeftPreAdjustedIndexList(bitSet, rightPreAdjustedIndexList);
    ImpalaRelColumnOrigin leftOrigin = (ImpalaRelColumnOrigin) mq.getColumnOrigin(leftSide, leftIndex);
    ImpalaRelColumnOrigin rightOrigin = (ImpalaRelColumnOrigin) mq.getColumnOrigin(rightSide, rightIndex);
    if (leftOrigin != null) {
      CalciteTable leftTable = (CalciteTable) leftOrigin.getOriginTable();
      Column leftTableColumn = leftTable.getColumn(leftOrigin.getOriginColumnOrdinal());
      ImmutableBitSet leftBitSet = ImmutableBitSet.of(leftIndex);
      rightReduction = mq.getDistinctRowCount(leftSide, leftBitSet, null) / leftTableColumn.getStats().getNumDistinctValues();
      if (leftTable.getName().equals("store_sales") && rightReduction < .1) {
      }
    }
    rightReduction = Math.min(rightReduction, 1.0);

    if (rightOrigin != null) {
      CalciteTable rightTable = (CalciteTable) rightOrigin.getOriginTable();
      Column rightTableColumn = rightTable.getColumn(rightOrigin.getOriginColumnOrdinal());
      ImmutableBitSet rightBitSet = ImmutableBitSet.of(rightIndex);
      leftReduction = mq.getDistinctRowCount(rightSide, rightBitSet, null) / rightTableColumn.getStats().getNumDistinctValues();
    }
    leftReduction = Math.min(leftReduction, 1.0);

    List<RuntimeFilterReductionContext> contextList;
    if (rightOrigin != null) {
      contextList =
           reductionMap.computeIfAbsent(rightOrigin.getTableScan(), k -> new ArrayList<>());
      contextList.add(new RuntimeFilterReductionContext(joinRelNodes, rightOrigin.getTableScan(), rightReduction, false, false));
    }
    if (leftOrigin != null) {
      contextList =
           reductionMap.computeIfAbsent(leftOrigin.getTableScan(), k -> new ArrayList<>());
      contextList.add(new RuntimeFilterReductionContext(joinRelNodes, leftOrigin.getTableScan(), leftReduction, true, false));
    }
    return reductionMap;
  }

  public static RelOptCost getCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    RuntimeFilterInfo runtimeFilterInfo = rel.getCluster().getPlanner().getContext().unwrap(RuntimeFilterInfo.class);
    runtimeFilterInfo.inputRefs_ = getInputRefsForContext(rel, runtimeFilterInfo.inputRefs_, 0);
    RelOptCost cost = getCumulativeCostInternal(rel, mq);
    runtimeFilterInfo.inputRefs_ = null;
    return cost;
  }

  public static RelOptCost getCumulativeCostInternal(RelNode rel, RelMetadataQuery mq) {
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

  private static int getAdjustedIndex(MultiJoin multiJoin, int index) {
    int totalFieldsSoFar = 0;
    for (RelNode r : multiJoin.getInputs()) {
      if (index - totalFieldsSoFar < r.getRowType().getFieldList().size()) {
        return index - totalFieldsSoFar;
      }
      totalFieldsSoFar += r.getRowType().getFieldList().size();
    }
    throw new RuntimeException("SJC: EXCEPTION");
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
    public final Map<TableScan, List<RuntimeFilterReductionContext>> reductionMap_ = new HashMap<>();
    public boolean useLeft_;
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
    public final JoinRelNodes joinRelNodes_;
    public final TableScan tableScan_;
    public final Double reductionPercentage_;
    //XXX: temp variable
    public final boolean isLeft_;
    public final boolean useAlways_;

    public RuntimeFilterReductionContext(JoinRelNodes joinRelNodes, TableScan tableScan,
        Double reductionPercentage, boolean isLeft, boolean useAlways) {
      this.joinRelNodes_ = joinRelNodes;
      this.tableScan_ = tableScan;
      this.reductionPercentage_ = reductionPercentage;
      this.isLeft_ = isLeft;
      this.useAlways_ = useAlways;
    }

    public static Double getTotalReductionPercentage(boolean useLeft,
        List<RuntimeFilterReductionContext> reductionList) {
      Double totalReduction = 1.0;
      for (RuntimeFilterReductionContext r : reductionList) {
        if (useLeft == r.isLeft_ || r.useAlways_) {
          totalReduction *=  r.reductionPercentage_;
        }
      }
      return totalReduction;
    }
  }

}
