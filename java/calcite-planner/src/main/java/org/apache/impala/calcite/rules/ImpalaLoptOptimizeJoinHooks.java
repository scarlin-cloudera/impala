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
import com.google.common.collect.Range;

/**
 * ImpalaLoptOptimizeJoinHooks contains the hooks for the ImpalaLoptOptimizeJoinRule.
 *
 * These hooks are:
 * getCumulativeCost(): returns the cumulative ImpalaCost cost for a given RelNode.
 * swapInputs(): Returns true if the given left/right RelNode inputs should be flipped.
 */
public class ImpalaLoptOptimizeJoinHooks {

  /**
   * swapInputs Compares left side and right side and decides if it would be
   * better to flip the sides to produce a better plan.
   *
   * @param mq - RelMetaDataQuery object providing metadata information.
   * @param multiJoin  - The multiJoin RelNode containing the left and right inputs
   * @param leftTree   - The left tree
   * @param rightTree  - The right tree
   * @param condition  - The RexNode condition.  May contain multiple "and" conditions.
   * @param rexBuilder
   * @param adjust     - flag specifying whether adjustment needs to be made based on the
   *                     inputRefs in the condition. If false, the inputrefs can be used
   *                     directly off the leftTree and rightTree. If true, the inputRefs
   *                     need to be calculated based on the "factor" of the multiJoin.
   * @return should the inputs be swapped based on cost analysis.
   */
  public static boolean swapInputs(RelMetadataQuery mq, LoptMultiJoin multiJoin,
      LoptJoinTree leftTree, LoptJoinTree rightTree, RexNode condition,
      RexBuilder rexBuilder, boolean adjust) {
    MultiJoin mjRel = multiJoin.getMultiJoinRel();
    RelNode leftSide = leftTree.getJoinTree();
    RelNode rightSide = rightTree.getJoinTree();
    // The mqContext is a global value for the query within the current planner.
    // It needs to be here so it can be passed through the RelMetaDataQuery framework.
    ImpalaMQContext mqContext =
        mjRel.getCluster().getPlanner().getContext().unwrap(ImpalaMQContext.class);

    // If there is no swapping needed, we need the cost of the right side without any
    // runtime filter since the runtime filter reduction only happens on the left side.
    RelOptCost rightSideWithoutFilterCost =
        getCumulativeCost(unwrapHepRelVertex(rightSide), mq);
    // If there is swapping needed, we need the cost of the left side without any
    // runtime filter since the runtime filter reduction only happens on the right side.
    // (that is, right side before the swap happens, which would eventually be on the
    // left side)).
    RelOptCost leftSideWithoutFilterCost =
        getCumulativeCost(unwrapHepRelVertex(leftSide), mq);

    // InputInfo is a helper class to get the index for the conditions within the
    // RelNode trees.
    InputInfo inputInfo = adjust
        ? new MultiJoinInputInfo(mjRel, leftTree, rightTree)
        : new JoinInputInfo(leftSide, rightSide);

    // Create the runtime filter information for the left side and calculate the cost.
    mqContext.reductionMap_.putAll(createRuntimeFilterReductionInfo(mqContext, condition,
        inputInfo, mq, /*isLeft*/ true));
    RelOptCost leftSideWithFilterCost =
        getCumulativeCost(unwrapHepRelVertex(leftSide), mq);

    // Create the runtime filter information for the right side and calculate the cost.
    mqContext.reductionMap_.putAll(createRuntimeFilterReductionInfo(mqContext, condition,
        inputInfo, mq, /*isLeft*/ false));
    RelOptCost rightSideWithFilterCost =
        getCumulativeCost(unwrapHepRelVertex(rightSide), mq);

    // Calculate the cumulative cost for the inputs underneath the join when there is no
    // swapping.
    RelOptCost totalPreJoinNonSwapCost =
        leftSideWithFilterCost.plus(rightSideWithoutFilterCost);
    // Add in the noncumulative cost at the join node to the cumulative input cost.
    RelOptCost totalJoinNonSwapCost = totalPreJoinNonSwapCost.plus(
        ImpalaRelMdNonCumulativeCost.getJoinCost(leftSide, rightSide, mq));

    // Calculate the cumulative cost for the inputs underneath the join when there is
    // swapping.
    RelOptCost totalPreJoinSwapCost =
        leftSideWithoutFilterCost.plus(rightSideWithFilterCost);
    // Add in the noncumulative cost at the join node to the cumulative input cost.
    RelOptCost totalJoinSwapCost = totalPreJoinSwapCost.plus(
        ImpalaRelMdNonCumulativeCost.getJoinCost(rightSide, leftSide, mq));

    // Compare the costs of swapping versus non-swapping.
    return totalJoinSwapCost.isLt(totalJoinNonSwapCost);
  }

  /**
   * getCumulativeCost() gets the cumulative cost of the given RelNode.
   */
  public static RelOptCost getCumulativeCost(RelOptRuleCall call, RelNode relNode) {
    return getCumulativeCost(relNode, call.getMetadataQuery());
  }

  public static RelOptCost getCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    ImpalaMQContext mqContext =
        rel.getCluster().getPlanner().getContext().unwrap(ImpalaMQContext.class);
    // Initialize the inputRefs member so that the current RelNode being analyzed
    // knows which inputRefs are used by its parent. Since this is only called at
    // the top level, the inputRefs are initalized to empty unless the current
    // RelNode has no inputs. If it has no inputs, then all of the fields are used
    // by its (non-existent) parent.
    mqContext.setInputRefs((rel.getInputs().size() == 0)
        ? ImmutableBitSet.range(rel.getRowType().getFieldList().size())
        : ImmutableBitSet.of());
    // call the recursive method getCumulativeCostInternal() to get the cost of
    // this RelNode and all its inputs.
    RelOptCost cost = getCumulativeCostInternal(rel, mq);
    mqContext.clear();
    return cost;
  }

  /**
   * createRuntimeFilterReductionInfo creates the runtime filter contexts for
   * the given inputInfo on the given side (left side if isLeft is true).
   *
   * The condition passed in can be an "and" condition which is broken up into
   * multiple conditions.
   *
   * Each individual condition can be applied to a different table scan which is
   * why a Map is returned.
   *
   * If two conditions from the and clause map to the same table, this algorithm
   * guesses that the columns are independent of each other and thus multiplies
   * the reduction percentage.
   */
  private static Map<TableScan, ImpalaMQContext.RuntimeFilterReductionInfo>
      createRuntimeFilterReductionInfo(ImpalaMQContext context, RexNode condition,
          InputInfo inputInfo, RelMetadataQuery mq, boolean isLeft) {
    Map<TableScan, ImpalaMQContext.RuntimeFilterReductionInfo> reductionMap =
        new HashMap<>();

    if (!context.calculateRuntimeFilters_) {
      return reductionMap;
    }

    // break up the and condition into multiple conditions.
    List<RexNode> andConditions = ExprConjunctsConverter.getAndConjuncts(condition);

    for (RexNode andCondition : andConditions) {
      // only handle equality conditions for now with one input ref on each side.
      if (!isEqualsCondition(andCondition)) {
        continue;
      }

      ImmutableBitSet bitSet = RelOptUtil.InputFinder.bits(andCondition);
      Preconditions.checkState(bitSet.cardinality() == 2);

      Integer leftIndex = inputInfo.getLeftIndex(bitSet);
      Integer rightIndex = inputInfo.getRightIndex(bitSet);
      // If index couldn't be found, do not create runtime filter
      if (leftIndex == null || rightIndex == null) {
        continue;
      }

      ImpalaRelColumnOrigin leftOrigin =
          (ImpalaRelColumnOrigin) mq.getColumnOrigin(inputInfo.leftSide_, leftIndex);
      ImpalaRelColumnOrigin rightOrigin =
          (ImpalaRelColumnOrigin) mq.getColumnOrigin(inputInfo.rightSide_, rightIndex);
      // If the table scan origin column could not be identified, skip the condition.
      if (leftOrigin == null || rightOrigin == null) {
        continue;
      }

      TableScan ts = (isLeft) ? leftOrigin.getTableScan() : rightOrigin.getTableScan();
      if (ts == null) {
        continue;
      }
      Double reduction = reductionMap.containsKey(ts)
          ? reductionMap.get(ts).reductionPercentage_ : 1.0;
      // If we are placing the runtime filter on the left side, we use the
      // reduction percentage of the right side. If there are multiple conditions
      // for the same table, we treat them as independent and multiply the
      // reduction percentage.
      reduction *= (isLeft)
          ? getReduction(inputInfo.rightSide_, rightOrigin, rightIndex, mq)
          : getReduction(inputInfo.leftSide_, leftOrigin, leftIndex, mq);

      reductionMap.put(ts, new ImpalaMQContext.RuntimeFilterReductionInfo(ts, reduction));
    }
    return reductionMap;
  }


  /**
   * getReduction calculates the runtime reduction percentage. The percentage
   * is the ratio of the distinct values on the RelNode right under the join
   * divided by the total number of distinct values for the column at the table
   * scan level.
   */
  private static Double getReduction(RelNode input, ImpalaRelColumnOrigin originCol,
      int inputColumnIndex, RelMetadataQuery mq) {
    ImmutableBitSet bitSet = ImmutableBitSet.of(inputColumnIndex);
    Double distinctRowCount = mq.getDistinctRowCount(input, bitSet, null);
    // Could not calculate the distinct row count for the input, no reduction.
    if (distinctRowCount == null) {
      return 1.0;
    }

    CalciteTable table = (CalciteTable) originCol.getOriginTable();
    Column tableColumn = table.getColumn(originCol.getOriginColumnOrdinal());
    long tsNDVs = tableColumn.getStats().getNumDistinctValues();
    if (tsNDVs <= 0) {
      return 1.0;
    }
    Double reduction = distinctRowCount / tsNDVs;
    Preconditions.checkState(reduction >= 0.0);
    return Math.min(reduction, 1.0);
  }

  /**
   * getCumulativeCostInternal gets the cumulative cost of the given RelNode. This method
   * gets called recursively and adds its children's cost to the cost of the RelNode
   * passed in. It will also create runtimefilter information if the current iteration
   * is a join node.
   */
  private static RelOptCost getCumulativeCostInternal(RelNode rel, RelMetadataQuery mq) {
    ImpalaMQContext mqContext =
        rel.getCluster().getPlanner().getContext().unwrap(ImpalaMQContext.class);

    // create join runtime filters and apply them if necessary
    Map<TableScan, ImpalaMQContext.RuntimeFilterReductionInfo> reductionInfoMap =
        createJoinRuntimeFilterReductionInfo(mqContext, rel, mq);
    for (ImpalaMQContext.RuntimeFilterReductionInfo reductionInfo :
        reductionInfoMap.values()) {
      if (shouldUseReductionInfo(mqContext.reductionMap_, reductionInfo)) {
        mqContext.reductionMap_.put(reductionInfo.tableScan_, reductionInfo);
      }
    }

    ImpalaRelMdNonCumulativeCost nonCumulativeCostHandler =
        new ImpalaRelMdNonCumulativeCost();
    // calculate cost for this RelNode.
    RelOptCost cost = nonCumulativeCostHandler.getNonCumulativeCost(rel, mq);
    if (cost == null) {
      return null;
    }
    // recursively get cumulative costs for childrean and add them to this cost.
    List<RelNode> inputs = rel.getInputs();
    for (int i = 0; i < inputs.size(); ++i) {
      mqContext.setInputRefs(getInputRefsForContext(rel, mqContext.getInputRefs(), i));
      RelNode realInput = unwrapHepRelVertex(inputs.get(i));
      RelOptCost inputCost = getCumulativeCostInternal(realInput, mq);
      if (inputCost == null) {
        return null;
      }
      cost = cost.plus(inputCost);
    }
    return cost;
  }

  private static Map<TableScan, ImpalaMQContext.RuntimeFilterReductionInfo>
      createJoinRuntimeFilterReductionInfo(ImpalaMQContext context, RelNode rel,
          RelMetadataQuery mq) {
    Map<TableScan, ImpalaMQContext.RuntimeFilterReductionInfo> reductionMap =
        new HashMap<>();

    // only create new runtime filter if relNode is a Join
    if (!(rel instanceof Join)) {
      return reductionMap;
    }

    Join join = (Join) rel;
    // only create runtime filter if it's an inner join.
    if (join.getJoinType() != JoinRelType.INNER) {
      return reductionMap;
    }

    InputInfo inputInfo = new JoinInputInfo(join.getInput(0), join.getInput(1));
    // only create runtime filters for left side of join, since  this is called from
    // an already created join which only applies runtime filters on the right side.
    return createRuntimeFilterReductionInfo(context, join.getCondition(), inputInfo, mq,
        true);
  }
  /**
   * shouldUseReductionInfo is a helper method to decide if the given reductionInfo should
   * be placed with the given map.  It will be placed in if either a) it doesn't
   * exist in the map or b) the new reductionInfo has a better reduction percentage
   * than the current one in the map.
   *
   * With this call, the current map contains a runtime filter from a different
   * join. There is no way of knowing if the conditions are independent of each
   * other. so the assumption here is that they are not independent and the
   * reduction percentage used is the lower of the two percentages.
   */
  private static boolean shouldUseReductionInfo(
      Map<TableScan, ImpalaMQContext.RuntimeFilterReductionInfo> map,
      ImpalaMQContext.RuntimeFilterReductionInfo reductionInfo) {
    if (!map.containsKey(reductionInfo.tableScan_)) {
      return true;
    }
    ImpalaMQContext.RuntimeFilterReductionInfo currentInfo =
        map.get(reductionInfo.tableScan_);
    return reductionInfo.reductionPercentage_ < currentInfo.reductionPercentage_;
  }

  /**
   * Return the bitset of the inputrefs used for the given RelNode and its parent's inputref
   * bitset. If the RelNdoe passed in is a Join RelNode, only return the inputs of the ith
   * input (where i is 0 or 1).
   */
  private static ImmutableBitSet getInputRefsForContext(RelNode rel,
      ImmutableBitSet currentSet, int i) {
    switch (ImpalaPlanRel.getRelNodeType(rel)) {
      case AGGREGATE:
      case SORT:
      case UNION:
        // these RelNodes use all of its inputs, so we return the range from 0..size of
        // its input.
        return ImmutableBitSet.range(
            rel.getInputs().get(0).getRowType().getFieldList().size());
      case JOIN:
        int leftSize = rel.getInputs().get(0).getRowType().getFieldList().size();
        // only return the inputs on the ith side.
        return (i == 0)
            ? ImmutableBitSet.range(leftSize)
            : ImmutableBitSet.range(rel.getRowType().getFieldList().size() - leftSize);
      case PROJECT:
        // a Project will only use inputrefs found in the project.
        return RelOptUtil.InputFinder.bits(((Project) rel).getProjects(), null);
      case FILTER:
        // a filter will use all the inputs from its parent, but will also use any
        // inputrefs found in the condition. Note, we have to look at the parent of the
        // filter because if the parent is a project, we only want to use the input fields
        // from the parent project, not all the fields passed into the filter.
        return currentSet.union(
            RelOptUtil.InputFinder.bits(((Filter) rel).getCondition()));
      default:
        throw new RuntimeException("Unknown RelNodeType: " +
            ImpalaPlanRel.getRelNodeType(rel));
    }
  }

  private static RelNode unwrapHepRelVertex(RelNode relNode) {
    return relNode instanceof HepRelVertex
        ? ((HepRelVertex)relNode).getCurrentRel()
        : relNode;
  }

  /**
   * returns true if this is a condition that can be used by a runtime filter.
   * The condition must be an equals operator with one input ref from each
   * side of the condition.
   */
  private static boolean isEqualsCondition(RexNode andCondition) {
    if (!(andCondition instanceof RexCall)) {
      return false;
    }

    RexCall rexCall = (RexCall) andCondition;
    if (rexCall.getOperator() != SqlStdOperatorTable.EQUALS) {
      return false;
    }

    if (RelOptUtil.InputFinder.bits(rexCall.getOperands().get(0)).cardinality() != 1) {
      return false;
    }

    if (RelOptUtil.InputFinder.bits(rexCall.getOperands().get(1)).cardinality() != 1) {
      return false;
    }

    return true;
  }

  /**
   * InputInfo is an abstract class that is used for two purposes:
   * 1) To obtain the top level RelNode on the left and right sides
   * 2) To calculate the index for a given bitset containing 2 bits
   *    for either the left or right side.
   * The second purpose can be a bit tricky because the lower bit
   * is not necessarily on the left side for a MultiJoin. This
   * calculation is done by a derived class.
   */
  private static abstract class InputInfo {
    public final RelNode leftSide_;
    public final RelNode rightSide_;

    public abstract Integer getLeftIndex(ImmutableBitSet bitSet);
    public abstract Integer getRightIndex(ImmutableBitSet bitSet);

    protected InputInfo(RelNode leftSide, RelNode rightSide) {
      leftSide_ = leftSide;
      rightSide_ = rightSide;
    }
  }

  /**
   * JoinInputInfo is a derived class of of InputInfo. The left and right inputs are
   * passed in directly.  The calculations of the indices are relatively straightforward.
   */
  private static class JoinInputInfo extends InputInfo {
    public JoinInputInfo(RelNode leftSide, RelNode rightSide) {
      super(leftSide, rightSide);
    }

    @Override
    public Integer getLeftIndex(ImmutableBitSet bitSet) {
      // left index is simply the lower bit.
      return bitSet.nth(0);
    }

    @Override
    public Integer getRightIndex(ImmutableBitSet bitSet) {
      // right index is the higher bit, but we only want the index relative
      // to the start of the right side. For instance, if the left side has
      // 11 fields and the higher bit is 13, we return the index value of 2,
      // since the relative index place on the right side is in the value 2
      // of the array.
      return bitSet.nth(1) - leftSide_.getRowType().getFieldList().size();
    }
  }

  /**
   * MultiJoinInputInfo handles input info information for when the RelNodes
   * come directly from a MultiJoin and the indices need to be "adjusted".
   *
   * This input info is a use case specifically designed for being called from
   * swapInputs. swapInputs will only be called when a new input is being added
   * to the join tree that is currently being created one input join at a time.
   * Given this fact, it is known that the right side MUST contain only one
   * input.  The left side may contain more than one input since it is the current
   * Join tree build in progress.
   */
  private static class MultiJoinInputInfo extends InputInfo {
    // the multijoin node being analyzed containing index information.
    public final MultiJoin multiJoin_;
    // The index range of the right child. As mentioned in the comment for the class,
    // there can be only one join input in the right side join tree, so we can track the
    // inputref range for this given input.
    public final Range<Integer> rightIndexRange_;

    public MultiJoinInputInfo(MultiJoin multiJoin,
        LoptJoinTree left, LoptJoinTree right) {
      super(left.getJoinTree(), right.getJoinTree());
      multiJoin_ = multiJoin;
      rightIndexRange_ = getRightIndexRange(right);
    }

    @Override
    public Integer getLeftIndex(ImmutableBitSet bitSet) {
      // The bitSet will contain two bits, but we do not know if the lower or higher
      // bit is on the left side.  Given that we know the range of the right side, we
      // use the bit that is not in the range of the right input. If both bits are
      // in the right side range, just return null.
      if (!rightIndexRange_.contains(bitSet.nth(0))) {
        return getLeftIndex(multiJoin_, bitSet.nth(0));
      }

      if (!rightIndexRange_.contains(bitSet.nth(1))) {
        return getLeftIndex(multiJoin_, bitSet.nth(1));
      }

      return null;
    }

    @Override
    public Integer getRightIndex(ImmutableBitSet bitSet) {
      // If the input is in the range of the nth bit, return the offset from
      // the lower index starting point. If neither bit is in range, return null.
      if (rightIndexRange_.contains(bitSet.nth(0))) {
        return bitSet.nth(0) - rightIndexRange_.lowerEndpoint();
      }
      if (rightIndexRange_.contains(bitSet.nth(1))) {
        return bitSet.nth(1) - rightIndexRange_.lowerEndpoint();
      }
      return null;
    }

    private Integer getLeftIndex(MultiJoin multiJoin, int index) {
      // There are multiple inputs on the left side.  While we know the index number,
      // we do not have access to the input containing the index. The LoptJoinTree for
      // the left side only contains the RelNode, but the RelNode doesn't necessarily
      // match the RelNode in the MultiJoin.  So we iterate through all the inputs
      // in the multijoin.  When we find the input that contains the index, we return
      // the offset from the beginning of the found input.
      // For instance, Let's say we have a MultiJoin with 3 inputs, and each input
      // contains 7 fields. And let's say the index value is 18. We iterate through
      // the inputs, and find that the value 12 is in the 3rd input.  At this point,
      // totalFieldsSoFar will be 14, our index value is 18, and we return 4.
      int totalFieldsSoFar = 0;
      for (RelNode r : multiJoin.getInputs()) {
        if (index - totalFieldsSoFar < r.getRowType().getFieldList().size()) {
          return index - totalFieldsSoFar;
        }
        totalFieldsSoFar += r.getRowType().getFieldList().size();
      }
      return null;
    }

    private Range getRightIndexRange(LoptJoinTree rightTree) {
      final List<Integer> joinOrder = new ArrayList<>();
      rightTree.getTreeOrder(joinOrder);
      Preconditions.checkState(joinOrder.size() == 1);
      int factor = joinOrder.get(0);

      int startIndex = 0;
      for (int i = 0; i < factor; ++i) {
        startIndex += multiJoin_.getInputs().get(i).getRowType().getFieldList().size();
      }
      int endIndex = startIndex + multiJoin_.getInputs().get(factor).getRowType().getFieldList().size();

      return Range.closedOpen(startIndex, endIndex);
    }
  }
}
