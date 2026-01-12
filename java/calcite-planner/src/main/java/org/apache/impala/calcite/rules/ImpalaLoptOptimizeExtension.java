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
 * ImpalaLoptOptimizeExtension contains static methods that are needed for extending
 * Calcite has been copy/pasted into Impala with some minimal changes to call the
 * static methods in this class.
 *
 * The getCumulativeCost() method is passed into the LoptOptimizeJoinRule as a cost
 * function to be used which has modifications from the Calcite algorithm to handle
 * only used inputRefs within the TableScan.
  *
 * A TODO: The CumulativeCost handler for RelMetaDataQuery is not used. The
 * RelMetadataQuery caches costs for RelNodes. While this is great for performance,
 * it cannot be used with the current code because of the TableScan change.
 * In this current iteration, we don't necessarily know if a runtime filter will
 * be created. However, we make a decent guess with the given join to see if the
 * creation will be beneficial. A TODO here is to combine the physical planner
 * runtime filter code with this runtime filter guesstimate.
 *
 * There is a performance hit with the current code that costs are not cached. This
 * probably can be improved upon, but based on tpcds testing on a 30TB system, it
 * did not have a huge impact.
 *
 * The swapInputs() method in this class replaces the swapInputs() method in the
 * ImpalaLoptOptimizationJoinRule class. This swapInputs() method calculates whether
 * the swapping should be done based on the Impala CostModel as opposed to just looking at
 * row counts and row widths of all the inputs, some of which are calculated differently
 * (or not used) in the Impala physical model, resulting in slightly different
 * calculations.
 */
public class ImpalaLoptOptimizeExtension {

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

    RelOptCost rightSideCost =
        getCumulativeCost(unwrapHepRelVertex(rightSide), mq);
    RelOptCost leftSideCost =
        getCumulativeCost(unwrapHepRelVertex(leftSide), mq);

    // Calculate the cumulative cost for the inputs underneath the join when there is no
    // swapping.
    RelOptCost totalPreJoinCost =
        leftSideCost.plus(rightSideCost);

    // Add in the noncumulative cost at the join node to the cumulative input cost.
    RelOptCost totalJoinNonSwapCost = totalPreJoinCost.plus(
        ImpalaRelMdNonCumulativeCost.getJoinCost(leftSide, rightSide, mq));

    // Add in the noncumulative cost at the join node to the cumulative input cost.
    RelOptCost totalJoinSwapCost = totalPreJoinCost.plus(
        ImpalaRelMdNonCumulativeCost.getJoinCost(rightSide, leftSide, mq));

    // Compare the costs of swapping versus non-swapping.
    return totalJoinSwapCost.isLt(totalJoinNonSwapCost);
  }

  public static RelOptCost getCumulativeCost(RelOptRuleCall call, RelNode relNode) {
    return getCumulativeCost(relNode, call.getMetadataQuery());
  }
  /**
   * getCumulativeCost gets the cumulative cost of the given RelNode. This is used in
   * lieu of the Calcite version because the Calcite version caches the costs. With the
   * current code, runtime filter information can change at the table scan level in
   * different cost calculations, so we need to avoid this until this can be taken into
   * account.
   */
  public static RelOptCost getCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    ImpalaMQContext mqContext =
        rel.getCluster().getPlanner().getContext().unwrap(ImpalaMQContext.class);
    mqContext.setInputRefs((rel.getInputs().size() == 0)
        ? ImmutableBitSet.range(rel.getRowType().getFieldList().size())
        : ImmutableBitSet.of());
    RelOptCost cost = getCumulativeCostInternal(rel, mq);
    mqContext.clear();
    return cost;
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

    ImpalaRelMdNonCumulativeCost noncumulativeCostHandler =
        new ImpalaRelMdNonCumulativeCost();
    // calculate cost for this RelNode.
    RelOptCost cost = noncumulativeCostHandler.getNonCumulativeCost(rel, mq);
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

  /**
   * Return the bitset of the inputrefs used for the given RelNode and its parent's inputref
   * bitset. If the RelNdoe passed in is a Join RelNode, only return the inputs of the ith
   * input (where i is 0 or 1).
   */
  private static ImmutableBitSet getInputRefsForContext(RelNode rel, ImmutableBitSet currentSet, int i) {
    switch (ImpalaPlanRel.getRelNodeType(rel)) {
      case AGGREGATE:
      case SORT:
      case UNION:
        // these RelNodes use all of its inputs, so we return the range from 0..size of its
        // input.
        return ImmutableBitSet.range(rel.getInputs().get(0).getRowType().getFieldList().size());
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
        // a filter will use all the inputs from its parent, but will also use any inputrefs
        // found in the condition. Note, we have to look at the parent of the filter because
        // if the parent is a project, we only want to use the input fields from the parent
        // project, not all the fields passed into the filter.
        return currentSet.union(RelOptUtil.InputFinder.bits(((Filter) rel).getCondition()));
      default:
        throw new RuntimeException("Unknown RelNodeType: " + ImpalaPlanRel.getRelNodeType(rel));
    }
  }

  private static RelNode unwrapHepRelVertex(RelNode relNode) {
    return relNode instanceof HepRelVertex
        ? ((HepRelVertex)relNode).getCurrentRel()
        : relNode;
  }
}
