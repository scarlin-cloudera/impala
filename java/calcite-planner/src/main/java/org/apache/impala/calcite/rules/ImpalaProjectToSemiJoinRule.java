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

package org.apache.impala.calcite.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.SemiJoinRule.ProjectToSemiJoinRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Variant of Calcite's ProjectToSemiJoinRule that keeps a selective DISTINCT.
 *
 * ProjectToSemiJoinRule turns Project(InnerJoin(left, Aggregate(input))), where the
 * Project only uses columns of 'left' and the join keys are exactly the group keys of
 * the Aggregate, into Project(SemiJoin(left, input)): the semi-join does not need the
 * uniqueness that the Aggregate provides, so Calcite drops it. When the Aggregate
 * reduces its input a lot this is a bad trade for Impala: the whole un-aggregated
 * input is shuffled and built into the hash table, whereas with the Aggregate Impala
 * runs a streaming pre-aggregation before the exchange (TPC-DS q14: 43M rows shuffled
 * instead of ~90K, 15 times).
 *
 * This rule produces Project(SemiJoin(left, Aggregate(input))) instead when the
 * Aggregate is a plain DISTINCT whose estimated input row count is at least
 * MIN_AGG_INPUT_ROWS and more than MIN_AGG_REDUCTION times its estimated output row
 * count. In all other cases it behaves exactly like ProjectToSemiJoinRule. The
 * minimum input size avoids adding aggregation and exchange stages for small inputs
 * (e.g. a filtered dimension table), whose shuffle or broadcast is cheap anyway.
 *
 * Correctness: the input row type of the join is the same in both forms (the right
 * input is still the Aggregate), so the original join condition is reused verbatim.
 * Because the join is an equi-join whose right keys are exactly the group keys, every
 * left row matches at most one Aggregate row, so the inner join followed by a Project
 * of left columns only returns the same rows as the semi-join.
 */
public class ImpalaProjectToSemiJoinRule extends ProjectToSemiJoinRule {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaProjectToSemiJoinRule.class.getName());

  // Keep the Aggregate under the semi-join if it is estimated to reduce the number of
  // rows by more than this factor ...
  public static final double MIN_AGG_REDUCTION =
      doubleProperty("impala.exp.calcite_dag.semijoin_min_reduction", 10);
  // ... and its input is estimated to have at least this many rows.
  public static final double MIN_AGG_INPUT_ROWS =
      doubleProperty("impala.exp.calcite_dag.semijoin_min_input_rows", 1000000);

  // ImpalaCoreRules creates an instance of this rule even when the experiment is off,
  // so a malformed property value must not fail class initialization.
  private static double doubleProperty(String name, double defaultValue) {
    try {
      String value = System.getProperty(name);
      return value == null ? defaultValue : Double.parseDouble(value.trim());
    } catch (RuntimeException e) {
      return defaultValue;
    }
  }

  public ImpalaProjectToSemiJoinRule(ProjectToSemiJoinRuleConfig config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Join join = call.rel(1);
    final RelNode left = call.rel(2);
    final Aggregate aggregate = call.rel(3);
    final RelMetadataQuery mq = call.getMetadataQuery();
    if (join.getJoinType() != JoinRelType.INNER || !isSelectiveDistinct(aggregate, mq)) {
      super.onMatch(call);
      return;
    }

    // Same preconditions as SemiJoinRule.perform() for an INNER join with a Project
    // on top.
    ImmutableBitSet usedFields = mq.getInputFieldsUsed(project);
    if (usedFields.isEmpty()) return;
    ImmutableBitSet rightFields = ImmutableBitSet.range(
        left.getRowType().getFieldCount(), join.getRowType().getFieldCount());
    if (usedFields.intersects(rightFields)) return;
    JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(ImmutableBitSet.range(aggregate.getGroupCount()))) {
      return;
    }
    if (!joinInfo.isEqui()) return;

    RelBuilder relBuilder = call.builder();
    relBuilder.push(left);
    relBuilder.push(aggregate);
    relBuilder.semiJoin(join.getCondition()).hints(join.getHints());
    relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
    LOG.trace("Keeping selective aggregate under semi-join");
    call.transformTo(relBuilder.build());
  }

  /**
   * Returns true if 'aggregate' is a plain DISTINCT (single grouping set, no aggregate
   * functions) with a large input that it is estimated to reduce by more than
   * MIN_AGG_REDUCTION.
   */
  private static boolean isSelectiveDistinct(Aggregate aggregate, RelMetadataQuery mq) {
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) return false;
    if (!aggregate.getAggCallList().isEmpty()) return false;
    if (aggregate.getGroupCount() == 0) return false;
    Double inputRows = mq.getRowCount(aggregate.getInput());
    Double outputRows = mq.getRowCount(aggregate);
    if (inputRows == null || outputRows == null) return false;
    return inputRows >= MIN_AGG_INPUT_ROWS
        && inputRows > MIN_AGG_REDUCTION * Math.max(outputRows, 1.0);
  }
}
