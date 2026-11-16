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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

/**
 * Planner rule that puts the cheaper input of an outer join on the right.
 *
 * <p>The rule matches a {@link Join} whose type is {@link JoinRelType#LEFT},
 * {@link JoinRelType#RIGHT}, or {@link JoinRelType#FULL}. It compares the
 * cumulative cost of the two inputs. When the left input is strictly cheaper,
 * the inputs are exchanged with {@link JoinCommuteRule#swap}. That swap also
 * converts the join type:
 *
 * <ul>
 * <li>a left join becomes a right join
 * <li>a right join becomes a left join
 * <li>a full join stays a full join
 * </ul>
 *
 * <p>{@code JoinCommuteRule.swap} returns a project over the swapped join so
 * the output columns stay in their original order. Equal costs are left
 * unchanged, so the same join is not swapped again.
 */
@Value.Enclosing
public class ImpalaJoinCostOrderRule
    extends RelRule<ImpalaJoinCostOrderRule.Config>
    implements TransformationRule {

  protected ImpalaJoinCostOrderRule(Config config) {
    super(config);
  }

  /** Left, right, and full joins that {@link JoinCommuteRule#swap} can commute. */
  private static boolean isReorderableOuterJoin(Join join) {
    final JoinRelType joinType = join.getJoinType();
    switch (joinType) {
      case LEFT:
      case RIGHT:
      case FULL:
        break;
      default:
        return false;
    }
    // Swap does not rewrite system fields, and a join of a node with itself
    // cannot be commuted.
    return join.getSystemFieldList().isEmpty()
        && join.getLeft().getId() != join.getRight().getId();
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptCost leftCost = cumulativeCost(join.getLeft(), mq);
    final RelOptCost rightCost = cumulativeCost(join.getRight(), mq);
    // A missing cost cannot be ordered. A tie must not swap, or the rule
    // would exchange the same inputs forever.
    if (leftCost == null || rightCost == null || !leftCost.isLt(rightCost)) {
      return;
    }

    // swapOuterJoins converts LEFT <-> RIGHT and leaves FULL as FULL.
    final RelNode swapped = JoinCommuteRule.swap(join, true, call.builder());
    if (swapped == null) {
      return;
    }
    call.transformTo(swapped);
  }

  /**
   * Cumulative cost of {@code input}. When the Impala metadata context is
   * present, cost the whole input row rather than whatever parent columns
   * happen to be recorded on the context.
   */
  private static RelOptCost cumulativeCost(RelNode input, RelMetadataQuery mq) {
    Context context = input.getCluster().getPlanner().getContext();
    ImpalaMQContext mqContext = context == null
        ? null
        : context.unwrap(ImpalaMQContext.class);
    if (mqContext == null) {
      return mq.getCumulativeCost(input);
    }
    ImmutableBitSet savedInputRefs = mqContext.getInputRefs();
    try {
      mqContext.setInputRefs(
          ImmutableBitSet.range(input.getRowType().getFieldCount()));
      return mq.getCumulativeCost(input);
    } finally {
      mqContext.setInputRefs(savedInputRefs);
    }
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableImpalaJoinCostOrderRule.Config.builder()
        .operandSupplier(b ->
            b.operand(Join.class)
                .predicate(ImpalaJoinCostOrderRule::isReorderableOuterJoin)
                .anyInputs())
        .build();

    @Override default ImpalaJoinCostOrderRule toRule() {
      return new ImpalaJoinCostOrderRule(this);
    }
  }
}
