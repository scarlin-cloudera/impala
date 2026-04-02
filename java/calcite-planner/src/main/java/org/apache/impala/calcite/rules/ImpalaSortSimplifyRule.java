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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.impala.calcite.operators.ImpalaRexSimplify;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * ImpalaSortSimplifyRule is a rule that simplifies the offset and limit clauses. Impala
 * allows constant expressions within these clauses (e.g. "limit 1 + 1", and this rule
 * will simplify this expression to a literal (e.g. "limit 2").
 */
public class ImpalaSortSimplifyRule extends RelOptRule {
  private final ImpalaRexSimplify simplifier_;

  public ImpalaSortSimplifyRule(ImpalaRexSimplify simplifier) {
    super(operand(Sort.class, none()));
    this.simplifier_ = simplifier;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Sort sort = call.rel(0);
    // the common case, most times a limit is not an expression.
    if (sort.fetch instanceof RexLiteral && sort.offset instanceof RexLiteral) {
      return;
    }
    RelOptCluster cluster = sort.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexExecutor executor = simplifier_.getRexExecutor();

    boolean changed = false;
    RexNode newFetch = sort.fetch;
    RexNode newOffset = sort.offset;
    List<RexNode> reducedExprs = new ArrayList<>();
    boolean somethingChanged = false;
    if (sort.fetch != null) {
      executor.reduce(rexBuilder, ImmutableList.of(sort.fetch), reducedExprs);
      newFetch = reducedExprs.get(0);
      if (!sort.fetch.equals(newFetch)) {
        somethingChanged = true;
      }
    }
    reducedExprs.clear();
    if (sort.offset != null) {
      executor.reduce(rexBuilder, ImmutableList.of(sort.offset), reducedExprs);
      newOffset = reducedExprs.get(0);
      if (!sort.offset.equals(newOffset)) {
        somethingChanged = true;
      }
    }
    if (!somethingChanged) {
      return;
    }

    Sort newSort = sort.copy(sort.getTraitSet(), sort.getInput(0), sort.getCollation(),
        newOffset, newFetch);
    call.transformTo(newSort);
  }
}
