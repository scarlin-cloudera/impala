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


import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.impala.analysis.FunctionCallExpr;

import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ImpalaSortRemoveConstantKeysRule is a copy of the SortRemoveConstantKeysRule but
 * with modifications for Impala. Specifically, non-deterministic functions are not
 * constant keys, e.g. "random"
 */
@Value.Enclosing
public class ImpalaSortRemoveConstantKeysRule extends SortRemoveConstantKeysRule {

  public ImpalaSortRemoveConstantKeysRule(SortRemoveConstantKeysRule.Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelNode input = sort.getInput();

    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    if (RelOptPredicateList.isEmpty(predicates)) {
      return;
    }

    final RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
    final List<RelFieldCollation> collationsList =
        sort.getCollation().getFieldCollations().stream()
            .filter(fc -> !containsConstantKeyOrNonConstantFunction(
                predicates, input, fc))
            .collect(Collectors.toList());

    if (collationsList.size() == sort.collation.getFieldCollations().size()) {
      return;
    }

    // No active collations. Remove the sort completely
    if (collationsList.isEmpty() && sort.offset == null && sort.fetch == null) {
      call.transformTo(input);
      call.getPlanner().prune(sort);
      return;
    }

    final Sort result =
        sort.copy(sort.getTraitSet(), input, RelCollations.of(collationsList));
    call.transformTo(result);
    call.getPlanner().prune(sort);
  }

  private static boolean containsConstantKeyOrNonConstantFunction(
      RelOptPredicateList predicates, RelNode input, RelFieldCollation fc) {

    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    RexInputRef inputRef = rexBuilder.makeInputRef(input, fc.getFieldIndex());
    if (!predicates.constantMap.containsKey(inputRef)) {
      return false; 
    }

    RexNode node = predicates.constantMap.get(inputRef);
    if (!(node instanceof RexCall)) {
      return true;
    }
    RexCall call = (RexCall) node;
    // Need to include a couple more from FunctionCallExpr
    String fnName = call.getOperator().getName().toLowerCase();
    if (FunctionCallExpr.NON_DETERMINISTIC_FNS.contains(fnName)) {
      return false;
    }
    return true;
  }
}
