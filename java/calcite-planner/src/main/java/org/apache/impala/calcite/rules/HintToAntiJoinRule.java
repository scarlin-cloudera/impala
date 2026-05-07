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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * ImpalaFilterSimplifyRule calls the given ImpalaRexSimplify.simplify()
 * method (derived from Calcite's RexSimplify) for the filter condition.
 * It also calls ImpalaRexExecutor.reduce() which does constant folding.
 */
public class HintToAntiJoinRule extends RelOptRule {
  protected static final Logger LOG = LoggerFactory.getLogger(HintToAntiJoinRule.class.getName());

  public HintToAntiJoinRule() {
    super(operand(Join.class, none()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    LOG.info("SJC: IN ANTI JOIN RULE");

    boolean hasAntiJoin = false;
    LOG.info("SJC: NUM HINTS IS " + join.getHints().size());
    if (join.getHints().stream().anyMatch(r -> r.hintName.toLowerCase().equals("anti"))) {
      LOG.info("SJC: HAS ANTI JOIN");
      hasAntiJoin = true;
    }

    if (hasAntiJoin) {
      Join newJoin = join.copy(join.getTraitSet(), join.getCondition(), join.getInputs().get(0),
          join.getInputs().get(1), JoinRelType.ANTI, join.isSemiJoinDone());
      LOG.info("SJC: CHANGING TYPE");
      call.transformTo(newJoin);
    }
  }
}
