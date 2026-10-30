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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ImpalaRelBuilder extends RelBuilder {
  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaRelBuilder.class.getName());
  public ImpalaRelBuilder(Context context, RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  /**
   * Override the isDistinctFrom. The Calcite version creates an expression
   * with the IS_DISTINCT_FROM conjunct that assumes the caller does not
   * support the IS_DISTINCT_FROM operator directly. This more complicated
   * version causes q87 in tpcds to run extremely slow. Because Impala does
   * implement the IS_DISTINCT_FROM operator directly, we override the method
   * here and return the simpler expression.
   */
  @Override
  public RexNode isDistinctFrom(RexNode operand0, RexNode operand1) {
    if (operand0.getType().isStruct()) {
      LOG.info("SJC: IN IS DISTINCT");
      return super.isDistinctFrom(operand0, operand1);
    }
    return getRexBuilder().makeCall(SqlStdOperatorTable.IS_DISTINCT_FROM, operand0,
        operand1);
  }

  /**
   * Override the isNotDistinctFrom. See explanation for isDistinctFrom.
   */
  @Override
  public RexNode isNotDistinctFrom(RexNode operand0, RexNode operand1) {
    if (operand0.getType().isStruct()) {
      LOG.info("SJC: IN NOT DISTINCT");
      return super.isNotDistinctFrom(operand0, operand1);
    }
    return getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, operand0,
        operand1);
  }
}
