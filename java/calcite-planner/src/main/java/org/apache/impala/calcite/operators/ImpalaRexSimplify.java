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
package org.apache.impala.calcite.operators;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.regex.Pattern;
/**
 * ImpalaRexSimplify extends the RexSimplify class in order to 'simplify' expressions.
 * The extension of the "simplify" method catches the double and float types used in
 * a binary predicate and avoids the simplification.  This is needed because Calcite
 * will simplify various cases of NaN the wrong way.  For example, NOT(my_col >= 3)
 * should include NaN in its calculation. But this gets simplified to my_col < 3
 * which does not include NaN in its calculation.
 */
public class ImpalaRexSimplify extends RexSimplify {
  private final RexExecutor rexExecutor_;

  public ImpalaRexSimplify(RexBuilder rexBuilder, RexExecutor rexExecutor) {
    super(rexBuilder, RelOptPredicateList.EMPTY, rexExecutor);
    rexExecutor_ = rexExecutor;
  }

  public RexExecutor getRexExecutor() {
    return rexExecutor_;
  }

  @Override
  public RexNode simplify(RexNode rexNode) {
    return simplify(rexNode, false);
  }

  public RexNode simplify(RexNode rexNode, boolean isFilter) {
    if (hasApproximateTypeIssues(rexNode)) {
      return rexNode;
    }

    return isFilter
        ? super.simplifyUnknownAs(rexNode, RexUnknownAs.FALSE)
        : super.simplify(rexNode);
  }

  @Override
  public RexNode simplifyUnknownAs(RexNode rexNode, RexUnknownAs unknownAs) {
    // Override the simplify for the LIKE command (see comment for simplifyLike)
    return rexNode.getKind() == SqlKind.LIKE
        ? simplifyLike(rexNode, unknownAs)
        : super.simplifyUnknownAs(rexNode, unknownAs);
  }

  /**
   * simplifyLike is overridden because it is not working properly for Impala.
   * The following expression causes a problem because of Calcite's simplification.
   * 'a\\' like 'a\\%%';
   * The Calcite simplification code treats multiple adjacent percentage signs as
   * redundant and removes all but one. So Calcite changes this expression to
   * 'a\\' like 'a\\%';
   * However, this simplification changes the return value for Impala. Impala will
   * treat this like expression as an escaped  percentage character,
   * i.e. 'a\%'.  The percentage sign here is not treated as a wildcard and Impala
   * returns false for 'a\\' like 'a\\%'.
   * Because the simplification changes values, we cannot use the Calcite simplification
   * code. To keep the code simple for this edge case, if we see that the like clause
   * contains a percentage sign, no further simplification is done on the like
   * expression.
   */
  private RexNode simplifyLike(RexNode rexNode, RexUnknownAs unknownAs) {
    RexCall rexCall = (RexCall) rexNode;
    if (rexCall.getOperands().get(1) instanceof RexLiteral) {
      final RexLiteral literal = (RexLiteral) rexCall.getOperands().get(1);
      String likeStr = literal.getValueAs(String.class);
      Pattern pattern = Pattern.compile("%+");
      String value = pattern.matcher(likeStr).replaceAll("%");
      // This simplification was copied from Calcite's simplifyLike
      if ("%".equals(value)) {
        // "x LIKE '%'" or "x LIKE '%...'" simplifies to "x = x"
        final RexNode x = rexCall.getOperands().get(0);
        return simplifyUnknownAs(rexBuilder.makeCall(
            rexCall.getParserPosition(), SqlStdOperatorTable.EQUALS, x, x), unknownAs);
      }
      if (likeStr.contains("%")) {
        return rexNode;
      }
    }
    return super.simplifyUnknownAs(rexCall, unknownAs);
  }

  private boolean hasApproximateTypeIssues(RexNode rexNode) {
    RexHasApproximateIssuesShuttle shuttle = new RexHasApproximateIssuesShuttle();
    shuttle.apply(rexNode);
    return shuttle.hasApproximateIssues_;
  }

  private static class RexHasApproximateIssuesShuttle extends RexShuttle {
    public boolean hasApproximateIssues_ = false;

    // Sets hasApproximateIssues to true if there is a binary predicate with a double
    // or float type in it.
    @Override
    public RexNode visitCall(RexCall rexCall) {
      super.visitCall(rexCall);
      if (rexCall.getOperator() instanceof SqlBinaryOperator) {
        if (SqlTypeUtil.isApproximateNumeric(rexCall.getOperands().get(0).getType()) ||
            SqlTypeUtil.isApproximateNumeric(rexCall.getOperands().get(1).getType())) {
          hasApproximateIssues_ = true;
        }
      }
      return rexCall;
    }
  }
}
