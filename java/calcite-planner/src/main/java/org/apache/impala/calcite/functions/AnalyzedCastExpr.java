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

package org.apache.impala.calcite.functions;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import java.util.List;

/**
 * A CastExpr that is always in analyzed state
 */
public class AnalyzedCastExpr extends CastExpr {

  // True if there is a real implicit function used.
  // Unfortunately, this has been coded in a slightly confusing way. There are two types
  // of "implicit" variables here.
  // The parent CastExpr contains an "isImplicit_" variable, but this one cannot be used.
  // When the substitute() method is called from the ExprSubstitutionMap, it drops all
  // casts where the CastExpr.isImplicit_ is true. This does not work for Calcite because
  // Calcite creates the properly resolved functions before optimization time. The
  // analysis step is complete, so any cast created within Calcite should not be blindly
  // removed by the substitute() statement. This is why isImplicit_ needs to always be
  // false for the parent.
  // However, when "unwrapSlotRef" is called, the logic is different and the cast does
  // need to be removed if it is implicit. So the userDefinedCast_ variable is set up
  // to handle these situations, and the unwrapSlotRef() method examines this variable.
  // The allowsImplicitConversion() method also exists for the partition pruning, since
  // pruning works differently if there is an actual implicit cast.
  private final boolean userDefinedCast_;

  public AnalyzedCastExpr(Type targetType, List<Expr> paramList, boolean userDefinedCast) {
    super(targetType, paramList.get(0).clone(), getFormat(paramList));
    userDefinedCast_ = userDefinedCast;
  }

  public AnalyzedCastExpr(AnalyzedCastExpr other) {
    super(other);
    userDefinedCast_ = other.userDefinedCast_;
  }

  @Override
  public Expr clone() {
    return new AnalyzedCastExpr(this);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
  }

  @Override
  public SlotRef unwrapSlotRef(boolean implicitOnly) {
    if (implicitOnly && userDefinedCast_) {
      return null;
    }
    Expr unwrappedExpr = children_.get(0);
    return (unwrappedExpr instanceof SlotRef) ? (SlotRef) unwrappedExpr : null;
  }

  /**
   * isImplicit() always returns false so that the Expr.substitute() method doesn't
   * remove the cast. See comment about userDefinedCost_ for more details.
   */
  @Override
  public boolean isImplicit() {
    return false;
  }

  @Override
  public boolean allowsImplicitConversion() {
    return !userDefinedCast_;
  }

  private static String getFormat(List<Expr> paramsList) {
    return paramsList.size() == 1
        ? null
        : ((StringLiteral)paramsList.get(1)).getStringValue();
  }
}
