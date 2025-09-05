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
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

/**
 * A CastExpr that is always in analyzed state
 */
public class AnalyzedCastExpr extends CastExpr {

  private final boolean isImplicit_;

  public AnalyzedCastExpr(Type targetType, Expr e, boolean isImplicit) {
    super(targetType, e.clone());
    isImplicit_ = isImplicit;
  }

  public AnalyzedCastExpr(AnalyzedCastExpr other) {
    super(other);
    isImplicit_ = other.isImplicit_;
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
    if (implicitOnly && !isImplicit_) {
      return null;
    }
    Expr unwrappedExpr = children_.get(0);
    return (unwrappedExpr instanceof SlotRef) ? (SlotRef) unwrappedExpr : null;
  }

  /**
   * XXX:
   */
  @Override
  public boolean isImplicit() {
    return false;
  }
}
