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

package org.apache.impala.calcite.service;

import java.util.List;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TQueryCtx;

import com.google.common.base.Preconditions;

public class ComplexTableDetector extends SqlShuttle {

  private final String currentDb_;
  private final FeCatalog catalog_;
  public String complexTable_ = null;

  public ComplexTableDetector(String currentDb, FeCatalog catalog) {
    currentDb_ = currentDb;
    catalog_ = catalog;
  }

  @Override
  public SqlNode visit(final SqlCall call) {
    super.visit(call);
    if (!(call instanceof SqlSelect)) {
      return call;
    }
    SqlSelect select = (SqlSelect) call;
    checkFromNode(select.getFrom());

    return call;
  }

  private void checkFromNode(SqlNode fromNode) {
    if (fromNode instanceof SqlJoin) {
      checkFromNode(((SqlJoin)fromNode).getLeft());
      checkFromNode(((SqlJoin)fromNode).getRight());
    }

    if (fromNode instanceof SqlBasicCall) {
      SqlBasicCall basicCall = (SqlBasicCall) fromNode;
      if (basicCall.getOperator().getKind().equals(SqlKind.AS)) {
        checkFromNode(basicCall.operand(0));
      }
    }

    if (fromNode instanceof SqlIdentifier) {
      SqlIdentifier fromIdentifier = (SqlIdentifier)fromNode;
      List<String> names = fromIdentifier.names;
      String table = names.get(names.size() - 1);
      if (names.size() > 1) {
        String potentialTable = names.get(names.size() - 2).toLowerCase();
        SqlIdentifier tableNameIdentifier = fromIdentifier;
        if (tableAndArrayColumnExists(names)) {
          complexTable_ = table;
        }
      }
    }
  }

  private boolean tableAndArrayColumnExists(List<String> tableStringList) {
    Preconditions.checkState(tableStringList.size() >= 2);
    int arrayOffset = 0;
    String dbName = tableStringList.size() == 2
        ? currentDb_
        : tableStringList.get(arrayOffset++).toLowerCase();
    FeDb db = catalog_.getDb(dbName);
    if (db == null) {
      return false;
    }
    FeTable feTable = db.getTable(tableStringList.get(arrayOffset++).toLowerCase());
    if (feTable == null) {
      return false;
    }
    return feTable.getColumnNames().contains(
        tableStringList.get(arrayOffset).toLowerCase());
  }

  public static void checkForComplexTables(SqlNode parsedSqlNode,
      TQueryCtx queryCtx, AnalysisContext ctx) throws UnsupportedFeatureException {
    ComplexTableDetector detector =
        new ComplexTableDetector(queryCtx.session.database, ctx.getCatalog());
    parsedSqlNode.accept(detector);
    if (detector.complexTable_ != null) {
      throw new UnsupportedFeatureException("Complex table " + detector.complexTable_ +
          " not supported.");
    }
  }
}
