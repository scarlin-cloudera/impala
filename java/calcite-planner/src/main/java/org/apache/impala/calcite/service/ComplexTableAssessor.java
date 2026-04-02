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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.AnalysisDriver;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ParsedStatement;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.schema.ImpalaCalciteCatalogReader;
import org.apache.impala.calcite.type.ImpalaTypeCoercionFactory;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.calcite.validate.ImpalaConformance;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ParseException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.thrift.TQueryCtx;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComplexTableAssessor {
  protected static final Logger LOG = LoggerFactory.getLogger(ComplexTableAssessor.class.getName());
  

  public ComplexTableAssessor(SqlNode parsedSqlNode, TQueryCtx queryCtx, AnalysisContext ctx)
      throws UnsupportedFeatureException {
    ComplexTableAssessorShuttle shuttle =
        new ComplexTableAssessorShuttle(queryCtx.session.database, ctx.getCatalog());
    parsedSqlNode.accept(shuttle);
    if (shuttle.complexTable_ != null) {
      LOG.info("SJC: COMPLEX TABLE ASSESSOR WORKED");
      throw new UnsupportedFeatureException("Complex table " + shuttle.complexTable_ +
          " not supported.");
    }

  }

  private static class ComplexTableAssessorShuttle  extends SqlShuttle {
    private final String currentDb_;
    private final FeCatalog catalog_;
    public String complexTable_ = null;

    public ComplexTableAssessorShuttle(String currentDb, FeCatalog catalog) {
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

      SqlNode fromNode = select.getFrom();

      getChangedFromClause(fromNode);

      return call;
    }

    private SqlNode getChangedFromClause(SqlNode fromNode) {
      if (fromNode instanceof SqlJoin) {
        getChangedFromClause(((SqlJoin)fromNode).getLeft());
        getChangedFromClause(((SqlJoin)fromNode).getRight());
      }

      if (fromNode instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) fromNode;
        if (basicCall.getOperator().getKind().equals(SqlKind.AS)) {
          getChangedFromClause(basicCall.operand(0));
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
      return fromNode;
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
  }
}
