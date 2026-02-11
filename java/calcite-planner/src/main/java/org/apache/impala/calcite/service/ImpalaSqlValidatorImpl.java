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

import com.google.common.base.Preconditions;

import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorScope.Resolve;
import org.apache.calcite.sql.validate.SqlValidatorScope.ResolvedImpl;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.schema.ImpalaViewTable;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.common.UnsupportedFeatureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The ImpalaSqlValidatorImpl is responsible for registering column-level and
 * function-level privilege requests in the given query. The methods in the class will be
 * invoked via SqlValidatorImpl#validate(SqlNode topNode).
 */
public class ImpalaSqlValidatorImpl extends SqlValidatorImpl {
  private Analyzer analyzer_;

  private static ViewValidatorAliasHelper NOOP_HELPER = new NoopValidatorHelper();
  private ViewValidatorAliasHelper viewAliasHelper_ = NOOP_HELPER;

  private UnsupportedFeatureException potentialCauseOfError_;

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaSqlValidatorImpl.class.getName());

  public ImpalaSqlValidatorImpl(SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory,
      SqlValidator.Config config, Analyzer analyzer) {
    super(opTab, catalogReader, typeFactory, config);
    analyzer_ = analyzer;
  }

  /**
   * Override of deriveType needed for IMPALA-14429 and
   * CALCITE-7120.
   *
   * Calcite always treats numerics like '1' as an integer, whereas
   * Impala treats this as a tinyint. So the derived type is overridden
   * to produce Impala's derived type rather than Calcite's derived type.
   */
  @Override
  public RelDataType deriveType(
      SqlValidatorScope scope,
      SqlNode operand) {
    if (operand instanceof SqlNumericLiteral) {
      SqlNumericLiteral numeric = (SqlNumericLiteral) operand;
      if (numeric.isInteger()) {
        RelDataType type = ImpalaTypeConverter.getLiteralDataType(
            new BigDecimal(numeric.toValue()), null);
        setValidatedNodeType(operand, type);
        return type;
      }
    }
    return super.deriveType(scope, operand);
  }

  @Override public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
    super.validateIdentifier(id, scope);
    // It is a bit inefficient to execute scope.fullyQualify() again because this has
    // been done within super.validateIdentifier(id, scope). But since we need 'fqId'
    // to register the privilege request, we have to do this for now. We created
    // CALCITE-7152 to keep track of this.
    SqlQualified fqId = scope.fullyQualify(id);

    SqlIdentifier prefix = id.getComponent(0, 1);
    SqlNameMatcher nameMatcher = this.getCatalogReader().nameMatcher();
    ResolvedImpl resolved = new ResolvedImpl();
    SqlValidatorNamespace fromNs;
    scope.resolve(prefix.names, nameMatcher, false, resolved);
    if (resolved.count() == 1) {
      Resolve resolve = resolved.only();
      fromNs = resolve.namespace;
      SqlValidatorTable validatorTable = fromNs.getTable();

      if (validatorTable instanceof CalciteTable) {
        FeFsTable feFsTable = ((CalciteTable) validatorTable).getFeFsTable();
        this.analyzer_.registerPrivReq(
            builder -> builder.allOf(Privilege.SELECT)
                .onColumn(feFsTable.getDb().getName(),
                    feFsTable.getTableName().getTbl(), fqId.identifier.names.get(1),
                    feFsTable.getOwnerUser()).build()
        );
        return;
      }

      // A view would be an instance of RelOptTableImpl if the view was created via
      // CalciteDb.Builder#createViewTable().
      if (validatorTable instanceof RelOptTableImpl) {
        Preconditions.checkState(validatorTable.table() instanceof ImpalaViewTable);
        FeView view = ((ImpalaViewTable) validatorTable.table()).getFeView();
        this.analyzer_.registerPrivReq(
            builder -> builder.allOf(Privilege.SELECT)
                .onColumn(view.getDb().getName(),
                    view.getTableName().getTbl(), fqId.identifier.names.get(1),
                    view.getOwnerUser()).build()
        );
      }
    }
  }

  @Override public void validateWithItem(SqlWithItem withItem) {
    // Little hack.  This code is already in Calcite. But this is supported
    // by Impala. So we need to throw an Unsupported error rather than a
    // validation error.
    SqlNodeList columnList = withItem.columnList;
    if (columnList != null) {
      final RelDataType rowType = getValidatedNodeType(withItem.query);
      final int fieldCount = rowType.getFieldCount();
      if (columnList.size() != fieldCount) {
        throw new CalciteContextException("", new UnsupportedFeatureException(
            "Number of columns in with clause must match number of query columns"));
      }   
    }
    super.validateWithItem(withItem);
  }

  @Override public void validateCall(
      SqlCall call,
      SqlValidatorScope scope) {
    super.validateCall(call, scope);

    SqlOperator operator = call.getOperator();
    // It's a bit hacky to assume each SqlFunction is associated with BuiltinsDb. Ideally
    // a function could be associated with any database. IMPALA-13095 was created to keep
    // track of this.
    if (operator instanceof SqlFunction) {
      this.analyzer_.registerPrivReq(
          builder -> builder.allOf(Privilege.VIEW_METADATA)
              .onDb(BuiltinsDb.getInstance().getName(), null).build());
    }
  }

  public void startValidatingView(boolean attemptCorrection) {
    if (attemptCorrection) {
      viewAliasHelper_ = new ViewAttemptAliasCorrection(viewAliasHelper_);
    } else {
      viewAliasHelper_ = new ViewGatherAliases();
    }
  }

  public void endValidatingView() {
    viewAliasHelper_.validateFinished();
    viewAliasHelper_ = NOOP_HELPER;
  }

  public boolean foundAliasIssue() {
    return !viewAliasHelper_.getItemsWithAliasIssue().isEmpty();
  }

  @Override
  protected void validateSelect(
      SqlSelect select,
      RelDataType targetRowType) {
    viewAliasHelper_.processSelect(select);
    super.validateSelect(select, targetRowType);
    viewAliasHelper_.endProcessSelect();
  }

  @Override
  protected void validateValues(
      SqlCall node,
      RelDataType targetRowType,
      final SqlValidatorScope scope) {
    validateImpalaValues(node);
    validateImpalaValues2(node);
    super.validateValues(node, targetRowType, scope);
  }

  @Override
  public SqlNode expandSelectExpr(SqlNode expr,
      SelectScope scope, SqlSelect select, Map<String, SqlNode> expansions) {
    expr = viewAliasHelper_.processSelectItem(expr);
    return super.expandSelectExpr(expr, scope, select, expansions);
  }

  private abstract static class ViewValidatorAliasHelper {
    protected List<SqlNode> topLevelSqlNodes_;

    protected List<SqlNode> secondLevelSqlNodes_;

    protected int selectStackCounter_ = 0;

    public void processWith() {
      //TODO:
    }

    public void processSelect(SqlSelect select) {
      selectStackCounter_++;
      processSelectImpl(select);
    }

    public void endProcessSelect() {
      selectStackCounter_--;
    }

    public void validateFinished() {
      Preconditions.checkState(selectStackCounter_ == 0);
    }

    public Set<Integer> getItemsWithAliasIssue() {
      Set<Integer> itemsWithAliasIssue = new HashSet<>();
      if (topLevelSqlNodes_ == null || secondLevelSqlNodes_ == null) {
        return itemsWithAliasIssue;
      }
      if (topLevelSqlNodes_.size() != secondLevelSqlNodes_.size()) {
        return itemsWithAliasIssue;
      }
      // XXX: maybe use streaming?
      for (int i = 0; i < topLevelSqlNodes_.size(); ++i) {
        if (hasAliasIssue(topLevelSqlNodes_.get(i), secondLevelSqlNodes_.get(i))) {
          itemsWithAliasIssue.add(i);
        }
      }
      return itemsWithAliasIssue;
    }

    protected boolean hasAliasIssue(SqlNode topLevelItem, SqlNode secondLevelItem) {
      //XXX: should be well structured since it's an Impala view
      SqlBasicCall call = (SqlBasicCall) topLevelItem;
      SqlIdentifier topLevelIdentifier = (SqlIdentifier) call.getOperandList().get(0);
      return !topLevelIdentifier.names.get(1).equals(
          SqlValidatorUtil.alias(secondLevelItem));
    }

    abstract public void processSelectImpl(SqlSelect select);
    //XXX: make this abstract?  make it similar to processSelectImpl?
    public SqlNode processSelectItem(SqlNode expr) {
      return expr;
    }
  }

  private static class ViewGatherAliases extends ViewValidatorAliasHelper {
    @Override
    public void processSelectImpl(SqlSelect select) {
      if (selectStackCounter_ == 1) {
        topLevelSqlNodes_ = select.getSelectList();
      }
      if (selectStackCounter_ == 2) {
        secondLevelSqlNodes_ = select.getSelectList();
      }
    }
  }

  private static class ViewAttemptAliasCorrection extends ViewValidatorAliasHelper {
    private Set<Integer> itemsWithAliasIssue_;
    
    private int secondLevelSelectItemCounter_ = 0;

    private int aliasCounter_ = 0;

    public ViewAttemptAliasCorrection(ViewValidatorAliasHelper helper) {
      // XXX: processed twice?
      this.itemsWithAliasIssue_ = helper.getItemsWithAliasIssue();
    }

    public void processWith() {
      //TODO:
    }

    @Override
    public void processSelectImpl(SqlSelect select) {
      if (selectStackCounter_ != 1) {
        return;
      }
      List<SqlNode> selectList = new ArrayList<>();
      for (int i = 0; i < select.getSelectList().size(); ++i) {
        SqlBasicCall call = (SqlBasicCall) select.getSelectList().get(i);
        if (!itemsWithAliasIssue_.contains(i)) {
          selectList.add(call);
          continue;
        }
        SqlIdentifier identifier = (SqlIdentifier) call.getOperandList().get(0);
        SqlIdentifier newIdentifier = identifier.setName(1, "EXPR$" + i);
        // XXX:add to previous line
        SqlNode asNode = 
            SqlStdOperatorTable.AS.createCall(
                newIdentifier.getParserPosition(),
                newIdentifier,
                call.getOperandList().get(1));
        selectList.add(asNode);
      }
      select.setSelectList(new SqlNodeList(selectList, SqlParserPos.ZERO));
    }

    public SqlNode processSelectItem(SqlNode sqlNode) {
      SqlNode returnNode = sqlNode;
      if (selectStackCounter_ != 2) {
        return returnNode;
      }
      
      if (itemsWithAliasIssue_.contains(secondLevelSelectItemCounter_)) {
        String alias = "EXPR$" + secondLevelSelectItemCounter_;
        returnNode = SqlStdOperatorTable.AS.createCall(
            sqlNode.getParserPosition(),
            sqlNode,
            new SqlIdentifier(alias, SqlParserPos.ZERO));
      }
      
      secondLevelSelectItemCounter_++;
      return returnNode;
    }
  }

  private static class NoopValidatorHelper extends ViewValidatorAliasHelper {
    @Override
    public void processSelectImpl(SqlSelect select) {}
  }

  private void validateImpalaValues(SqlNode sqlNode) {
    SqlBasicCall row = (SqlBasicCall) sqlNode;
    if (row.operandCount() > 1) {
      return;
    }

    if (!(row.operand(0) instanceof SqlBasicCall)) {
      return;
    }
    SqlBasicCall topLevelRow = (SqlBasicCall) row.operand(0);
    if (!(topLevelRow.operand(0) instanceof SqlBasicCall)) {
      return;
    }
    int numParams = ((SqlBasicCall)topLevelRow.operand(0)).operandCount();
    for (int i = 1; i < topLevelRow.operandCount(); ++i) {
      if (!(topLevelRow.operand(i) instanceof SqlBasicCall)) {
        return;
      }
      SqlBasicCall subrow = (SqlBasicCall) topLevelRow.operand(i);
      if (subrow.getKind() != SqlKind.ROW || subrow.operandCount() != numParams) {
        return;
      }
    }
    potentialCauseOfError_ = new UnsupportedFeatureException("Values clause not " +
        "supported with double parentheses.");
  }

  private void validateImpalaValues2(SqlNode sqlNode) {
    SqlBasicCall row = (SqlBasicCall) sqlNode;
    if (!(row.operand(0) instanceof SqlBasicCall)) {
      return;
    }
    SqlBasicCall firstRow = (SqlBasicCall) row.operand(0);
    if (firstRow.operandCount() > 1 || !(firstRow instanceof SqlBasicCall)) {
      return;
    }
    if (firstRow instanceof SqlBasicCall &&
        ((SqlBasicCall) firstRow.operand(0)).getKind() == SqlKind.AS) {
      potentialCauseOfError_ = new UnsupportedFeatureException("Error handling " +
          "values clause in Calcite with only one column that has an alias " +
          "(IMPALA-XXXXX)");
    }
  }

  public UnsupportedFeatureException getPossibleValidationException() {
    return potentialCauseOfError_;
  }
}
