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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.iceberg.Table;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.Type;
  
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.immutables.value.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Enclosing
public class CountStarProjectIcebergRule
    extends RelRule<CountStarProjectIcebergRule.Config>
    implements TransformationRule {
 protected static final Logger LOG = LoggerFactory.getLogger(CountStarProjectIcebergRule.class.getName());

 public static final CountStarProjectIcebergRule INSTANCE =
      CountStarProjectIcebergRule.Config.DEFAULT.toRule();

  protected CountStarProjectIcebergRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate agg = call.rel(0);
    final Project proj  = call.rel(1);
    RelOptCluster cluster = agg.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();

/*
    if (hasCountStarOnly(agg)) {
      LogicalAggregate newAgg = LogicalAggregate.create(proj.getInputs().get(0), agg.getHints(), agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
      call.transformTo(newAgg);
      return;
    }
    */

    RelNode projInput = proj.getInputs().get(0);
    if (!(projInput instanceof TableScan)) {
      return;
    }
    TableScan ts = (TableScan) projInput;

    CalciteTable table = (CalciteTable) ts.getTable();
    FeIcebergTable iceTable = (FeIcebergTable) table.getFeFsTable();

    if (!table.isIcebergTable()) {
      return;
    }

    //XXX: move inside test
    //XXX: need to implement if any count(*) is in there
    // XXX: move up to top
    boolean hasACountStar = hasACountStar(agg);
    if (!hasACountStar(agg)) {
      return;
    }

    Long count = 0L;
    try {
      if (FeIcebergTable.Utils.hasDeleteFiles(iceTable, null)) { 
        return;
      }
      count = FeIcebergTable.Utils.getRecordCountV1(iceTable.getIcebergApiTable(), null);
    } catch (Exception e) {
      //XXX: need something better
      throw new RuntimeException(e);
    }
    RexLiteral countLiteral = rexBuilder.makeLiteral(count, ImpalaTypeConverter.getRelDataType(Type.BIGINT));


    LOG.info("SJC: MADE IT INTO ONMATCH WITH PROJECT");

    List<AggregateCall> nonCountStarAggregates = new ArrayList<>();
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (!(aggCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
          aggCall.getArgList().size() == 0)) {
        nonCountStarAggregates.add(aggCall);
      }
    }
    LogicalAggregate newAgg = LogicalAggregate.create(agg.getInputs().get(0), agg.getHints(), agg.getGroupSet(), agg.getGroupSets(), nonCountStarAggregates);
    List<RexNode> projects = new ArrayList<>();
    int i = 0;
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (!(aggCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
          aggCall.getArgList().size() == 0)) {
        projects.add(rexBuilder.makeInputRef(aggCall.getType(), i++));
      } else {
        projects.add(countLiteral);
      }
    }

    LogicalProject project = LogicalProject.create(newAgg, new ArrayList<>(), projects, agg.getRowType());
    call.transformTo(project);
  }

  private boolean hasCountStarOnly(Aggregate agg) {
    if (agg.getGroupSet().size() > 0) {
      return false;
    }
    if (agg.getGroupSets().size() > 0) {
      return false;
    }

    if (agg.getAggCallList().size() == 0) {
      return false;
    }
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (!aggCall.getAggregation().getKind().equals(SqlKind.COUNT)) {
        return false;
      }
      if (aggCall.getArgList().size() > 0) {
        return false;
      }
    }
    return true;
  }
  public static boolean test(Project proj) {
    return true;
    /*
    CalciteTable table = (CalciteTable) scan.getTable();
    LOG.info("SJC: IN TEST, ICEBERG TABLE IS " + table.getName() + ", AND ISICEBERG IS " + table.isIcebergTable());
    return table.isIcebergTable();
    */
    /*
    // We can only push filters into a FilterableTable or
    // ProjectableFilterableTable.
    final RelOptTable table = scan.getTable();
    return table.unwrap(FilterableTable.class) != null
        || table.unwrap(ProjectableFilterableTable.class) != null;
        */
  }

  private boolean hasACountStar(Aggregate agg) {
    if (agg.getGroupSet().size() > 0) {
      return false;
    }
    if (agg.getGroupSets().size() > 0) {
      return false;
    }

    if (agg.getAggCallList().size() == 0) {
      return false;
    }
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (aggCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
          aggCall.getArgList().size() == 0) {
        return true;
      }
    }
    return false;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCountStarProjectIcebergRule.Config.of("dummy")
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class).oneInput(b1 ->
                b1.operand(Project.class).anyInputs()));

    @Value.Parameter
    //XXX: need to figure out how to generate of() without this
    public abstract String dummy();

    @Override default CountStarProjectIcebergRule toRule() {
      return new CountStarProjectIcebergRule(this);
    }
  }
}

