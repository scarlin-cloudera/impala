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
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.iceberg.Table;
import org.apache.impala.calcite.operators.ImpalaCustomOperatorTable;
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

public class IcebergCountStarOptimizer extends RelShuttleImpl {
 protected static final Logger LOG = LoggerFactory.getLogger(IcebergCountStarOptimizer.class.getName());

  @Override
  public RelNode visit(LogicalAggregate agg) {
    
    TableScan ts = getAggregatedTableScan(agg);
    if (ts == null) {
      return super.visit(agg);
    }

    RelOptCluster cluster = agg.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();

    CalciteTable table = (CalciteTable) ts.getTable();
    if (!(table.getFeFsTable() instanceof FeIcebergTable)) {
      return super.visit(agg);
    }
    FeIcebergTable iceTable = (FeIcebergTable) table.getFeFsTable();

    //XXX: need to implement if any count(*) is in there
    boolean hasACountStar = hasACountStar(agg);
    if (!hasACountStar(agg)) {
      return agg;
    }

    boolean hasCountStarOnly = hasCountStarOnly(agg);

    Long count = 0L;
    try {
      if (FeIcebergTable.Utils.hasDeleteFiles(iceTable, null)) { 
        count = FeIcebergTable.Utils.getRecordCountV2(iceTable, null);
        if (count <= 0) {
          return agg;
        }  
        RexLiteral countLiteral = rexBuilder.makeLiteral(count, ImpalaTypeConverter.getRelDataType(Type.BIGINT));
        List<RexNode> projects = new ArrayList<>();
        int i = 0;
        for (AggregateCall aggCall : agg.getAggCallList()) {
          RexNode aggInput = rexBuilder.makeInputRef(aggCall.getType(), i++);
          if (!(aggCall.getAggregation().getKind().equals(SqlKind.COUNT) &&
              aggCall.getArgList().size() == 0)) {
            projects.add(aggInput);
          } else {
            projects.add(rexBuilder.makeCall(ImpalaCustomOperatorTable.PLUS, aggInput, countLiteral));
          }
        }

        LogicalProject project = LogicalProject.create(agg, new ArrayList<>(), projects, agg.getRowType());
        return project;
      }
      count = FeIcebergTable.Utils.getRecordCountV1(iceTable.getIcebergApiTable(), null);
      if (count <= 0) {
        return agg;
      }
    } catch (Exception e) {
      //XXX: need something better
      throw new RuntimeException(e);
    }
    RexLiteral countLiteral = rexBuilder.makeLiteral(count, ImpalaTypeConverter.getRelDataType(Type.BIGINT));


    LOG.info("SJC: MADE IT INTO ONMATCH");

    if (hasCountStarOnly) {
      List<RexLiteral> literals = new ArrayList<>();
      for (AggregateCall aggCall : agg.getAggCallList()) {
        literals.add(countLiteral);
      }
      LogicalValues values = LogicalValues.create(cluster, agg.getRowType(), ImmutableList.of(ImmutableList.copyOf(literals)));
      return values;
    } else {
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
      return project;
    }
  }

  //XXX: in ImpalaAggRel
  private boolean hasCountStarOnly(Aggregate agg) {
    int size = agg.getGroupSet().size();
    if (!agg.getGroupSet().isEmpty()) {
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

  private boolean hasACountStar(Aggregate agg) {
    int size = agg.getGroupSet().size();
    if (!agg.getGroupSet().isEmpty()) {
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

  private TableScan getAggregatedTableScan(LogicalAggregate agg) {
    if (agg.getInputs().get(0) instanceof TableScan) {
      return (TableScan) agg.getInputs().get(0);
    }

    if (!(agg.getInputs().get(0) instanceof LogicalProject)) {
      return null;
    }

    LogicalProject proj = (LogicalProject) agg.getInputs().get(0);

    return proj.getInputs().get(0) instanceof TableScan
        ? (TableScan) proj.getInputs().get(0)
        : null;
  }
}

