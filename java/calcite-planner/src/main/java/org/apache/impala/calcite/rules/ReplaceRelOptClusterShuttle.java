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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Shuttle to replace the RelOptCluster that is used for RelNodes.
 * The RelOptCluster contains the RexBuilder object.  The RelNodes
 * are created in the RelNodeConverter phase and optimized in the
 * Optimizer phase, but they require different customizations for
 * the RexBuilder. So this shuttle replaces the RelNodeConverter
 * ImpalaRexBuilder with the RexBuilder.
 *
 * The general implementation was grabbed from RelShuttleImpl with
 * the adjustments to do the RelOptCluster replacement.
 */
public class ReplaceRelOptClusterShuttle implements RelShuttle {
  private final RelOptCluster cluster_;

  protected final Deque<RelNode> stack = new ArrayDeque<>();

  public ReplaceRelOptClusterShuttle(RelOptCluster cluster) {
    cluster_ = cluster;
  }

  /**
   * Visits a particular child of a parent.
   */
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    stack.push(parent);
    try {
      RelNode child2 = child.accept(this);
      if (child2 != child) {
        final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
        newInputs.set(i, child2);
        return parent.copy(parent.getTraitSet(), newInputs);
      }
      return parent;
    } finally {
      stack.pop();
    }
  }

  protected RelNode visitChildren(RelNode rel) {
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      rel = visitChild(rel, input.i, input.e);
    }
    return rel;
  }

  @Override public RelNode visit(LogicalAggregate aggregate) {
    LogicalAggregate agg =
        (LogicalAggregate) visitChild(aggregate, 0, aggregate.getInput());
    return new LogicalAggregate(cluster_, agg.getTraitSet(), agg.getHints(),
        agg.getInput(), agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
  }

  @Override public RelNode visit(LogicalMatch match) {
    throw new RuntimeException("Not implemented");
  }

  @Override public RelNode visit(TableScan scan) {
    return new LogicalTableScan(cluster_, scan.getTraitSet(),
        scan.getHints(), scan.getTable());
  }

  @Override public RelNode visit(TableFunctionScan scan) {
    TableFunctionScan newScan = (TableFunctionScan) visitChildren(scan);
    return new LogicalTableFunctionScan(cluster_, newScan.getTraitSet(),
        newScan.getHints(), newScan.getInputs(), newScan.getCall(),
        newScan.getElementType(), newScan.getRowType(), newScan.getColumnMappings());
  }

  @Override public RelNode visit(LogicalValues values) {
    return new LogicalValues(cluster_, values.getTraitSet(), values.getHints(),
        values.getRowType(), values.getTuples());
  }

  @Override public RelNode visit(LogicalFilter filter) {
    LogicalFilter newFilter =
        (LogicalFilter) visitChild(filter, 0, filter.getInput());
    return new LogicalFilter(cluster_, newFilter.getTraitSet(), newFilter.getHints(),
        newFilter.getInput(), newFilter.getCondition(),
        ImmutableSet.copyOf(newFilter.getVariablesSet()));
  }

  @Override public RelNode visit(LogicalCalc calc) {
    throw new RuntimeException("Not implemented");
  }

  @Override public RelNode visit(LogicalProject project) {
    LogicalProject newProject =
        (LogicalProject) visitChild(project, 0, project.getInput());
    return new LogicalProject(cluster_, newProject.getTraitSet(),
        newProject.getHints(), newProject.getInput(), newProject.getProjects(),
        newProject.getRowType(), ImmutableSet.copyOf(newProject.getVariablesSet()));
  }

  @Override public RelNode visit(LogicalJoin join) {
    LogicalJoin newJoin =
        (LogicalJoin) visitChildren(join);
    return new LogicalJoin(cluster_, newJoin.getTraitSet(), newJoin.getHints(),
        newJoin.getLeft(), newJoin.getRight(), newJoin.getCondition(),
        ImmutableSet.copyOf(newJoin.getVariablesSet()), newJoin.getJoinType(),
        false, ImmutableList.copyOf(newJoin.getSystemFieldList()));
  }

  @Override public RelNode visit(LogicalCorrelate correlate) {
    LogicalCorrelate newCorrelate =
        (LogicalCorrelate) visitChildren(correlate);
    return new LogicalCorrelate(cluster_, newCorrelate.getTraitSet(),
        newCorrelate.getHints(), newCorrelate.getLeft(), newCorrelate.getRight(),
        newCorrelate.getCorrelationId(), newCorrelate.getRequiredColumns(),
        newCorrelate.getJoinType());
  }

  @Override public RelNode visit(LogicalUnion union) {
    LogicalUnion newUnion =
        (LogicalUnion) visitChildren(union);
    return new LogicalUnion(cluster_, newUnion.getTraitSet(), newUnion.getHints(),
        newUnion.getInputs(), newUnion.all);
  }

  @Override public RelNode visit(LogicalIntersect intersect) {
    LogicalIntersect newIntersect =
        (LogicalIntersect) visitChildren(intersect);
    return new LogicalIntersect(cluster_, newIntersect.getTraitSet(),
        newIntersect.getHints(), newIntersect.getInputs(), newIntersect.all);
  }

  @Override public RelNode visit(LogicalMinus minus) {
    LogicalMinus newMinus =
        (LogicalMinus) visitChildren(minus);
    return new LogicalMinus(cluster_, newMinus.getTraitSet(), newMinus.getHints(),
        newMinus.getInputs(), newMinus.all);
  }

  @Override public RelNode visit(LogicalSort sort) {
    LogicalSort newSort = (LogicalSort) visitChildren(sort);
    return LogicalSort.create(newSort.getInput(), newSort.getCollation(),
        newSort.offset, newSort.fetch);
  }

  @Override public RelNode visit(LogicalExchange exchange) {
    throw new RuntimeException("Not implemented");
  }

  @Override public RelNode visit(LogicalTableModify modify) {
    throw new RuntimeException("Not implemented");
  }

  @Override public RelNode visit(RelNode other) {
    throw new RuntimeException("Not implemented");
  }
}
