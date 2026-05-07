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

package org.apache.impala.calcite.rel.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.calcite.rel.phys.ImpalaHdfsScanNode;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.rel.util.PrunedPartitionHelper;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.planner.IcebergScanPlanner;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.ScanNode;
import org.apache.impala.planner.SingleNodePlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
/**
 * ImpalaHdfsScanRel. Calcite RelNode which maps to an Impala TableScan node.
 */
public class ImpalaHdfsScanRel extends TableScan
    implements ImpalaPlanRel {

  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaHdfsScanRel.class.getName());
  public ImpalaHdfsScanRel(TableScan scan) {
    super(scan.getCluster(), scan.getTraitSet(), scan.getHints(), scan.getTable());
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {

    CalciteTable table = (CalciteTable) getTable();

    BaseTableRef baseTblRef = table.createBaseTableRef();

    produceSlotDescriptorsForTable(baseTblRef, context);

    TupleDescriptor tupleDesc = baseTblRef.getDesc();

    // outputExprs will contain all the needed columns from the table
    List<Expr> outputExprs = createScanOutputExprs(tupleDesc.getSlots());

    Analyzer analyzer = context.ctx_.getRootAnalyzer();

    // break up the filter condition (if given) to ones that can be used for
    // partition pruning and ones that cannot.
    PrunedPartitionHelper pph = table.createPrunedPartitionHelper(
        context.filterCondition_, outputExprs, tupleDesc,
        getCluster().getRexBuilder());
    List<? extends FeFsPartition> impalaPartitions = pph.getPrunedPartitions();

    List<Expr> partitionConjuncts = pph.getPartitionedConjuncts();
    List<Expr> filterConjuncts = pph.getNonPartitionedConjuncts();

    PlanNodeId nodeId = context.ctx_.getNextNodeId();

    // Under special conditions, a count star optimization can be applied, which
    // needs a special slot descriptor and slot ref.
    SlotDescriptor countStarDesc =
        canUseCountStarOptimization(table, context, filterConjuncts, baseTblRef)
            ? ScanNode.createCountStarOptimizationDesc(tupleDesc, analyzer)
            : null;

    Expr countStarOptimizationExpr = countStarDesc != null
        ? new SlotRef(countStarDesc)
        : null;

    PlanNode physicalNode;
    if (SingleNodePlanner.addAcidSlotsIfNeeded(analyzer, baseTblRef,
        impalaPartitions)) {
      // Add the Acid Join Node if needed, which places a join node on top of the scan
      // node to handle the acid deltas.
      physicalNode = SingleNodePlanner.createAcidJoinNode(analyzer,
          baseTblRef, filterConjuncts, impalaPartitions, partitionConjuncts,
          context.ctx_);
    } else {
      boolean isPartitionScanOnly = isPartitionScanOnly(context, table);
      if (isPartitionScanOnly &&
          context.ctx_.getQueryOptions().optimize_partition_key_scans) {
        physicalNode =
            SingleNodePlanner.createOptimizedPartitionUnionNode(nodeId, impalaPartitions,
            tupleDesc, analyzer);
      } else if (table.isIcebergTable()) {
        LOG.info("SJC: THIS IS AN ICEBERG TABLE");
        // XXX: should change internals of iceberg to handle immutable
        List<Expr> copiedConjuncts = new ArrayList<>(filterConjuncts);
        boolean isDistinctOnly = context.parentAggregate_ != null &&
            context.parentAggregate_.hasDistinctOnly() ;
        IcebergScanPlanner icebergPlanner = new IcebergScanPlanner(analyzer, context.ctx_,
            baseTblRef, copiedConjuncts, null, isDistinctOnly);
        physicalNode = icebergPlanner.createIcebergScanPlan();
      } else {
        physicalNode = new ImpalaHdfsScanNode(nodeId, tupleDesc, impalaPartitions,
            baseTblRef, null, partitionConjuncts, filterConjuncts, countStarDesc,
            isPartitionScanOnly(context, table));
      }
    }
    physicalNode.setOutputSmap(new ExprSubstitutionMap());
    physicalNode.init(analyzer);

    return new NodeWithExprs(physicalNode, outputExprs,
        getRowType().getFieldNames(), countStarOptimizationExpr);
  }

  /**
   * Return a list of (SlotRef) expressions for the scan node.  The list will
   * be the size of the # of columns in the table. We initialize the list to
   * contain null values for all elements.
   *
   * If a column isn't projected out by the parent of the scan node, the array
   * location for the column will remain null.
   */
  private List<Expr> createScanOutputExprs(List<SlotDescriptor> slotDescs)
      throws ImpalaException {
    CalciteTable calciteTable = (CalciteTable) getTable();
    FeFsTable table = calciteTable.getFeFsTable();
    // IMPALA-12961: The output expressions are contained in a list which
    // may have holes in it (if the table scan column is not in the output).
    // The width of the list must include all columns, including the acid ones,
    // even though the acid columns are not currently supported in the
    // Calcite.getRowType()
    int totalCols = calciteTable.getNumberColumnsIncludingAcid();

    // Initialize all fields to null.
    List<Expr> scanOutputExprs = new ArrayList<>(Collections.nCopies(totalCols, null));

    for (SlotDescriptor slotDesc : slotDescs) {
      Column impalaColumn = slotDesc.getColumn();
      if (impalaColumn == null) {
        // The slot descriptor list does not contain a column in the case of the
        // STATS_NUM_ROWS label, which is only used for count star optimization.
        Preconditions.checkState(slotDesc.getLabel().equals(ScanNode.STATS_NUM_ROWS));
        continue;
      }

      // On a "select *" with partitioned columns, the partition columns occur after the
      // nonpartitioned columns.  But Impala displays the partition columns first. The
      // getCalcitePosition() method gets the correct column number.
      Integer calcitePosition =
          calciteTable.getCalcitePosition(impalaColumn.getPosition());
      if (calcitePosition == null ) {
        throw new UnsupportedFeatureException(
            "Calcite does not support column: " + slotDesc.getColumn().getName());
      }

      scanOutputExprs.set(calcitePosition, new SlotRef(slotDesc));
    }
    return scanOutputExprs;
  }

  /**
   * Produce the slot descriptors for the given table.
   *
   * This code is a bit clunky, imo. The analyze method for SlotRef registers the
   * SlotDescriptor for global use within the analyzer. It's not too apparent, you
   * gotta dig underneath the covers to see this, but it's a necessary step to
   * produce the SlotDescriptor objects needed for the query.
   *
   */
  private void produceSlotDescriptorsForTable(BaseTableRef baseTblRef,
      ParentPlanRelContext context) throws ImpalaException {
    Map<Integer, String> inputRefMap = getInputRefMap(context);
    for (Integer i : inputRefMap.keySet()) {
      String fieldName = inputRefMap.get(i);
      SlotRef slotref =
          new SlotRef(Path.createRawPath(baseTblRef.getUniqueAlias(), fieldName));
      slotref.analyze(context.ctx_.getRootAnalyzer());
      SlotDescriptor slotDesc = slotref.getDesc();
      if (slotDesc.getType().isComplexType()) {
        throw new UnsupportedFeatureException(String.format(fieldName + " "
            + "is a complex type (array/map) column. "
            + "This is not currently supported."));
      }
      if (isMaterializedInputRef(context, i)) {
        slotDesc.setIsMaterialized(true);
      }
    }
  }

  /**
   * Retrieves the special count star descriptor if it exists, which is used
   * for count star optimization.
   */
  private SlotDescriptor getCountStarDescriptor(List<SlotDescriptor> slotDescs) {
    Optional<SlotDescriptor> returnVal = slotDescs.stream()
        .filter(n -> n.getLabel().equals(ScanNode.STATS_NUM_ROWS)).findFirst();
    return returnVal.isPresent() ? returnVal.get() : null;
  }

  /**
   * Returns true if the partition scan optimization can be applied. The parentAggregate
   * will be set if the aggregate is close to this scan rel node. The parentAggregate will
   * be above the TableScan RelNode either directly, or above a Filter or Project.
   * It can be applied if only clustered columns are being projected up into the Agg
   * RelNode.
   */
  private boolean isPartitionScanOnly(ParentPlanRelContext context, CalciteTable table)
      throws ImpalaException {
    return context.parentAggregate_ != null &&
        context.parentAggregate_.hasDistinctOnly() &&
        table.isOnlyClusteredCols(getInputRefFieldNames(context));
  }

  /**
   * Returns true if we can use the count star optimization. The re
   */
  private boolean canUseCountStarOptimization(CalciteTable table,
      ParentPlanRelContext context, List<Expr> filterConjuncts,
      TableRef tableRef) {
    // The count(*) will exist in the parent aggregate if it can be used.
    if (context.parentAggregate_ == null ||
        !context.parentAggregate_.hasCountStarOnly()) {
      return false;
    }

    // Maybe this one works for all
    if (context.inputRefs_ == null || !context.inputRefs_.isEmpty()) {
      return false;
    }


    // XXX: not sure I like this. Also, what if num <= 0 (see SelectStmt line 1572)?
    if (context.parentAggregate_.hasCountStarOnly()) {
      if (table.getFeFsTable() instanceof FeIcebergTable) {
        FeIcebergTable iceTable = (FeIcebergTable) table.getFeFsTable();
        try {
          if (table.isIcebergTable() && FeIcebergTable.Utils.hasDeleteFiles(iceTable, null)) {
            tableRef.setOptimizeCountStarForIcebergV2(true);
          }
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
      }
    }

    if (!table.canApplyCountStarOptimization()) {
      return false;
    }

    if (!(table.isOnlyClusteredCols(getInputRefFieldNames(context)))) {
      if (table.isIcebergTable()) {
        if ((context.inputMaterializedRefs_ == null ||
          !context.inputMaterializedRefs_.isEmpty())) {
          return false;
        }
      } else {
        return false;
      }
    }

    // Can only use the optimization if there are no filters applied on this scan.
    if (filterConjuncts.size() > 0 && !table.isIcebergTable()) {
      return false;
    }
    return true;
  }

  private Collection<String> getInputRefFieldNames(ParentPlanRelContext context) {
    return getInputRefMap(context).values();
  }

  private Map<Integer, String> getInputRefMap(ParentPlanRelContext context) {
    // If the parent context didn't pass in input refs, we will select all the
    // columns from the table.
    Map<Integer, String> inputRefMap = new LinkedHashMap<>();
    if (context.inputRefs_ == null) {
      //XXX: perhaps a stream function
      for (int i = 0; i < getRowType().getFieldNames().size(); ++i) {
        inputRefMap.put(i, getRowType().getFieldNames().get(i));
      }
      return inputRefMap;
    }

    List<String> inputRefFieldNames = new ArrayList<>();
    for (Integer i : context.inputRefs_) {
      inputRefMap.put(i, getRowType().getFieldNames().get(i));
    }
    return inputRefMap;
  }

  private boolean isMaterializedInputRef(ParentPlanRelContext context, int i) {
    if (context.inputMaterializedRefs_ == null) {
      return true;
    }
    return context.inputMaterializedRefs_.get(i);
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.HDFSSCAN;
  }
}
