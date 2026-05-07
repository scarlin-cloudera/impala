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

package org.apache.impala.calcite.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.rel.util.ImpalaBaseTableRef;
import org.apache.impala.calcite.rel.util.PrunedPartitionHelper;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.type.ImpalaTypeFactoryImpl;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.planner.HdfsEstimatedMissingTableStats;
import org.apache.impala.planner.HdfsPartitionPruner;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.IcebergUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CalciteTable extends RelOptAbstractTable
    implements Table, Prepare.PreparingTable {
  private final FeFsTable table_;

  private final Map<Integer, Integer> impalaPositionMap_;

  private final List<String> qualifiedTableName_;

  // Pruned partition map. The pruned partitions are calculated during join optimization.
  // Keep them in a cache so they don't have to be recalculated.
  private final Map<RexNode, PrunedPartitionHelper> prunedPartitionMap = new HashMap<>();

  private final List<Column> columns_;

  private final SimplifiedAnalyzer analyzer_;

  // Not final due to lazy loading
  private HdfsEstimatedMissingTableStats estimatedMissingStats_;

  // The tuple descriptor which is needed by the partition pruner.
  private TupleDescriptor tupleDescForPruning_;

  // Output expressions of the tuple descriptor used by the partition pruner
  // (just a bunch of SlotRefs)
  private List<Expr> outputExprs_ = null;

  public CalciteTable(FeTable table, CalciteCatalogReader reader,
      Analyzer analyzer) throws ImpalaException {
    super(reader, table.getName(), buildColumnsForRelDataType(table));
    this.table_ = (FeFsTable) table;
    this.qualifiedTableName_ = table.getTableName().toPath();
    this.columns_ = table.getColumnsInHiveOrder();
    this.impalaPositionMap_ = buildPositionMap();
    this.analyzer_ = (SimplifiedAnalyzer) analyzer;

    checkIfTableIsSupported(table);
  }

  public static RelDataType buildColumnsForRelDataType(FeTable table)
      throws ImpalaException {
    RelDataTypeFactory typeFactory = ImpalaTypeFactoryImpl.INSTANCE;

    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

    // skip clustering columns, save them for the end
    for (Column column : table.getColumnsInHiveOrder()) {
      if (column.getType().isComplexType()) {
        throw new UnsupportedFeatureException(
            "Calcite does not support complex types yet.");
      }
      RelDataType type =
          ImpalaTypeConverter.createRelDataType(typeFactory, column.getType());
      builder.add(column.getName(), type);
    }
    return builder.build();
  }

  private void checkIfTableIsSupported(FeTable table) throws ImpalaException {
    if (table instanceof FeView) {
      throw new UnsupportedFeatureException("Views are not supported yet.");
    }

    if (!(table instanceof FeFsTable)) {
      String tableType = table.getClass().getSimpleName().replace("Table", "");
      throw new UnsupportedFeatureException(tableType + " tables are not supported yet.");
    }
  }

  public BaseTableRef createBaseTableRef() throws ImpalaException {

    TableRef tblRef = new TableRef(qualifiedTableName_, null);

    Path resolvedPath = analyzer_.resolvePath(tblRef.getPath(), Path.PathType.TABLE_REF);

    BaseTableRef baseTblRef = new ImpalaBaseTableRef(tblRef, resolvedPath, analyzer_);
    baseTblRef.analyze(analyzer_);
    return baseTblRef;
  }

  /**
   * Return the pruned partitions
   * TODO: Currently all partitions are returned since filters aren't yet supported.
   */
  public List<? extends FeFsPartition> getPrunedPartitions(Analyzer analyzer,
      TupleDescriptor tupleDesc) throws ImpalaException {
    HdfsPartitionPruner pruner = new HdfsPartitionPruner(tupleDesc);
    // TODO: pass in the conjuncts needed. An empty conjunct will return all partitions.
    List<Expr> conjuncts = new ArrayList<>();
    Pair<List<? extends FeFsPartition>, List<Expr>> impalaPair =
        pruner.prunePartitions(analyzer, conjuncts, true, false, null);
    return impalaPair.first;
  }

  public FeFsTable getFeFsTable() { return table_; }

  @Override
  public List<String> getQualifiedName() {
    return qualifiedTableName_;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String column,
      SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return true;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public Statistic getStatistic() {
    return null;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return getRowType();
  }

  @Override
  public <T> T unwrap(Class<T> arg0) {
    // Generic unwrap needed by the Calcite framework to process the table.
    return arg0.isInstance(this) ? arg0.cast(this) : null;
  }

  @Override
  public boolean columnHasDefaultValue(RelDataType rowType, int ordinal,
      InitializerContext initializerContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTemporal() {
    return false;
  }

  @Override
  public boolean supportsModality(SqlModality modality) {
    return true;
  }

  @Override
  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }

  @Override
  public SqlMonotonicity getMonotonicity(String columnName) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  @Override
  public double getRowCount() {
    if (table_.getNumRows() >= 0.0) {
      return (double) table_.getNumRows();
    }

    if (estimatedMissingStats_ == null) {
      estimatedMissingStats_ = new HdfsEstimatedMissingTableStats(
          analyzer_.getQueryOptions(), table_, table_.loadAllPartitions(), -1);
    }
    return (estimatedMissingStats_.statsNumRows_ >= 0.0)
        ? estimatedMissingStats_.statsNumRows_
        : Double.MAX_VALUE;

  }

  /**
   * Create a pruned partition helper. This is called from join optimization for
   * retrieving row counts and from the HdfsScanRel when creating the final
   * pruned partitions to be used at runtime.
   */
  public PrunedPartitionHelper createPrunedPartitionHelper(RexNode condition,
      List<Expr> inputExprs, TupleDescriptor tupleDesc, RexBuilder rexBuilder
      ) throws ImpalaException {
    ExprConjunctsConverter converter = new ExprConjunctsConverter(
        condition, inputExprs, rexBuilder, analyzer_);

    return new PrunedPartitionHelper(this, converter, tupleDesc, rexBuilder, analyzer_);
  }

  /**
   * Get the pruned partition helper, creating one if it doesn't exist.
   */
  public PrunedPartitionHelper getPrunedPartitionHelper(RexNode condition,
      RexBuilder rexBuilder) throws ImpalaException {
    // Check if pruned partition helper is in the cache.
    if (prunedPartitionMap.get(condition) != null) {
      return prunedPartitionMap.get(condition);
    }

    // lazy creation, since this is only needed for pruning.
    if (tupleDescForPruning_ == null) {
      tupleDescForPruning_ = createTupleAndSlotDesc(createBaseTableRef(),
          getRowType().getFieldNames());
      // the 'output' exprs for the table.  This is needed by the
      // Expr converter since the conditions will have references
      // to the SlotExpr for the table.
      outputExprs_ = createOutputExprs(tupleDescForPruning_.getSlots());
    }

    PrunedPartitionHelper pph = createPrunedPartitionHelper(condition,
        outputExprs_, tupleDescForPruning_, rexBuilder);

    prunedPartitionMap.put(condition, pph);
    return pph;
  }

  // Create tuple and slot descriptors for this base table
  private TupleDescriptor createTupleAndSlotDesc(BaseTableRef baseTblRef,
      List<String> fieldNames) throws ImpalaException {
    // create the slot descriptors corresponding to this tuple descriptor
    // by supplying the field names from Calcite's output schema for this node
    for (String fieldName : fieldNames) {
      SlotRef slotref =
          new SlotRef(Path.createRawPath(baseTblRef.getUniqueAlias(), fieldName));
      slotref.analyze(analyzer_);
      SlotDescriptor slotDesc = slotref.getDesc();
      if (slotDesc.getType().isCollectionType()) {
        throw new AnalysisException(String.format(fieldName + " "
            + "is a complex type (array/map/struct) column. "
            + "This is not currently supported."));
      }
      slotDesc.setIsMaterialized(true);
    }
    TupleDescriptor tupleDesc = baseTblRef.getDesc();
    return tupleDesc;
  }


  public List<Column> getColumns() {
    return columns_;
  }

  public Column getColumn(int i) {
    return columns_.get(i);
  }

  private List<Expr> createOutputExprs(List<SlotDescriptor> slotDescs) {
    ImmutableList.Builder<Expr> builder = new ImmutableList.Builder();

    for (SlotDescriptor slotDesc : slotDescs) {
      builder.add(new SlotRef(slotDesc));
    }
    return builder.build();
  }

  /**
   * Returns a position map from Impala column numbers to Calcite
   * column numbers. Calcite places the columns in "Hive Order"
   * so that a 'select *' will return the column list in the
   * right order, but this map is needed because sometimes we
   * only have the "Column.getPosition()" column number which
   * is the Impala column number.
   */
  private Map<Integer, Integer> buildPositionMap() {

    Map<Integer, Integer> impalaPositionMap = new HashMap<>();
    // skip clustering columns, save them for the end
    int i = 0;
    for (Column column : table_.getColumnsInHiveOrder()) {
      impalaPositionMap.put(column.getPosition(), i);
      i++;
    }

    return impalaPositionMap;
  }

  public int getCalcitePosition(int impalaPosition) {
    return impalaPositionMap_.get(impalaPosition);
  }

  public int getNumberColumnsIncludingAcid() {
    return impalaPositionMap_.keySet().size();
  }

  /**
   * Returns true if the conditions on the table meet the requirements
   * needed to apply the count star optimization.
   */
  public boolean canApplyCountStarOptimization() {
    Set<HdfsFileFormat> fileFormats = table_.getFileFormats();
    if (fileFormats.size() != 1) {
      return false;
    }
    if (!fileFormats.contains(HdfsFileFormat.ORC) &&
        !fileFormats.contains(HdfsFileFormat.PARQUET) &&
        !fileFormats.contains(HdfsFileFormat.HUDI_PARQUET)) {
      return false;
    }
    if (AcidUtils.isFullAcidTable(table_.getMetaStoreTable().getParameters())) {
      return false;
    }

    try {
      if (table_ instanceof FeIcebergTable) {
        FeIcebergTable iceTable = (FeIcebergTable) table_;
        if (FeIcebergTable.Utils.hasDeleteFiles(iceTable, null)) { 
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }

    return true;
  }

  public boolean isOnlyClusteredCols(Collection<String> fieldNames) {
    for (String fieldName : fieldNames) {
      Column c = table_.getColumn(fieldName);
      if (c instanceof IcebergColumn) {
        FeIcebergTable icebergTable = (FeIcebergTable) table_;
        for (IcebergPartitionSpec spec : icebergTable.getPartitionSpecs()) {
          if (IcebergUtil.getPartitionTransformType((IcebergColumn)c, spec) !=
              TIcebergPartitionTransformType.IDENTITY) {
            return false;
          }    
        }  
      } else {
        if (!table_.isClusteringColumn(c)) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean isIcebergTable() {
    return table_ instanceof FeIcebergTable;
  }
}
