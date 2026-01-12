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
package org.apache.impala.calcite.schema;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;

/**
 * ImpalaRelColumnOrigin is an extension of the RelColumnOrigin class with
 * the added TableScan member. The TableScan is needed for the Runtime
 * Filter calculations. The RelColumnOrigin class by itself only gives the
 * table/column information which is a problem if the same table is used
 * twice within a query.
 */
public class ImpalaRelColumnOrigin extends RelColumnOrigin {
  private final TableScan tableScan_;

  public ImpalaRelColumnOrigin (
      RelOptTable originTable,
      int iOriginColumn,
      boolean isDerived,
      TableScan tableScan) {
    super(originTable, iOriginColumn, isDerived);
    this.tableScan_ = tableScan;
  }

  public TableScan getTableScan() {
    return tableScan_;
  }
}
