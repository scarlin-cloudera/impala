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

package org.apache.impala.planner;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes row count for a Kudu table when HMS stats are unavailable.
 *
 * <p>Follows the same fallback logic as {@link KuduScanNode}: use HMS stats when
 * present, otherwise fetch Kudu's live row count.
 */
public final class KuduEstimatedMissingTableStats {
  private static final Logger LOG =
      LoggerFactory.getLogger(KuduEstimatedMissingTableStats.class);

  private KuduEstimatedMissingTableStats() {}

  /**
   * Fetches Kudu's live row count when HMS stats are unavailable.
   * Caller must ensure {@code table.getNumRows() < 0}. Returns -1 on failure.
   */
  public static long getStatsNumRows(FeKuduTable table, Analyzer analyzer) {
    try {
      // This is similar to what we do in KuduScanNode#init() in the classic planner.
      org.apache.kudu.client.KuduTable rpcTable = analyzer.getKuduTable(table);
      return rpcTable.getTableStatistics().getLiveRowCount();
    } catch (AnalysisException | KuduException e) {
      LOG.warn("Unable to get live row count for Kudu table '{}'",
          table.getFullName(), e);
      return -1;
    }
  }
}
