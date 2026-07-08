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

package org.apache.impala.calcite.validate;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.impala.analysis.TimeTravelSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * The ImpalaSnapshotSqlNode extends the Calcite SnapshotSqlNode with a TimeTravelSpec
 * needed by Impala for the snapshot.
 */
public class ImpalaSnapshotSqlNode extends SqlSnapshot {

  private static final String DUMMY_TIMESTAMP = "1970-01-01 00:00:01";

  public final TimeTravelSpec timeTravelSpec_;
  public final SqlIdentifier tableRefOriginal_;

  public ImpalaSnapshotSqlNode(SqlParserPos parserPos, SqlNode tableRefInput,
      TimeTravelSpec t) {
    super(parserPos, getTimeTravelSpecTableRef((SqlIdentifier) tableRefInput, t),
        getDummyPeriod(parserPos));
    tableRefOriginal_ = (SqlIdentifier) tableRefInput;
    timeTravelSpec_ = t;
  }

  /**
   * Create a new time travel SqlNode which contains a modified table name that
   * contains the real Iceberg table concatenated with the unique identifier
   * "_tt_<hash_code>".
   * Note: It would be a little better to use the Impala snapshotId rather than the
   * object's hashcode as a unique identifier. However, this object is created at
   * parse time along with its unique identifier, and the catalog table object is
   * not known at this point.
   */
  private static SqlNode getTimeTravelSpecTableRef(SqlIdentifier tableRef,
      TimeTravelSpec t) {
    List<String> names = new ArrayList<>();
    // The first <n-1> names (probably just the db) do not change. The table name is
    // the last element in the array.
    for (int i = 0; i < tableRef.names.size() - 1; ++i) {
      names.add(tableRef.names.get(i));
    }
    String newTableName = tableRef.names.get(tableRef.names.size() - 1);
    newTableName += "_tt_" + Integer.toString(t.hashCode());
    return new SqlIdentifier(names, tableRef.getParserPosition());
  }

  /**
   * The "period" variable is not used by Impala since Impala cannot just
   * use a timestamp field. The TimeTravelSpec can contain not only a timestamp,
   * but also a long snapshotId. A null is not allowed, so a random dummy timestamp
   * is provided.
   */
  public static SqlTimestampLiteral getDummyPeriod(SqlParserPos pos) {
    return SqlParserUtil.parseTimestampLiteral(DUMMY_TIMESTAMP, pos);
  }
}
