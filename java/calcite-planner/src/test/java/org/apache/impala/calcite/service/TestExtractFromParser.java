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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Locale;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Litmus;
import org.apache.impala.common.ParseException;
import org.junit.Test;

/** Tests SQL-standard EXTRACT syntax and the existing two-argument syntax. */
public class TestExtractFromParser {
  @Test
  public void testDateOperand() throws Exception {
    assertEquivalent("day", "date '2019-08-17'", "DAY");
  }

  @Test
  public void testTimestampUnits() throws Exception {
    for (String unit : new String[] {"year", "month", "day", "hour", "minute",
        "second", "millisecond", "microsecond"}) {
      assertEquivalent(unit, "timestamp '2024-01-02 03:04:05'",
          unit.toUpperCase(Locale.ROOT));
    }
  }

  @Test
  public void testExpressionOperand() throws Exception {
    assertEquivalent("year", "cast(ts as timestamp)", "YEAR");
    assertEquivalent("month", "coalesce(ts1, ts2)", "MONTH");
  }

  @Test
  public void testNullOperand() throws Exception {
    assertEquivalent("year", "null", "YEAR");
  }

  @Test
  public void testCaseAndWhitespace() throws Exception {
    SqlNode actual = parseExpression("ExTrAcT( YeAr\n FrOm ts )");
    assertTrue(actual.equalsDeep(parseExpression("extract(ts, 'YEAR')"), Litmus.IGNORE));
  }

  @Test
  public void testCustomUnitName() throws Exception {
    assertEquivalent("custom_unit", "ts", "CUSTOM_UNIT");
  }

  @Test
  public void testTwoArgumentForm() throws Exception {
    SqlCall call = (SqlCall) parseExpression("extract(ts, 'year')");
    assertTrue("extract".equalsIgnoreCase(call.getOperator().getName()));
    assertEquals(2, call.operandCount());
    assertTrue("ts".equalsIgnoreCase(call.operand(0).toString()));
    assertEquals("'year'", call.operand(1).toString());
  }

  @Test
  public void testMalformedSyntax() throws Exception {
    for (String expression : new String[] {"extract(year from)", "extract(from ts)",
        "extract(year from ts, 'month')", "extract(year month from ts)",
        "extract(year from ts"}) {
      try {
        parseExpression(expression);
        fail("Expected rejection of malformed EXTRACT syntax");
      } catch (ParseException expected) {
        // The parser must reject malformed syntax.
      }
    }
  }

  private void assertEquivalent(String unit, String value, String expectedUnit)
      throws Exception {
    SqlNode actual = parseExpression("extract(" + unit + " from " + value + ")");
    SqlNode expected = parseExpression("extract(" + value + ", '" + expectedUnit + "')");
    assertTrue(actual.equalsDeep(expected, Litmus.IGNORE));
  }

  private SqlNode parseExpression(String expression) throws Exception {
    SqlSelect select = (SqlSelect) new CalciteQueryParser("select " + expression).parse();
    return select.getSelectList().get(0);
  }
}
