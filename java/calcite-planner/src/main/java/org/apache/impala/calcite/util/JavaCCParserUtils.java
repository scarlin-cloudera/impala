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

package org.apache.impala.calcite.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

/**
 * Utils to help with JavaCC.
 *
 * The code in this file was produced from a Gemini query.
 */
public class JavaCCParserUtils {

  /**
   * Extracts a string from a parser file based on line and column numbers.
   *
   * @param originalSource The full text of the parsed file.
   * @param beginLine      The 1-based starting line number.
   * @param beginColumn    The 1-based starting column number.
   * @param endLine        The 1-based ending line number.
   * @param endColumn      The 1-based ending column number.
   * @return The extracted substring.
   * @throws RuntimeException if the string could not be extracted.
   */
  public static String extractStringFromCoordinates(String originalSource,
      int beginLine, int beginColumn, int endLine, int endColumn) {
    try (BufferedReader reader = new BufferedReader(new StringReader(originalSource))) {
      int currentLine = 1;
      int currentAbsoluteIndex = 0;
      int startIndex = -1;
      int endIndex = -1;

      String line;
      while ((line = reader.readLine()) != null) {
        int lineLength = line.length();

        // If we are on the starting line, calculate the starting character index
        if (currentLine == beginLine) {
          // Convert 1-based column to 0-based index
          startIndex = currentAbsoluteIndex + (beginColumn - 1);
        }

        // If we are on the ending line, calculate the ending character index
        if (currentLine == endLine) {
          endIndex = currentAbsoluteIndex + endColumn;
          break; // We have both indices, we can stop reading
        }

        // Add 1 for the newline character that readLine() strips
        currentAbsoluteIndex += lineLength + 1;
        currentLine++;
      }

      // Extract the string if valid coordinates were found
      if (startIndex != -1 && endIndex != -1 && startIndex <= endIndex &&
          endIndex <= originalSource.length()) {
        return originalSource.substring(startIndex, endIndex);
      }
    } catch (IOException e) {
      // This should never happen. Assuming the caller passes in correct values.
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Error in JavaCC parser, bad coordinates passed in." +
        "beginLine=" + beginLine + ", beginColumn=" + beginColumn + ", endLine=" +
        endLine + ",endColumn=" + endColumn);
  }
}
