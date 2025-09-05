# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function
import logging

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (add_mandatory_exec_option)

LOG = logging.getLogger(__name__)


@CustomClusterTestSuite.with_args(start_args="--env_vars=USE_CALCITE_PLANNER=true")
class TestCalcitePlanner(CustomClusterTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCalcitePlanner, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'use_calcite_planner', 'true')

  def test_calcite_frontend(self, vector, unique_database):
    """Calcite planner does not work in local catalog mode yet."""
    vector.get_value('exec_option')['calcite_fallback'] = 'nonquery_only'
    self.run_test_case('QueryTest/calcite', vector, use_db=unique_database)

  def test_calcite_fallback(self):
    # a select from a complex column will work for the 2 fallback options tested here.
    options = {'calcite_fallback': 'all_exceptions'}
    self.execute_query("select int_array_col from functional.allcomplextypes where 0 = 1",
        options)
    options = {'calcite_fallback': 'unsupported_and_nonquery'}
    self.execute_query("select int_array_col from functional.allcomplextypes where 0 = 1",
        options)

  @pytest.mark.execute_serially
  def test_semicolon(self, cursor):
    cursor.execute("set use_calcite_planner=true;")
    cursor.execute("select 4;")

  def test_cte_plans(self, vector, unique_database):
    # Force single node plans to focus on Calcite planner.
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['cte_threshold'] = 1
    self.run_test_case('QueryTest/cte', vector, use_db=unique_database)

  def test_cte_distributed(self, vector, unique_database):
    vector.get_value('exec_option')['cte_threshold'] = 1
    self.run_test_case('QueryTest/cte-distributed', vector, use_db=unique_database)
