/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.jupiter.api.Test

class FlinkSqlTestForUpgrade  extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  util
    .addDataStream[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  util.addDataStream[(Int, String, Long)](
    "MyTable2",
    'a,
    'b,
    'c,
    'proctime.proctime,
    'rowtime.rowtime)

  util.tableEnv.executeSql(s"""
                              |CREATE TABLE MyTable3 (
                              |  a int,
                              |  b bigint,
                              |  c string,
                              |  rowtime as TO_TIMESTAMP_LTZ(b, 3),
                              |  watermark for rowtime as rowtime
                              |) WITH (
                              |  'connector' = 'values',
                              |  'bounded' = 'false'
                              |)
       """.stripMargin)

  @Test
  def testLikeWithoutOptions(): Unit = {
    util.tableEnv.executeSql(s"""
                                |CREATE TABLE MyTable5 (
                                |  d int
                                |) like MyTable3
       """.stripMargin)
  }
}
