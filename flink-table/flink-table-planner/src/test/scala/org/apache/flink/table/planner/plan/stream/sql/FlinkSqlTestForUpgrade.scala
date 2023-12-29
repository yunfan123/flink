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
import org.apache.flink.table.planner.plan.stream.sql.FlinkSqlTestForUpgrade.UpdateKind.UpdateKind
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

object FlinkSqlTestForUpgrade {
  private def inputUniqueKeyContainsJoinKey() = {
    import org.apache.calcite.util.ImmutableBitSet

    val inputUniqueKeys = Set(ImmutableBitSet.of(0))
    val inputUniqueKeys1 = List(Array(0, 1))
    val joinKeys = Array(0, 1)
    println(inputUniqueKeys1.exists(uniqueKey => joinKeys.forall(uniqueKey.contains(_))))
    println(inputUniqueKeys.exists {
      uniqueKey => joinKeys.forall(uniqueKey.toArray.contains(_))
    })
  }

  private def beforeAfterOrNone(inputModifyContainsUpdate: Boolean): String = {
    if (inputModifyContainsUpdate) {
      "before_and_after"
    } else {
      "none"
    }
  }

  private def onlyAfterOrNone(inputModifyContainsUpdate: Boolean): String = {
    if (inputModifyContainsUpdate) {
      "only_after"
    } else {
      "none"
    }
  }

  private def testInit(
      allJoinKeyInUniq: Boolean,
      inputModifyContainsUpdate: Boolean,
      requiredUpdateKind: UpdateKind): String = {
    val requiredUpdateBeforeByParent = requiredUpdateKind == UpdateKind.BEFORE_AND_AFTER
    val needUpdateBefore = !allJoinKeyInUniq
    if (needUpdateBefore || requiredUpdateBeforeByParent) {
      beforeAfterOrNone(inputModifyContainsUpdate)
    } else {
      onlyAfterOrNone(inputModifyContainsUpdate)
    }
  }

  private def testAfterHotFix(
      allJoinKeyInUniq: Boolean,
      inputModifyContainsUpdate: Boolean,
      requiredUpdateKind: UpdateKind): String = {
    val onlyAfterByParent = requiredUpdateKind == UpdateKind.ONLY_UPDATE_AFTER
    if (onlyAfterByParent) {
      if (inputModifyContainsUpdate && !allJoinKeyInUniq) {
        "hot_fix_none"
      } else {
        onlyAfterOrNone(inputModifyContainsUpdate)
      }
    } else {
      beforeAfterOrNone(inputModifyContainsUpdate)
    }
  }

  private def testByted(
      allJoinKeyInUniq: Boolean,
      inputModifyContainsUpdate: Boolean,
      requiredUpdateKind: UpdateKind,
      generateNull: Boolean = false): String = {
    val requiredUpdateBeforeByParent = requiredUpdateKind == UpdateKind.BEFORE_AND_AFTER
    val needUpdateBefore = !allJoinKeyInUniq || generateNull && inputModifyContainsUpdate
    if (needUpdateBefore || requiredUpdateBeforeByParent) {
      beforeAfterOrNone(inputModifyContainsUpdate)
    } else {
      onlyAfterOrNone(inputModifyContainsUpdate)
    }
  }

  def main(arg: Array[String]): Unit = {
    inputUniqueKeyContainsJoinKey()
    val allUpdateKind = Array(UpdateKind.BEFORE_AND_AFTER, UpdateKind.ONLY_UPDATE_AFTER, UpdateKind.NONE)
    for (joinKeyAllInUniqKey <- Array(true, false)) {
      for (inputModifyContainsUpdate <- Array(true, false)) {
        for (updateKind <- allUpdateKind) {
          val init = testInit(joinKeyAllInUniqKey, inputModifyContainsUpdate, updateKind)
          val afterHotFix = testAfterHotFix(joinKeyAllInUniqKey, inputModifyContainsUpdate, updateKind)
          val byted = testByted(joinKeyAllInUniqKey, inputModifyContainsUpdate, updateKind)
          println(joinKeyAllInUniqKey, inputModifyContainsUpdate, updateKind,
            init, afterHotFix, byted)
        }
      }
    }
  }

  object UpdateKind extends Enumeration {
    type UpdateKind = Value
    val

    /** NONE doesn't represent any kind of update operation. */
    NONE,

    ONLY_UPDATE_AFTER,

    BEFORE_AND_AFTER = Value
  }
}
