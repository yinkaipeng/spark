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
package org.apache.spark.deploy.history.yarn.unit

import org.scalatest.FunSuite

import org.apache.spark.Logging
import org.apache.spark.deploy.history.ApplicationHistoryInfo
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.{ExtraAssertions, YarnTimelineUtils}

/**
 * Test of utility methods in [[YarnTimelineUtils]]
 */
class TimelineUtilsSuite extends FunSuite with Logging with ExtraAssertions {

  def historyInfo(id: String, started: Long, ended: Long, complete: Boolean):
      ApplicationHistoryInfo = {
    new ApplicationHistoryInfo(id, id, started, ended, Math.max(started, ended),
      "user", complete)
  }

  val h12 = historyInfo("h12",1,2, true)
  val h22 = historyInfo("h22",2,2, true)
  val i20 = historyInfo("i20",2,0, false)
  val i30 = historyInfo("i30",3,0, false)
  val h33 = historyInfo("h30",3,3, true)
  val h44 = historyInfo("h44",4,4, true)
  val iA10 = historyInfo("iA", 1, 0, false)
  val iA11 = historyInfo("iA", 1, 1, true)

  test("timeShort") {
    assert("unset" === timeShort(0, "unset"))
    assert("unset" !== timeShort(System.currentTimeMillis(), "unset"))
  }

  test("findOldest") {
    assert(Some(h12) === findOldestApplication(List(h12, h22, i20)))
  }

  test("findOldest-2") {
    assert(Some(h22) === findOldestApplication(List(h44, h22, i20)))
  }

  test("findOldest-3") {
    assert(Some(i20) === findOldestApplication(List(h44, h33, i20)))
  }

  test("findOldest-4") {
    assert(None === findOldestApplication(Nil))
  }

  test("findIncomplete") {
    assert(List(i20, i30) === findIncompleteApplications(List(h44, i20, i30, h33)))
  }

  test("findIncomplete-2") {
    assert(Nil === findIncompleteApplications(Nil))
  }

  test("findIncomplete-3") {
    assert(Nil === findIncompleteApplications(List(h44,h33)))
  }

  test("findStartOfWindow") {
    assert(Some(i20) === findStartOfWindow(List(h44, i20, i30, h33)))
  }

  test("findStartOfWindow-2") {
    assert(Some(h44) === findStartOfWindow(List(h44, h12, h33)))
  }

  test("combineResults") {
    assert((h44 :: Nil) === combineResults(List(h44), Nil))
  }

  test("combineResults-2") {
    assert((h44 :: Nil) === combineResults(Nil, List(h44)))
  }

  test("combineResults-3") {
    assert(Nil === combineResults(Nil, Nil))
  }
  test("combineResults-5") {
    assert((h44 :: i20 :: Nil) === combineResults(List(h44), List(i20)))
  }

  test("combineResults-6") {
    assert((h44 :: Nil) === combineResults(List(h44), List(h44)))
  }

  test("combineResults-7") {
    assert((iA11 :: Nil) === combineResults(List(iA10), List(iA11)))
  }

  test("SortApplications-1") {
    assert((h33 :: h44 :: Nil) == sortApplicationsByStartTime(List(h44, h33)))
  }

  test("SortApplications-2") {
    assert((h22 :: i20 :: h33 :: Nil) == sortApplicationsByStartTime(List(h22, i20, h33)))
  }

  test("SortApplications-3") {
    assert((i20 :: h22 :: Nil) == sortApplicationsByStartTime(List(i20, h22)))
  }

  test("findLatest") {
    assert(Some(h22) === findLatestApplication(List(h12, h22, i20)))
  }

  test("findLatest-2") {
    assert(Some(h22) === findLatestApplication(List(h22, i20)))
  }

  test("findLatest-3") {
    assert(Some(i20) === findLatestApplication(List(h12, i20)))
  }

}
