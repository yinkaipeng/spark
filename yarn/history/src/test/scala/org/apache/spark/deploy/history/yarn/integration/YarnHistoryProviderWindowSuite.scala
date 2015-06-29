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
package org.apache.spark.deploy.history.yarn.integration

import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.ApplicationHistoryInfo
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.{TimeManagedHistoryProvider, HandleSparkEvent, HistoryServiceNotListeningToSparkContext, StubApplicationId, TimelineSingleEntryBatchSize, YarnHistoryService}
import org.apache.spark.util.Utils

/**
 * check windowed providder.
 *
 * More than one history service is started here, each publishing their own events, with
 * their own app ID. For this to work they are set up to not listen to context events.
 */
class YarnHistoryProviderWindowSuite
    extends AbstractTestsWithHistoryServices
    with HistoryServiceNotListeningToSparkContext
    with TimelineSingleEntryBatchSize {

  val appId1 = "app1"
  val appId2 = "app2"
  val user = Utils.getCurrentUserName()

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.setAppName(APP_NAME)
  }

  test("Windowed publishing") {
    describe("Windowed publishing across apps")
    var history2: YarnHistoryService = null
    var provider: TimeManagedHistoryProvider = null
    try {
      historyService = startHistoryService(sparkCtx)
      assert(!historyService.listening, s"listening $historyService")
      assert(historyService.bondedToATS, s"not bonded to ATS: $historyService")
      // post in an app start
      var flushes = 0
      logDebug("posting app start")
      val start1Time = 100
      val start1 = appStartEvent(start1Time, appId1, user)
      historyService.enqueue(new HandleSparkEvent(start1, start1Time))
      flushHistoryServiceToSuccess()
      awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)

      describe("application 2")
      // the second application starts then stops after the first one
      val applicationId2 = new StubApplicationId(2, 2L)
      history2 = startHistoryService(sparkCtx, applicationId2)
      val start2Time = start1Time + 1
      val start2 = appStartEvent(start2Time, appId2, user)
      history2.enqueue(new HandleSparkEvent(start2, start2Time))
      val end2Time = start2Time + 1
      val end2 = appStopEvent(end2Time)
      history2.enqueue(new HandleSparkEvent(end2, end2Time))
      // stop the second application
      history2.stop()
      flushHistoryServiceToSuccess(history2)
      history2 = null

      // here there is one incomplete application, and a completed one
      // which started and stopped after the incomplete one started
      /*

      historyService.stop()
      sparkCtx.stop()
      flushHistoryServiceToSuccess()

*/


      // now read it in via history provider
      describe("read in listing")

      provider = new TimeManagedHistoryProvider(sparkCtx.conf, end2Time, 1)
      val listing1 = awaitListingSize(provider, 2, TEST_STARTUP_DELAY)
      logInfo(s"Listing 1: $listing1")
      assertCompleted(lookup(listing1, applicationId2), s"app2 in listing1 $listing1")
      val applicationInfo1_1 = lookup(listing1, applicationId)
      assert(!applicationInfo1_1.completed, s"$applicationInfo1_1 completed in L1 $listing1")

      describe("stop application 1")

      // now stop application 1
      val end3Time = provider.tick()
      val end3 = appStopEvent(end3Time)
      historyService.enqueue(new HandleSparkEvent(end3, end3Time))
      historyService.stop()
      flushHistoryServiceToSuccess()

      // move time forwards
      provider.incrementTime(5)
      // Now await a refresh
      describe("read in listing #2")

      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)
      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)

      val allApps = provider.listApplications()
      logInfo(s"allApps : ${allApps.applications}")

      // get a new listing
      val listing2 = provider.getListing()
      logInfo(s"Listing 2: $listing2")
      // which had better be updated or there are refresh problems
      assert(listing1 !== listing2, "updated listing was unchanged")
      // get the updated value and expect it to be complete

      assertCompleted(lookup(listing2, applicationId2), s"app2 in L2 $listing2")
      assertCompleted(lookup(listing2, applicationId), s"app1 in L2 $listing2")
      provider.stop()
    } finally {
      describe("teardown")
      ServiceOperations.stopQuietly(history2)
      if (provider != null) {
        provider.stop()
      }
    }

  }


  def lookup(listing: Seq[ApplicationHistoryInfo], id: ApplicationId): ApplicationHistoryInfo = {
    findAppById(listing, id.toString) match {
      case Some(applicationInfo2) =>
        applicationInfo2
      case None =>
        throw new TestFailedException(s"Did not find $id entry in $listing", 4)
    }
  }

  def assertCompleted(info: ApplicationHistoryInfo, text: String = ""): Unit = {
    assert(info.completed, s"$text $info not flagged as completed")
  }
}
