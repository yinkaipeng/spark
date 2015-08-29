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

import java.net.URL

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{YarnTimelineUtils, YarnEventListener, YarnHistoryProvider, YarnHistoryService}
import org.apache.spark.scheduler.cluster.YarnExtensionServices
import org.apache.spark.util.Utils

/**
 * test to see that incomplete spark UIs are handled
 */
class IncompleteSparkUISuite extends AbstractTestsWithHistoryServices {


  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
  }


  test("incomplete UI must not be cached") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val connector = createUrlConnector()
      val incompleteAppsURL = new URL(webUI, "/?" + incomplete_flag)
      def listIncompleteApps: String = {
        connector.execHttpOperation("GET",
          incompleteAppsURL, null, "").responseBody
      }
      historyService = startHistoryService(sparkCtx)
      val timeline = historyService.getTimelineServiceAddress()
      val listener = new YarnEventListener(sparkCtx, historyService)
      // initial view has no incomplete applications
      assertContains(listIncompleteApps, no_incomplete_applications)

      val startTime = now()

      val started = appStartEvent(startTime,
                                   sparkCtx.applicationId,
                                   Utils.getCurrentUserName())
      listener.onApplicationStart(started)
      val jobId = 2
      listener.onJobStart(jobStartEvent(startTime + 1 , jobId))
      awaitEventsProcessed(historyService, 1, 2000)
      flushHistoryServiceToSuccess()

      // await for a  refresh

      // listing
      val incompleteListing = awaitListingSize(provider, 1, TEST_STARTUP_DELAY)

      // here we can do a GET of the application history and expect to see something incomplete

      val queryClient = createTimelineQueryClient()

      // check for work in progress
      awaitURLDoesNotContainText(connector, incompleteAppsURL,
        no_completed_applications, TEST_STARTUP_DELAY, "expecting incomplete app listed")


      val yarnAppId = applicationId.toString()

      val appPath = s"/history/${yarnAppId}"
      // GET the app
      val appURL = new URL(webUI, appPath)
      val appUIBody = connector.execHttpOperation("GET", appURL, null, "").responseBody
      assertContains(appUIBody, APP_NAME, s"application name in $appURL")
      // look for active jobs marker
      assertContains(appUIBody, activeJobsMarker, s"active jobs string in $appURL")

      // resolve to entry
      val appUIwrapper = provider.getAppUI(yarnAppId)
      appUIwrapper match {
        case Some(yarnAppUI) =>
        // success
        case None => fail(s"Did not get a UI for $yarnAppId")
      }

      logInfo("Ending job and application")
      //job completion event
      listener.onJobEnd(jobSuccessEvent(startTime + 1, jobId))
      //stop the app
      historyService.stop()
      awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)

      flushHistoryServiceToSuccess()

      // spin for a refresh event
      awaitRefreshExecuted(provider, true, TEST_STARTUP_DELAY)

      // root web UI declares it complete
      val completeBody = awaitURLDoesNotContainText(connector, webUI,
        no_completed_applications, TEST_STARTUP_DELAY,
        s"expecting application listed as completed")

      // final view has no incomplete applications
      assertContains(listIncompleteApps, no_incomplete_applications,
        "incomplete applications still in list view")

      // the underlying timeline entity
      val entity = provider.getTimelineEntity(yarnAppId)

      val history = awaitListingSize(provider, 1, TEST_STARTUP_DELAY).head

      val historyDescription = YarnTimelineUtils.describeApplicationHistoryInfo(history)
      assert(1 === history.attempts.size, "wrong number of app attempts ")
      val attempt1 = history.attempts.head
      assert(attempt1.completed,
        s"application attempt considered incomplete: ${historyDescription}")

      // get the final app UI
      val finalAppUIPage = connector.execHttpOperation("GET", appURL, null, "").responseBody
      assertContains(finalAppUIPage, APP_NAME, s"Application name $APP_NAME not found at $appURL")
      // the active jobs section must no longer exist
/* DISABLED. SPARK-7889
      assertDoesNotContain(finalAppUIPage, activeJobsMarker,
        s"Web UI $appURL still declared active")

      // look for the completed job
      assertContains(finalAppUIPage, completedJobsMarker,
        s"Web UI $appURL does not declare completed jobs")
*/
    }
    webUITest("submit and check", submitAndCheck)
  }

}
