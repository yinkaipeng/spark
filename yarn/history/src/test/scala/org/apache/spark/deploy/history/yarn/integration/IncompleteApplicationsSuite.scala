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
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryProvider, YarnHistoryService}
import org.apache.spark.scheduler.cluster.YarnExtensionServices
import org.apache.spark.util.Utils

/**
 * Test handling/logging of incomplete applications.
 *
 * This implicitly tests some of the windowing logic. Specifically, do completed
 * applications get picked up?
 */
class IncompleteApplicationsSuite extends AbstractTestsWithHistoryServices {

  val EVENT_PROCESSED_TIMEOUT = 2000

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
  }


  test("WebUI incomplete view") {
    def checkEmptyIncomplete(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val connector = createUrlConnector()
      val url = new URL(webUI, "/?" + page1_incomplete_flag)
      val incompleted = connector.execHttpOperation("GET", url, null, "")
      val body = incompleted.responseBody
      logInfo(s"$url => $body")
      assertContains(body, no_incomplete_applications, s"In $url")
    }

    webUITest("incomplete view", checkEmptyIncomplete)
  }

  test("Publish Events and GET the web UI") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val connector = createUrlConnector()
      val incompleteURL = new URL(webUI, "/?" + incomplete_flag)
      awaitURL(incompleteURL, TEST_STARTUP_DELAY)

      def listIncompleteApps(): String = {
        connector.execHttpOperation("GET", incompleteURL, null, "").responseBody
      }
      historyService = startHistoryService(sparkCtx)
      val timeline = historyService.getTimelineServiceAddress()
      val listener = new YarnEventListener(sparkCtx, historyService)
      // initial view has no incomplete applications
      assertContains(listIncompleteApps(), no_incomplete_applications,
        "initial incomplete page is empty")

      val startTime = now()

      val started = appStartEvent(startTime,
                                   sparkCtx.applicationId,
                                   Utils.getCurrentUserName())
      listener.onApplicationStart(started)
      val jobId = 2
      listener.onJobStart(jobStartEvent(startTime + 1, jobId))
      awaitEventsProcessed(historyService, 1, EVENT_PROCESSED_TIMEOUT)
      flushHistoryServiceToSuccess()

      // await for a  refresh

      // listing
      val incompleteListing = awaitListingSize(provider, 1, EVENT_PROCESSED_TIMEOUT)

      val queryClient = createTimelineQueryClient()

      // check for work in progress
      assertDoesNotContain(listIncompleteApps(), no_incomplete_applications,
        "'incomplete application' list empty")

      logInfo("Ending job and application")
      //job completion event
      listener.onJobEnd(jobSuccessEvent(startTime + 1, jobId))
      //stop the app
      historyService.stop()
      awaitEmptyQueue(historyService, EVENT_PROCESSED_TIMEOUT)
      val yarnAppId = applicationId.toString()
      // validate ATS has it
      val timelineEntities =
        queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                                  primaryFilter = Some(FILTER_APP_END, FILTER_APP_END_VALUE))
      assertResult(1, "entities listed by app end filter") {
        timelineEntities.size
      }
      val entry = timelineEntities.head
      assertResult(yarnAppId, s"no entry of id $yarnAppId") {
        entry.getEntityId
      }

      val entity = queryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, yarnAppId)

      // at this point the REST UI is happy. Check the provider level

      // listing
      val history = awaitListingSize(provider, 1, EVENT_PROCESSED_TIMEOUT)

      //and look for the complete app

      awaitURL(webUI, EVENT_PROCESSED_TIMEOUT)
      val completeBody = awaitURLDoesNotContainText(connector, webUI,
           no_completed_applications, EVENT_PROCESSED_TIMEOUT)
      // look for the link
      assertContains(completeBody, s"${yarnAppId}</a>")

      val appPath = s"/history/${yarnAppId}"
      // GET the app
      val appURL = new URL(webUI, appPath)
      val appUI = connector.execHttpOperation("GET", appURL, null, "")
      val appUIBody = appUI.responseBody
      logInfo(s"Application\n$appUIBody")
      assertContains(appUIBody, APP_NAME)
      // look for the completed job
      assertContains(appUIBody, completedJobsMarker)
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/jobs"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/stages"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/storage"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/environment"), null, "")
      connector.execHttpOperation("GET", new URL(appURL, s"$appPath/executors"), null, "")


      // resolve to entry
      val appUIwrapper = provider.getAppUI(yarnAppId)
      appUIwrapper match {
        case Some(yarnAppUI) =>
        // success
        case None => fail(s"Did not get a UI for $yarnAppId")
      }


      // final view has no incomplete applications
      assertContains(listIncompleteApps, no_incomplete_applications)

    }

    webUITest("submit and check", submitAndCheck)
  }

}
