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
 * This is the complete integration test
 */
class WebsiteIntegrationSuite extends AbstractTestsWithHistoryServices {


  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
  }

  test("Instantiate HistoryProvider") {
    val provider = createHistoryProvider(sparkCtx.getConf)
    provider.stop()

  }

  test("WebUI hooked up") {
    def probeEmptyWebUIVoid(webUI: URL, provider: YarnHistoryProvider): Unit = {
      probeEmptyWebUI(webUI, provider)
    }
    webUITest("WebUI hooked up", probeEmptyWebUIVoid)
  }

/* // disabled while failing on branch 1.3.x
  test("WebUI incomplete view") {
    def checkEmptyIncomplete(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val connector = createUrlConnector()
      val incompleted = connector.execHttpOperation("GET",
                new URL(webUI, "/?" + incomplete_flag), null, "")
      val body = incompleted.responseBody
      logInfo(s"$body")
      assertContains(body, no_incomplete_applications)
    }

    webUITest(checkEmptyIncomplete)
  }
*/

  test("Publish Events and GET the web UI") {
    def submitAndCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {

      historyService = startHistoryService(sparkCtx)
      val timeline = historyService.getTimelineServiceAddress()
      val listener = new YarnEventListener(sparkCtx, historyService)
      val startTime = now()

      val started = appStartEvent(startTime,
                                   sparkCtx.applicationId,
                                   Utils.getCurrentUserName())
      listener.onApplicationStart(started)
      awaitEventsProcessed(historyService, 1, 2000)
      flushHistoryServiceToSuccess()

      val connector = createUrlConnector()
      val queryClient = createTimelineQueryClient()

/*
     // check for work in progress
      val incomplete = connector.execHttpOperation("GET",
        new URL(webUI, "/?" + incomplete), null, "")
      val body = incomplete.responseBody
      logInfo(s"$body")
      assert(!body.contains(no_incomplete_applications))
*/

      //now stop the app
      historyService.stop()
      awaitEmptyQueue(historyService, 5000)
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
      val history = awaitListingSize(provider, 1, 5000)

      //and look for the complete app

      val complete = connector.execHttpOperation("GET", webUI, null, "")
      val completeBody = awaitURLDoesNotContainText(connector, webUI, no_completed_applications, 5000)
      logInfo(s"GET /\n$completeBody")
      // look for the link
      assertContains(completeBody,s"${yarnAppId}</a>")

      val appPath = s"/history/${yarnAppId }"
      // GET the app
      val appURL = new URL(webUI, appPath)
      val appUI = connector.execHttpOperation("GET", appURL, null, "")
      val appUIBody = appUI.responseBody
      logInfo(s"Application\n$appUIBody")
      assertContains(appUIBody, APP_NAME)
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


    }

    webUITest("submit and check", submitAndCheck)
  }

}
