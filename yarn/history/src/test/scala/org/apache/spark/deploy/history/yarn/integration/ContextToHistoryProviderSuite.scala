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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.rest.TimelineQueryClient
import org.apache.spark.deploy.history.yarn.{HandleSparkEvent, HistoryServiceListeningToSparkContext, TimelineSingleEntryBatchSize, YarnHistoryProvider}
import org.apache.spark.util.Utils

/**
 * full hookup from spark context to timeline then reread. This is the big one
 */
class ContextToHistoryProviderSuite
    extends AbstractTestsWithHistoryServices
    with HistoryServiceListeningToSparkContext
    with TimelineSingleEntryBatchSize {


  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.setAppName(APP_NAME)
  }

  test("Stop Event via Context") {
    describe("Stop Event via Context")
    try {
      // hook up to spark context
      historyService = startHistoryService(sparkCtx)
      assert(historyService.listening, s"listening $historyService")
      assertResult(1, s"batch size in $historyService") {
        historyService.getBatchSize
      }
      assert(historyService.bondedToATS, s"not bonded to ATS: $historyService")
      // post in an app start
      var flushes = 0
      logDebug("posting app start")
      val startTime = now()
      val event = appStartEvent(startTime,
                                 sparkCtx.applicationId,
                                 Utils.getCurrentUserName())
      historyService.enqueue(new HandleSparkEvent(event, startTime))
      flushes += 1
      awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)

      // add a local file to generate an update event
      /*
          val tempFile: File = File.createTempFile("test", ".txt")
          sparkCtx.addFile(tempFile.getAbsolutePath)
          tempFile.delete()

      */

      // closing context generates an application stop
      describe("stopping context")
      sparkCtx.stop()
      flushHistoryServiceToSuccess()

      val timeline = historyService.getTimelineServiceAddress()
      val queryClient = new TimelineQueryClient(timeline,
           sparkCtx.hadoopConfiguration, createClientConfig())
      val entities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
      logInfo(s"Entity listing returned ${entities.size} entities")
      entities.foreach { en =>
        logInfo(describeEntityVerbose(en))
      }
      assertResult(1, "number of listed entities (unfiltered)") {
        entities.size
      }
      assertResult(1, "entities listed by app end filter") {
        queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                                  primaryFilter = Some(FILTER_APP_END, FILTER_APP_END_VALUE)
                                ).size
      }

      assertResult(1, "entities listed by app start filter") {
        queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                                  primaryFilter = Some(FILTER_APP_START, FILTER_APP_START_VALUE)
                                ).size
      }


      // now read it in via history provider
      val provider = new YarnHistoryProvider(sparkCtx.conf)
      val history = awaitListingSize(provider, 1, TEST_STARTUP_DELAY)
      val info = history.head
      assert(info.completed, s"application not flagged as completed")
      provider.getAppUI(info.id)
      provider.stop()
    } finally {
      describe("teardown")
    }

  }

}
