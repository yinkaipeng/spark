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

import scala.collection.JavaConverters._

import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryProvider, YarnHistoryService}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.apache.spark.util.Utils

/**
 * Set up a listener and feed events in; verify they get through to ATS
 */
class TimelineListenerSuite extends AbstractTestsWithHistoryServices {

  test("Listener Events") {
    describe("Listener events pushed out")
    // listener is still not hooked up to spark context
    historyService = startHistoryService(sparkCtx)
    val timeline = historyService.getTimelineServiceAddress()
    val listener = new YarnEventListener(sparkCtx, historyService)
    val startTime = now()
    val started = appStartEvent(startTime,
                               sparkCtx.applicationId,
                               Utils.getCurrentUserName())
    listener.onApplicationStart(started)
    awaitEventsProcessed(historyService, 1, TEST_STARTUP_DELAY)
    flushHistoryServiceToSuccess()
    historyService.stop()
    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
    describe("reading events back")


    val queryClient = createTimelineQueryClient()

    // list all entries
    val entities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
    assertResult(1, "number of listed entities") { entities.size }
    assertResult(1, "entities listed by app start filter") {
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                primaryFilter = Some(FILTER_APP_START, FILTER_APP_START_VALUE)
                              ).size
    }
    val timelineEntities =
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                                primaryFilter = Some(FILTER_APP_END, FILTER_APP_END_VALUE))
    assertResult(1, "entities listed by app end filter") {
      timelineEntities.size
    }
    val yarnAppId = applicationId.toString()
    val entry = timelineEntities.head
    assertResult(yarnAppId, s"no entry of id $yarnAppId") {
      entry.getEntityId
    }
    val entity = queryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, yarnAppId)

    // here the events should be in the system
    val provider = new YarnHistoryProvider(sparkCtx.conf)
    val history = awaitListingSize(provider, 1, TEST_STARTUP_DELAY)
    val info = history.head
    logInfo(s"App history = $info")
    // validate received data matches that saved
    assertResult(started.sparkUser, s"username in $info") {
      info.sparkUser
    }
    assertResult(startTime, s"started.time != startTime") {
      started.time
    }
    assertResult(started.time, s"info.startTime != started.time in $info") {
      info.startTime
    }
    assertResult(yarnAppId, s"info.id != yarnAppId in $info") {
      info.id
    }
    assert(info.endTime> 0, s"end time is 0 in $info")
    // on a completed app, lastUpdated is the end time
    assertResult(info.endTime, s"info.lastUpdated != info.endTime time in $info") {
      info.lastUpdated
    }
    assertResult(started.appName, s"info.name != started.appName in $info") {
      info.name
    }
    val appUI = provider.getAppUI(info.id)

    val timelineEntity= queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, info.id)
    val events = timelineEntity.getEvents.asScala.toList
    assertResult(2, s"number of events in ${describeEntity(timelineEntity)}") {
      events.size
    }
    // first event must be the start one
    val sparkListenerEvents = events.map(toSparkEvent(_)).reverse
    val (firstEvent :: secondEvent :: Nil)  = sparkListenerEvents
    val fetchedStartEvent = firstEvent.asInstanceOf[SparkListenerApplicationStart]
    val fetchedEndEvent = secondEvent.asInstanceOf[SparkListenerApplicationEnd]
    assertResult(started.time, "start time") { fetchedStartEvent.time}

    // direct retrieval
    val entity2 = queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, yarnAppId)

  }


}
