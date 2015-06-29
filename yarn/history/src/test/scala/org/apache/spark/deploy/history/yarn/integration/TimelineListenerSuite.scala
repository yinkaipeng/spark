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

  private val appStartFilter = Some(FILTER_APP_START, FILTER_APP_START_VALUE)

  private val appEndFilter = Some(FILTER_APP_END, FILTER_APP_END_VALUE)

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
                primaryFilter = appStartFilter).size
    }
    val timelineEntities =
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                                primaryFilter = appEndFilter)
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
    assert(info.lastUpdated >= info.endTime,
      s"info.lastUpdated  < info.endTime time in $info")
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

  /**
   * This test is like its predecessor except the application is queried while
   * it has not yet completed; we are looking at how that intermediate state is
   * described
   */
  test("Last-Updated time of incompleted app") {
    describe("Last-Updated time of incompleted app")
    // listener is still not hooked up to spark context
    historyService = startHistoryService(sparkCtx)
    val timeline = historyService.getTimelineServiceAddress()
    val listener = new YarnEventListener(sparkCtx, historyService)
    val startTime = now()
    val userName = Utils.getCurrentUserName()
    val started = appStartEvent(startTime,
      sparkCtx.applicationId,
      sparkCtx.sparkUser)
    // initial checks to make sure the event is fully inited
    assert(userName === started.sparkUser, s"started.sparkUser")
    assert(Some(sparkCtx.applicationId) === started.appId, s"started.appId")
    assert(APP_NAME === started.appName, s"started.appName")
    listener.onApplicationStart(started)
    awaitEventsProcessed(historyService, 1, TEST_STARTUP_DELAY)
    flushHistoryServiceToSuccess()

    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
    describe("reading events back")
    val queryClient = createTimelineQueryClient()

    // list all entries
    val entities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
    assertResult(1, "number of listed entities") { entities.size }
    val timelineEntities = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
        primaryFilter = appStartFilter)
    assertResult(1, "entities listed by app start filter") {
      timelineEntities.size
    }
    assertResult(0, "entities listed by app end filter") {
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
        primaryFilter = appEndFilter).size
    }
    val yarnAppId = applicationId.toString()
    val entry = timelineEntities.head
    assertResult(yarnAppId, s"no entry of id $yarnAppId") {
      entry.getEntityId
    }

    // first grab the initial entity and extract it manually
    // this helps isolate any unmarshalling problems
    val entity = queryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, yarnAppId)
    val entityDetails = describeEntity(entity)
    logInfo(s"Timeline Event = $entityDetails")
    logInfo(s"Timeline Event = ${eventDetails(entity) }")
    val unmarshalledEntity = toApplicationHistoryInfo(entity)
    assert(started.appName === unmarshalledEntity.name,
      s"unmarshalledEntity.name != started.appName in $unmarshalledEntity")
    assert(userName === unmarshalledEntity.sparkUser,
      s"unmarshalledEntity.sparkUser != username in $unmarshalledEntity")

    // here the events should be in the system
    val provider = new YarnHistoryProvider(sparkCtx.conf)
    val history = awaitListingSize(provider, 1, TEST_STARTUP_DELAY)
    val info = history.head

    logInfo(s"App history = $info")

    // validate received data matches that saved

    assertResult(startTime, s"started.time != startTime") {
      started.time
    }
    assertResult(started.time, s"info.startTime != started.time in $info") {
      info.startTime
    }
    assert(yarnAppId === info.id, s"info.id != yarnAppId in $info")
    assert(info.endTime === 0, s"end time != 0 in $info")
    // on a completed app, lastUpdated is the end time
    assert(info.lastUpdated > 0, s"info.lastUpdated zero time in $info")
    assert(started.appName === info.name, s"info.name != started.appName in $info")
    assert(userName === info.sparkUser, s"info.sparkUser != username in $info")
    val appUI = provider.getAppUI(info.id)

    val timelineEntity = queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, info.id)
    val events = timelineEntity.getEvents.asScala.toList
    assert(1 === events.size, s"number of events in ${describeEntity(timelineEntity) }")
    // first event must be the start one
    val sparkListenerEvents = events.map(toSparkEvent(_)).reverse
    val (firstEvent :: Nil) = sparkListenerEvents
    val fetchedStartEvent = firstEvent.asInstanceOf[SparkListenerApplicationStart]
    assert(started.time === fetchedStartEvent.time, "start time")

    // direct retrieval
    val entity2 = queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, yarnAppId)

  }


}
