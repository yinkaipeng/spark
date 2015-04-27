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
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, TimelineQueryClient}
import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryProvider}
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
    awaitEventsProcessed(historyService, 1, 2000)
    flushHistoryServiceToSuccess()
    historyService.stop()
    awaitEmptyQueue(historyService, 5000)
    describe("reading events back")


    val clientConfig = JerseyBinding.createClientConfig()
    val jerseyClient = JerseyBinding.createJerseyClient(sparkCtx.hadoopConfiguration, clientConfig)
    val queryClient = new TimelineQueryClient(timeline, jerseyClient)

    // list all entries
    val entities = queryClient.listEntities(ENTITY_TYPE)
    assertResult(1, "number of listed entities") { entities.size }
    assertResult(1, "entities listed by app start filter") {
      queryClient.listEntities(ENTITY_TYPE,
                primaryFilter = Some(FILTER_APP_START, FILTER_APP_START_VALUE)
                              ).size
    }
    assertResult(1, "entities listed by app end filter") {
      queryClient.listEntities(ENTITY_TYPE,
                primaryFilter = Some(FILTER_APP_END, FILTER_APP_END_VALUE)
                              ).size
    }


    // here the events should be in the system
    val provider = new YarnHistoryProvider(sparkCtx.conf)
    val history = provider.getListing()
    assertResult(1, "size of history") {
      history.size
    }
    val info = history.head
    logInfo(s"App history = $info")
    // validate received data matches that saved
    assertResult(started.sparkUser, s"username in $info") {
      info.sparkUser
    }
    assertResult(started.time, s"time in $info") {
      info.startTime
    }
    assertResult(appId.toString, s"app ID in $info") {
      info.id
    }
    assertResult(started.appName, s"application name in $info") {
      info.name
    }
    val appUI = provider.getAppUI(info.id)

    val timelineEntity= queryClient.getEntity(ENTITY_TYPE, info.id)
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

  }


}
