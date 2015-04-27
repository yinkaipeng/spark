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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.service.{Service, ServiceOperations}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer
import org.apache.hadoop.yarn.server.timeline.TimelineStore

import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.{AbstractYarnHistoryTests, FreePortFinder, HistoryServiceNotListeningToSparkContext, TimelineServiceEnabled, HandleSparkEvent, YarnHistoryService, YarnTimelineUtils}
import org.apache.spark.scheduler.SparkListenerEvent

/**
 * Integration tests with history services setup and torn down
 */
abstract class AbstractTestsWithHistoryServices
    extends AbstractYarnHistoryTests
    with FreePortFinder
    with HistoryServiceNotListeningToSparkContext
    with TimelineServiceEnabled {

  private var _applicationHistoryServer: ApplicationHistoryServer = _;
  private var _timelineClient: TimelineClient = _
  protected var historyService: YarnHistoryService = _

  def applicationHistoryServer: ApplicationHistoryServer = _applicationHistoryServer
  def timelineClient: TimelineClient = _timelineClient

  /*
   * Setup phase creates a local ATS server and a client of it
   */

  override def setup(): Unit = {
    // abort the tests if the server is offline
    cancelIfOffline()
    super.setup()
    startTimelineClientAndAHS(sparkCtx.hadoopConfiguration)
  }

  /**
   * Teardown stops all services, including, if set, anything in
   * <code>historyService</code>
   */
  override def teardown(): Unit = {
    logInfo("Teardown of history server, timeline client and history service")
    if (historyService != null && !historyService.isInState(Service.STATE.STARTED)) {
      flushHistoryServiceToSuccess()
      spinForState(50, 5000,
                    (() => outcomeFromBool(!historyService.isPostThreadActive)),
                    (_, _) => ())
      ServiceOperations.stopQuietly(historyService)
      awaitServiceThreadStopped(historyService, 5000)
    }
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    ServiceOperations.stopQuietly(_timelineClient)
    super.teardown()
  }

  /**
   * Get at the timeline store
   * @return timeline store
   */
  protected def timelinestore: TimelineStore = {
    assertNotNull(applicationHistoryServer,"applicationHistoryServer")
    applicationHistoryServer.getTimelineStore
  }

  /**
   * Create the client and the app server
   * @param conf the hadoop configuration
   */
  protected def startTimelineClientAndAHS(conf: Configuration) {
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    ServiceOperations.stopQuietly(_timelineClient)
    _timelineClient = TimelineClient.createTimelineClient
    _timelineClient.init(conf)
    _timelineClient.start
    _applicationHistoryServer = new ApplicationHistoryServer
    _applicationHistoryServer.init(_timelineClient.getConfig)
    _applicationHistoryServer.start()
    // Wait for AHS to come up
    val endpoint = YarnTimelineUtils.timelineWebappUri(conf, "")
    awaitURL(endpoint.toURL, 5000)
  }

  /**
   * Put a timeline entity to the timeline client; this is expected
   * to eventually make it to the history server
   * @param entity
   * @return
   */
  def putTimelineEntity(entity: TimelineEntity): TimelinePutResponse = {
    assertNotNull(_timelineClient, "timelineClient")
    _timelineClient.putEntities(entity)
  }


  /**
   * Marshall and post a spark event to the timeline; return the outcome
   * @param sparkEvt event
   * @param time event time
   * @return a triple of the wrapped event, marshalled entity and the response
   */
  protected def postEvent(sparkEvt: SparkListenerEvent, time: Long):
  (TimelineEvent, TimelineEntity, TimelinePutResponse) = {
    val event = toTimelineEvent(new HandleSparkEvent(sparkEvt, time))
    val entity = newEntity(time)
    entity.addEvent(event)
    val response = putTimelineEntity(entity)
    val description = describePutResponse(response);
    logInfo(s"response: $description")
    assert(response.getErrors.isEmpty,
            s"errors in response: $description")
    (event, entity, response)
  }

  /**
   * flush the history service of its queue, await it to complete,
   * then assert that there were no failures
   */
  protected def flushHistoryServiceToSuccess(): Unit = {
    assertNotNull(historyService, "null history queue")
    historyService.asyncFlush()
    awaitEmptyQueue(historyService, 5000)
    assertResult(0, s"-Post failure count: $historyService") {
      historyService.getEventPostFailures
    }
  }
}
