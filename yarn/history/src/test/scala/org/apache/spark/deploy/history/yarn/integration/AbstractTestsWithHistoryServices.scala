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

import java.net.{Socket, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
import org.apache.hadoop.service.{Service, ServiceOperations}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer

import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.rest.{SpnegoUrlConnector, TimelineQueryClient}
import org.apache.spark.deploy.history.yarn.{AbstractYarnHistoryTests, FreePortFinder, HandleSparkEvent, HistoryServiceNotListeningToSparkContext, TimelineServiceEnabled, YarnHistoryProvider, YarnHistoryService, YarnTimelineUtils}
import org.apache.spark.deploy.history.{ApplicationHistoryProvider, FsHistoryProvider, HistoryServer}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.{SecurityManager, SparkConf}

/**
 * Integration tests with history services setup and torn down
 */
abstract class AbstractTestsWithHistoryServices
    extends AbstractYarnHistoryTests
    with FreePortFinder
    with HistoryServiceNotListeningToSparkContext
    with TimelineServiceEnabled {

  protected var _applicationHistoryServer: ApplicationHistoryServer = _
  protected var _timelineClient: TimelineClient = _
  protected var historyService: YarnHistoryService = _

  protected val incomplete_flag = "&showIncomplete=true"
  protected val page1_flag = "&page=1"
  protected val page1_incomplete_flag = "&page=1&showIncomplete=true"



  protected val no_completed_applications = "No completed applications found!"
  protected val no_incomplete_applications = "No incomplete applications found!"

  def applicationHistoryServer: ApplicationHistoryServer = {
    _applicationHistoryServer
  }

  def timelineClient: TimelineClient = {
    _timelineClient
  }

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
    describe("Teardown of history server, timeline client and history service")
    if (historyService != null && !historyService.isInState(Service.STATE.STARTED)) {
      flushHistoryServiceToSuccess()
      spinForState("post thread halting in teardown",
                    interval = 50,
                    timeout = TEST_STARTUP_DELAY,
                    probe = (() => outcomeFromBool(!historyService.isPostThreadActive)),
                    failure = (_, _, _) => ())
      ServiceOperations.stopQuietly(historyService)
      awaitServiceThreadStopped(historyService, TEST_STARTUP_DELAY)
    }
    ServiceOperations.stopQuietly(_applicationHistoryServer)
    ServiceOperations.stopQuietly(_timelineClient)
    super.teardown()
  }

  /**
   * Create a SPNEGO-enabled URL Connector
   * @return a URL connector for issuing HTTP requests
   */
  protected def createUrlConnector(): SpnegoUrlConnector = {
    SpnegoUrlConnector.newInstance(sparkCtx.hadoopConfiguration,
      new DelegationTokenAuthenticatedURL.Token)
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
    awaitURL(endpoint.toURL, TEST_STARTUP_DELAY)
  }


  protected def createTimelineQueryClient(): TimelineQueryClient = {
    val timeline = historyService.getTimelineServiceAddress()
    new TimelineQueryClient(timeline,
                       sparkCtx.hadoopConfiguration,
                       createClientConfig())
  }

  /**
   * Put a timeline entity to the timeline client; this is expected
   * to eventually make it to the history server
   * @param entity entity to put
   * @return the response
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
    val description = describePutResponse(response)
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
    flushHistoryServiceToSuccess(historyService)
  }

  /**
   * Flush a history service to success
   * @param history service to flush
   * @param delay time to wait for an empty queue
   */
  def flushHistoryServiceToSuccess(history: YarnHistoryService, delay: Int= TEST_STARTUP_DELAY):
  Unit = {
    assertNotNull(history, "null history queue")
    historyService.asyncFlush()
    awaitEmptyQueue(history, delay)
    assertResult(0, s"Post failure count: $history") {
      history.getEventPostFailures
    }
  }

  /**
   * Create a history provider instance, following the same process
   * as the history web UI itself: querying the configuration for the
   * provider and falling back to the [[FsHistoryProvider]]. If
   * that falback does take place, however, and assertion is raised.
   * @param conf configuration
   * @return the instance
   */
  protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val providerName = conf.getOption("spark.history.provider")
        .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = Class.forName(providerName)
        .getConstructor(classOf[SparkConf])
        .newInstance(conf)
        .asInstanceOf[ApplicationHistoryProvider]
    assert(provider.isInstanceOf[YarnHistoryProvider],
            s"Instantiated $providerName to get $provider")

    provider.asInstanceOf[YarnHistoryProvider]
  }

  /**
   * Crete a history server and maching provider, execute the
   * probe against it. After the probe completes, the history server
   * is stopped.
   * @param probe probe to run
   */
  def webUITest(name: String, probe: (URL, YarnHistoryProvider) => Unit) {
    val s = new Socket()
    val (port, server, webUI, provider) = createHistoryServer(findPort())
    try {
      server.bind()
      describe(name)
      probe(webUI, provider)
    } finally {
      describe("stopping history service")
      try {
        server.stop()
      } catch {
        case e: Exception =>
          logInfo(s"In History Server teardown: $e", e)
      }
    }
  }

  /**
   * Probe the empty web UI for not having any completed apps; expect
   * a text/html response with specific text and history provider configuration
   * elements.
   * @param webUI web UI
   * @param provider provider
   */
  def probeEmptyWebUI(webUI: URL, provider: YarnHistoryProvider): String = {
    val body: String = getHtmlPage(webUI,
       "<title>History Server</title>"
        :: no_completed_applications
        :: YarnHistoryProvider.KEY_PROVIDER_NAME
        :: YarnHistoryProvider.KEY_PROVIDER_DETAILS
        :: Nil)
    logInfo(s"$body")
    body
  }

  /**
   * Get an HTML page. Includes a check that the content type is `text/html`
   * @param page web UI
   * @param checks list of strings to assert existing in the response
   * @return the body of the response
   */
  protected def getHtmlPage(page: URL, checks: List[String]): String = {
    val connector = createUrlConnector()
    val outcome = connector.execHttpOperation("GET", page, null, "")
    logDebug(s"$page => $outcome")
    assert(outcome.contentType.startsWith("text/html"),
            s"content type of $outcome")
    val body = outcome.responseBody
    assertStringsInBody(body, checks)
    body
  }

  /**
   * Assert that a list of checks are in the HTML body
   * @param body body of HTML (or other string)
   * @param checks list of strings to assert are present
   */
  def assertStringsInBody(body: String, checks: List[String]): Unit = {
    var missing: List[String] = Nil
    var text = "[ "
    checks foreach { check =>
        if (!body.contains(check)) {
          missing = check :: missing
          text = text +"\"" + check +"\" "
        }
    }
    text = text +"]"
    if (missing.nonEmpty) {
      fail(s"Did not find $text in\n$body")
    }
  }

  /**
   * Create a [[HistoryServer]] instance with a coupled history provider.
   * @param defaultPort a port to use if the property `spark.history.ui.port` isn't
   *          set in the spark context. (default: 18080)
   * @return (port, server, web UI URL, history provider)
   */
  protected def createHistoryServer(defaultPort: Int = 18080):
  (Int, HistoryServer, URL, YarnHistoryProvider) = {
    val conf = sparkCtx.getConf
    val securityManager = new SecurityManager(conf)
    val args: List[String] = Nil
    val port = conf.getInt("spark.history.ui.port", defaultPort)
    val provider = createHistoryProvider(sparkCtx.getConf)
    val server = new HistoryServer(conf, provider, securityManager, port)
    val webUI = new URL("http", "localhost", port, "/")
    (port, server, webUI, provider)
  }
}
