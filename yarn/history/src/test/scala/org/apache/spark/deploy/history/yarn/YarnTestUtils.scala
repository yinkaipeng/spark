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
package org.apache.spark.deploy.history.yarn

import java.io.IOException
import java.net.URL

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.ApplicationHistoryInfo
import org.apache.spark.deploy.history.yarn.rest.SpnegoUrlConnector
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEnvironmentUpdate, SparkListenerEvent}
import org.apache.spark.util.Utils

object YarnTestUtils extends ExtraAssertions with FreePortFinder {

  val environmentUpdate = SparkListenerEnvironmentUpdate(Map[String, Seq[(String, String)]](
    "JVM Information" -> Seq(("GC speed", "9999 objects/s"), ("Java home", "Land of coffee")),
    "Spark Properties" -> Seq(("Job throughput", "80000 jobs/s, regardless of job type")),
    "System Properties" -> Seq(("Username", "guest"), ("Password", "guest")),
    "Classpath Entries" -> Seq(("Super library", "/tmp/super_library"))))

  val applicationId: ApplicationId = new StubApplicationId(1, 1L)
  val applicationStart = SparkListenerApplicationStart("YarnTestUtils",
                Some(applicationId.toString), 42L, "bob")
  val applicationEnd = SparkListenerApplicationEnd(84L)

  /**
   * Time to wait for anything to start/state to be reached
   */
  val TEST_STARTUP_DELAY = 5000

  /**
   * Cancel a test if the network isn't there.
   * If called during setup, this will abort the test
   */
  def cancelIfOffline(): Unit = {

    try { {
      val hostname = Utils.localHostName()
      log.debug(s"local hostname is $hostname")
    }
    }
    catch {
      case ex: IOException => {
        cancel(s"Localhost name not known: $ex", ex)
      }
    }
  }


  /**
   * Return a time value
   * @return a time
   */
  def now(): Long = {
    System.currentTimeMillis()
  }

  /**
   * Get a time in the future
   * @param millis future time in millis
   * @return now + the time offset
   */
  def future(millis: Long): Long = {
    now() + millis
  }

  /**
   * Log an entry with a line either side. This aids
   * splitting up tests from the noisy logs
   * @param text text to log
   */
  def describe(text: String): Unit = {
    logInfo(s"\nTest:\n  $text\n\n")
  }

  /**
   * Set a hadoop opt in the config.
   * This adds the "spark.hadoop." prefix to all entries which
   * do not already have it
   * @param sparkConfig target configuration
   * @param key hadoop option key
   * @param value value
   */
  def hadoopOpt(sparkConfig: SparkConf, key: String, value: String): SparkConf = {
    if (key.startsWith("spark.hadoop.")) {
      sparkConfig.set(key, value)
    } else {
      sparkConfig.set("spark.hadoop." + key, value)
    }
  }

  /**
   * Bulk set of an entire map of Hadoop options
   * @param sparkConfig target configuration
   * @param options option map
   */
  def applyHadoopOptions(sparkConfig: SparkConf, options: Map[String, String]): SparkConf = {
    options.foreach( e => hadoopOpt(sparkConfig, e._1, e._2))
    sparkConfig
  }

  /**
   * Apply the basic timeline options to the hadoop config
   * @return the modified config
   */
  def addBasicTimelineOptions(sparkConf: SparkConf): SparkConf = {
    applyHadoopOptions(sparkConf,
      Map(YarnConfiguration.TIMELINE_SERVICE_ENABLED -> "true",
         YarnConfiguration.TIMELINE_SERVICE_ADDRESS -> findIPv4AddressAsPortPair(),
         YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS -> findIPv4AddressAsPortPair(),
         YarnConfiguration.TIMELINE_SERVICE_STORE -> classOf[MemoryTimelineStore].getName,
         YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES -> "1",
         YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS -> "200"))
    // check for updates every second
    sparkConf.set(YarnHistoryProvider.OPTION_UPDATE_INTERVAL, "1")
  }

  /**
   * Convert the single timeline event in a timeline entity to a spark event
   * @param entity
   * @return
   */
  def convertToSparkEvent(entity: TimelineEntity): SparkListenerEvent = {
    assertResult(1, "-wrong # of events in the timeline entry") {
      entity.getEvents().size()
    }
    YarnTimelineUtils.toSparkEvent(entity.getEvents().get(0))
  }

  /**
   * Application name used in the app start event and tests
   */
  val APP_NAME = "spark-demo"

  /**
   * User submitting the job
   */
  val APP_USER = "data-scientist"

  /**
   * ID for events
   */
  val EVENT_ID = "eventId-0101"
  /**
   * Spark option to set for the history provider
   */
  val SPARK_HISTORY_PROVIDER = "spark.history.provider"

  /**
   * Constant used to define history port in Spark `HistoryServer` class
   */
  val SPARK_HISTORY_UI_PORT = "spark.history.ui.port"

  /**
   * Create an app start event, using the fixed [[APP_NAME]] and [[APP_USER]] values
   * for appname and user
   * @param time application start time
   * @param eventId event ID; default is [[EVENT_ID]]
   * @return the event
   */
  def appStartEvent(time: Long = 1,
      eventId: String = EVENT_ID,
      user:String = APP_USER): SparkListenerApplicationStart = {
    new SparkListenerApplicationStart(APP_NAME, Some(eventId), time, user)
  }

  def appStopEvent(time: Long = 1): SparkListenerApplicationEnd = {
    new SparkListenerApplicationEnd(time)
  }

  def newEntity(time: Long): TimelineEntity = {
    val entity = new TimelineEntity
    entity.setStartTime(time)
    entity.setEntityId("post")
    entity.setEntityType(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    entity
  }

  /**
   * Outcomes of probes
   */
  sealed abstract class Outcome
  case class Fail() extends Outcome
  case class Retry() extends Outcome
  case class Success() extends Outcome
  case class TimedOut() extends Outcome

  /**
   * Spin and sleep awaiting an observable state
   * @param interval sleep interval
   * @param timeout time to wait
   * @param probe probe to execute
   * @param failure closure invoked on timeout/probe failure
   */
  def spinForState(description: String, interval: Long, timeout: Long, probe: () => Outcome, failure: (Int, Boolean) => Unit): Unit = {
    logInfo(description)
    val timelimit = now() + timeout
    var result: Outcome = Retry()
    var current = 0L
    var iterations = 0
    do {
      iterations += 1
      result = probe()
      if (result == Retry()) {
        // probe says retry
        current = now()
        if (current> timelimit) {
          // timeout, uprate to fail
          result = TimedOut()
        } else {
          Thread.sleep(interval)
        }
      }
    } while (result == Retry())
    result match {
      case Fail() => failure(iterations, false)
      case TimedOut() => failure(iterations, true)
      case _ =>
    }
  }

  def outcomeFromBool(value: Boolean): Outcome = {
    if (value) {
      Success()
    }
    else {
      Retry()
    }
  }

  /**
   * Curryable function to use for timeouts if something
   * more specific is not needed
   * @param text
   * @param iterations
   * @param timeout
   */
  def timeout(text: String, iterations: Int, timeout: Boolean): Unit = {
    fail(text)
  }

  var interval: Long = _

  /**
   * Spin for the number of processed
   * events to exactly match the
   * supplied value.
   * <p>
   *   Fails if the timeout is exceeded
   * @param historyService history
   * @param expected exact number to await
   * @param timeout timeout in millis
   */
  def awaitEventsProcessed(historyService: YarnHistoryService,
    expected: Int, timeout: Long): Unit = {

    def eventsProcessedCheck(): Outcome = {
      outcomeFromBool(historyService.getEventsProcessed == expected)
    }

    def eventProcessFailure(iterations: Int, timeout: Boolean): Unit = {
      val eventsCount = historyService.getEventsProcessed
      val details = s"after $iterations iterations; events processed=$eventsCount"
      logError(s"event process failure $details")
      if (timeout) {
        fail(s"timeout $details")
      } else {
        assertResult(expected, details) {
          eventsCount
        }
      }
    }

    spinForState("awaitEventsProcessed",
                  interval = 50,
                  timeout = timeout,
                  probe = eventsProcessedCheck,
                  failure = eventProcessFailure)
  }

  /**
   * Spin awaiting a URL to be accessible. Useful to await a web application
   * going live before running the tests against it
   * @param url URL to probe
   * @param timeout timeout in mils
   */
  def awaitURL(url: URL, timeout: Long): Unit = {
    def probe(): Outcome = {
      try { {
        url.openStream().close()
        Success()
      }
      } catch {
        case ioe: IOException => Retry()
      }
    }

    /*
     failure action is simply to attempt the connection without
     catching the exception raised
     */
    def failure(iterations: Int, timeout: Boolean): Unit = {
      url.openStream().close()
    }

    spinForState(s"Awaiting a response from URL $url",
                  interval = 50, timeout = timeout, probe = probe, failure = failure)

  }


  /**
   * Wait for a history service's queue to become empty
   * @param historyService service
   * @param timeout timeout
   */
  def awaitEmptyQueue(historyService: YarnHistoryService, timeout: Long): Unit = {

    spinForState("awaiting empty queue",
                  interval = 50,
                  timeout = timeout,
                  probe = (() => outcomeFromBool(historyService.getQueueSize == 0)),
                  failure = ((_, _) => fail(s"queue never cleared: ${
                    historyService.getQueueSize
                  }")))
  }

  /**
   * Await for the count of flushes in the history service to match the expected value
   * @param historyService service
   * @param count number of flushes
   * @param timeout timeout
   */
  def awaitFlushCount(historyService: YarnHistoryService, count: Int, timeout: Long): Unit = {
    spinForState(s"awaiting flush count of $count",
                  interval = 50,
                  timeout = timeout,
                  probe = (() => outcomeFromBool(historyService.getFlushCount() == count)),
                  failure = ((_, _) => fail(s"flush count not $count in $historyService")))
  }


  /**
   * Probe operation to wait for an empty queue
   * @param historyService history service
   * @param timeout
   */
  def awaitServiceThreadStopped(historyService: YarnHistoryService, timeout: Long): Unit = {
    assertNotNull(historyService, "null historyService")
    spinForState("awaitServiceThreadStopped",
      interval = 50,
      timeout = timeout,
      probe = (() => outcomeFromBool(!historyService.isPostThreadActive)),
      failure = ((_, _) => fail(s"history service post thread did not finish : $historyService")))
  }

  /**
   * Wait for the listing size to match that desired.
   * @param provider provider
   * @param size size to require
   * @param timeout timeout
   * @return the successful listing
   */
  def awaitListingSize(provider: YarnHistoryProvider, size: Long, timeout: Long):
      Seq[ApplicationHistoryInfo] = {
    def listingProbe(): Outcome = {
      outcomeFromBool(provider.getListing().size == size)
    }
    def failure(i: Int, b: Boolean): Unit = {
      fail(s"after $i attempts, provider listing size !=${size}1:  ${provider.getListing() }\n" +
          s"${provider}")
    }
    spinForState("await listing size",
      100,
      timeout,
      listingProbe,
      failure)
    provider.getListing()
  }

  /**
   * Wait for the listing size to match that desired
   * @param provider provider
   * @param size size to require
   * @param timeout timeout
   * @return the successful listing
   */
  def awaitRefreshExecuted(provider: YarnHistoryProvider, timeout: Long): Unit = {
    def listingProbe(): Outcome = {
      outcomeFromBool(provider.getRefreshCount() > 0)
    }
    def failure(i: Int, b: Boolean): Unit = {
      fail(s"after $i attempts, refresh count is 0: $provider")
    }
    require(provider.isRefreshThreadRunning(),
     s"refresh thread is not running in $provider")
    spinForState("await refresh count",
      100,
      timeout,
      listingProbe,
      failure)
  }

  /**
   * Spin awaiting a URL to not contain some text
   * @param connector connector to use
   * @param url URL to probe
   *  @param text text which must not be present
   * @param timeout timeout in mils
   */
  def awaitURLDoesNotContainText(connector: SpnegoUrlConnector,
      url: URL, text: String, timeout: Long): String = {
    def get: String = {
      connector.execHttpOperation("GET", url, null, "").responseBody
    }
    def probe(): Outcome = {
      outcomeFromBool(!get.contains(text))
    }

    /*
     failure action is simply to attempt the connection without
     catching the exception raised
     */
    def failure(iterations: Int, timeout: Boolean): Unit = {
      assertDoesNotContain(get, text)
    }

    spinForState(s"Awaiting a response from URL $url",
                  interval = 50, timeout = timeout, probe = probe, failure = failure)

    get
  }

  /**
   * Spin awaiting a URL to contain some text
   * @param connector connector to use
   * @param url URL to probe
   * @param text text which must be present
   * @param timeout timeout in mils
   */
  def awaitURLContainsText(connector: SpnegoUrlConnector,
      url: URL, text: String, timeout: Long): String = {
    def get: String = {
      connector.execHttpOperation("GET", url, null, "").responseBody
    }
    def probe(): Outcome = {
      outcomeFromBool(get.contains(text))
    }

    /*
     failure action is simply to attempt the connection without
     catching the exception raised
     */
    def failure(iterations: Int, timeout: Boolean): Unit = {
      assertContains(get, text)
    }

    spinForState(s"Awaiting a response from URL $url",
                  interval = 50, timeout = timeout, probe = probe, failure = failure)

    get
  }


}

  


