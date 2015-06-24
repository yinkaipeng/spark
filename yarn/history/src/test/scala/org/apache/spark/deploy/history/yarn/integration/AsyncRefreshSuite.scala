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

import java.net.{URI, URL}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryProvider._
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.{TimeManagedHistoryProvider, YarnHistoryProvider, YarnHistoryService}
import org.apache.spark.scheduler.cluster.YarnExtensionServices

/**
 * Tests to verify async refreshes work as expected
 */
class AsyncRefreshSuite extends AbstractTestsWithHistoryServices {

  val SLEEP_INTERVAL = 100
  val EVENT_PROCESSED_TIMEOUT = 5000
  val WINDOW = YarnHistoryProvider.DEFAULT_MIN_REFRESH_INTERVAL_SECONDS

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(OPTION_MIN_REFRESH_INTERVAL, WINDOW.toString())
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
  }

  /**
   * Start time. This has to be big enough that `now()-lastRefresTime > window`,
   * As the initial refresh time is 0; this is
   */
  val CLOCK_START_TIME = 10 * 60 * 1000
  val TICK_TIME = 1000

  /**
   * Create a failing history provider instance, with the flag set to say "the initial
   * health check" has not been executed.
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    new TimeManagedHistoryProvider(conf, CLOCK_START_TIME, TICK_TIME)
  }

  def timelineRootEndpoint(): URI = {
    val realTimelineEndpoint = getTimelineEndpoint(sparkCtx.hadoopConfiguration).toURL
    new URL(realTimelineEndpoint, "/").toURI
  }


  /**
   * This test verifies that `YarnHistoryProvider.getListing()` events
   * trigger asynchronous refreshes, except when the last refresh
   * was within the limits of the refresh window.
   *
   * To do this it makes use of the counter in the refresher recording the number
   * of events received in its action queue; so verifying when the (async) refresh
   * actions are received and processed.
   */

  test("RefreshInterval") {
    describe("verify reset interval logic")
    val history = createHistoryProvider(new SparkConf()).asInstanceOf[TimeManagedHistoryProvider]
    val refresher = history.refresher
    assert (refresher.isRunning(), s"refresher not running in $history")
    awaitRefreshMessageProcessed(history, 0, EVENT_PROCESSED_TIMEOUT, s"startup of  $refresher")

    val attempt0 = history.getLastRefreshAttemptTime()
    assert(attempt0 === CLOCK_START_TIME, s"initial event in $refresher")
    val count0 = refresher.messagesProcessed;
    // add thirty seconds
    history.incrementTime(30000)
    history.getListing()
    // yield a bit for the refresher thread
    awaitRefreshMessageProcessed(history, count0, EVENT_PROCESSED_TIMEOUT, "in-window")

    // expect no refresh
    assert(history.getLastRefreshAttemptTime() === CLOCK_START_TIME,
      s"last refresh at clock time in $refresher")

    //now move clock forwards a minute
    history.incrementTime(2 * WINDOW * 1000)
    // and expect the new listing to trigger another refresh
    val count2 = refresher.messagesProcessed
    val time2 = history.now()
    history.getListing()
    awaitRefreshMessageProcessed(history, count2, EVENT_PROCESSED_TIMEOUT, "out of window")
    val refreshAttemptTime2 = history.getLastRefreshAttemptTime()
    val time3 = history.now()
    assert(2 === refresher.refreshesExecuted, s"refreshes executed in $refresher")
    assert(time2 <= refreshAttemptTime2,
      s"refresh time in $refresher was $refreshAttemptTime2; time2 was $time2; current =$time3")
  }

  test("Size Zero refresh window") {
    describe("Test with a zero size window to verify that the refresh always takes ")
    val conf = new SparkConf()
    conf.set(OPTION_MIN_REFRESH_INTERVAL, "0")
    val history = createHistoryProvider(conf).asInstanceOf[TimeManagedHistoryProvider]
    val refresher = history.refresher
    assert(refresher.isRunning(), s"refresher not running in $history")
    awaitRefreshMessageProcessed(history, 0, EVENT_PROCESSED_TIMEOUT, s"startup of  $refresher")

    val attempt0 = history.getLastRefreshAttemptTime()
    assert(attempt0 === CLOCK_START_TIME, s"initial event in $refresher")
    val count0 = refresher.messagesProcessed;
    val t2 = history.tick()
    history.getListing()
    // yield a bit for the refresher thread
    awaitRefreshMessageProcessed(history, count0, EVENT_PROCESSED_TIMEOUT, "in-window")

    // expect no refresh
    assert(history.getLastRefreshAttemptTime() === t2,
      s"refresh didnt' happen in $refresher")
  }


  /**
   * Wait for the refresh count to increment by at least one iteration
   * @param provider provider
   * @param timeout timeout
   * @return the successful listing
   */
  def awaitRefreshMessageProcessed (provider: TimeManagedHistoryProvider,
      initialCount: Long, timeout: Long,  text: String): Unit = {
    val refresher = provider.refresher

    def listingProbe(): Outcome = {
      outcomeFromBool(refresher.messagesProcessed > initialCount)
    }
    def failure(outcome: Outcome, attempts: Int, timedOut: Boolean): Unit = {
      val outcome = if (timedOut) "timeout" else "failed"
      fail(s"$text -$outcome after $attempts attempts," +
          s" refresh count is < $initialCount in: $refresher")
    }
    require(provider.isRefreshThreadRunning(),
      s"refresher is not running in $provider")
    spinForState("await refresh message count",
      SLEEP_INTERVAL,
      timeout,
      listingProbe,
      failure)
  }

}
