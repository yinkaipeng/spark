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
package org.apache.spark.deploy.history.yarn.failures

import java.net.{NoRouteToHostException, URL}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.integration.AbstractTestsWithHistoryServices
import org.apache.spark.deploy.history.yarn.rest.HttpRequestException
import org.apache.spark.deploy.history.yarn.YarnTestUtils._

class WebsiteFailureSuite extends AbstractTestsWithHistoryServices {

  /**
   * Create a failing history provider instance with the health check bypassed (to verify low-level)
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    FailingYarnHistoryProvider.createFailingProvider(true)
  }

  /**
   * this is the probe for if exceptions are swallowed/handled
   * @param webUI web UI
   * @param provider provider
   */
  def expectFailuresToBeSwallowed(webUI: URL, provider: YarnHistoryProvider): Unit = {
    val connector = createUrlConnector()
    val outcome = connector.execHttpOperation("GET", webUI, null, "")
    // get that last exception
    val Some((ex, date)) = provider.getLastFailure()

    // on the next attempt: the old stack is retained
    val secondAttempt = connector.execHttpOperation("GET", webUI, null, "")
    assertContains(secondAttempt.responseBody, FailingTimelineQueryClient.ERROR_TEXT)
  }

  /**
   * this is the probe for exceptions passed back to the caller.
   * @param url URL to GET
   */
  def expectFailuresToPropagate(url: URL): Unit = {
    val connector = createUrlConnector()
    try {
      val outcome = connector.execHttpOperation("GET", url, null, "")
      fail(s"Expected an exception, got $outcome")
    } catch {
      case ex: HttpRequestException =>
        if (ex.status != 500) {
          throw ex
        }
        logInfo(s"received exception ", ex)
        val body = ex.body
        assert(!body.isEmpty,s" empty body from exception $ex")
        assertContains(body, FailingTimelineQueryClient.ERROR_TEXT)
      case ex: Exception =>
        throw ex
    }
  }

  test("WebUI swallows failures") {
    webUITest("WebUI swallows failures", expectFailuresToBeSwallowed)
  }



  test("listAndCacheApplications failure handling") {
    describe("Checking listAndCacheApplications behavior")
    val provider = createHistoryProvider(new SparkConf())

    provider.getApplications()
    assertResult(0, "initial applications.timestamp not zero") {
      provider.getApplications().timestamp
    }

    logDebug("Asking for a listing")

    val listing = provider.listAndCacheApplications()
    assertResult(0, "Non-empty listing") { listing.applications.size }
    assert(listing.timestamp  > 0, "zero timestamp")
    assert(listing.failed,"listing did not fail")
    provider.getLastFailure() match {
      case Some((ex: NoRouteToHostException, time)) =>
        assert(time.getTime > 0, s"zero time value")
      case Some((ex, time)) =>
        // wrong exception
        throw ex
      case None =>
        fail("no failure logged")
    }

    // the inner application listing has a timestamp of zero
    val applications = provider.getApplications()
    assertResult(0, "updated applications.timestamp not zero") { applications.timestamp }

    val config = provider.getConfig()
    assertMapValueContains(config,
                            YarnHistoryProvider.KEY_LAST_FAILURE,
                            FailingTimelineQueryClient.ERROR_TEXT)
    assertMapValueContains(config,
                            YarnHistoryProvider.KEY_LAST_UPDATED,
                            YarnHistoryProvider.TEXT_NEVER_UPDATED)
  }


  /*
    test("WebUI propagates failures") {
      webUITest(expectFailuresToPropagate)
    }
  */

}
