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

import java.net.{URI, URL}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.integration.AbstractTestsWithHistoryServices
import org.apache.spark.deploy.history.yarn.rest.{HttpRequestException, JerseyBinding, TimelineQueryClient}
import org.apache.spark.deploy.history.yarn.{YarnHistoryProvider, YarnHistoryService}
import org.apache.spark.scheduler.cluster.YarnExtensionServices

/**
 * Test reporting of connectivity problems to the caller, specifically how
 * the `YarnHistoryProvider` handles the initial binding & reporting of problems.
 *
 */
class WebsiteDiagnosticsSuite extends AbstractTestsWithHistoryServices {


  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
  }


  var failingHistoryProvider: FailingYarnHistoryProvider = _

  /**
   * Create a failing history provider instance, with the flag set to say "the initial
   * health check" has not been executed.
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val yarnConf = sparkCtx.hadoopConfiguration

    val client = new TimelineQueryClient(timelineRootEndpoint(),
                             yarnConf, JerseyBinding.createClientConfig())
    failingHistoryProvider = new
            FailingYarnHistoryProvider(client, false, client.getTimelineURI(), conf)
    failingHistoryProvider
  }

  def timelineRootEndpoint(): URI = {
    val realTimelineEndpoint = getTimelineEndpoint(sparkCtx.hadoopConfiguration).toURL
    new URL(realTimelineEndpoint, "/").toURI
  }

  /**
   * Issue a GET request against the Web UI and expect it to fail with an error
   * message indicating that `text/html` is not a supported type.
   * with error text indicating it was in the health check
   * @param webUI URL to the web UI
   * @param provider the provider
   */
  def expectApplicationLookupToFailInHealthCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
    val connector = createUrlConnector()
    val appURL = new URL(webUI, "/history/app-0001")
    describe(s"Expecting health checks to fail while retrieving $appURL")
    awaitURL(webUI, TEST_STARTUP_DELAY)
    try {
      assert(!failingHistoryProvider.isHealthy())
      val body = getHtmlPage(appURL, Nil)
      fail(s"Expected a failure from GET $appURL -but got\n$body")
    } catch {
      case ex: HttpRequestException =>
        assertContains(ex.toString, TimelineQueryClient.MESSAGE_CHECK_URL)
    }
  }

  test("Probe UI with Health check") {
    def probeUIWithFailureCaught(webUI: URL, provider: YarnHistoryProvider): Unit = {
      awaitURL(webUI, TEST_STARTUP_DELAY)

      val body = getHtmlPage(webUI,
          YarnHistoryProvider.TEXT_NEVER_UPDATED :: Nil)
    }
    webUITest("Probe UI with Health check", probeUIWithFailureCaught)
  }

  test("Probe App ID with Health check") {
    def expectAppIdToFail(webUI: URL, provider: YarnHistoryProvider): Unit = {
      expectApplicationLookupToFailInHealthCheck(webUI, provider)
    }
    webUITest("Probe App ID with Health check", expectAppIdToFail)
  }

}
