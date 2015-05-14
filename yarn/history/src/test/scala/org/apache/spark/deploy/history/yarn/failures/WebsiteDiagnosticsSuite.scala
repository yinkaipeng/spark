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

class WebsiteDiagnosticsSuite extends AbstractTestsWithHistoryServices {


  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
  }


  /**
   * Create a history provider instance.
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    val yarnConf = sparkCtx.hadoopConfiguration

    val client = new TimelineQueryClient(timelineRootEndpoint(),
                             yarnConf, JerseyBinding.createClientConfig())
    new FailingYarnHistoryProvider(client,false, client.getTimelineURI())
  }

  def timelineRootEndpoint(): URI = {
    val realTimelineEndpoint = getTimelineEndpoint(sparkCtx.hadoopConfiguration).toURL
    new URL(realTimelineEndpoint, "/").toURI
  }

  test("Instantiate HistoryProvider") {
    val conf = sparkCtx.hadoopConfiguration
    new TimelineQueryClient(timelineRootEndpoint(),
         conf, JerseyBinding.createClientConfig())

    createHistoryProvider(sparkCtx.getConf)
  }

  /**
   * Issue a GET request against the Web UI and expect it to fail
   * with error text indicating it was in the health check
   * @param webUI URL to the web UI
   * @param provider the provider
   */
  def expectGetToFailInHealthCheck(webUI: URL, provider: YarnHistoryProvider): Unit = {
    val connector = createUrlConnector()
    val ex = intercept[HttpRequestException] {
      connector.execHttpOperation("GET", webUI, null, "")
    }
    assertContains(ex.toString, "Check the URL")
  }

  test("Probe UI with Health check") {
    webUITest(expectGetToFailInHealthCheck)
  }

  test("Probe App UI with Health check") {
    def expectAppIdToFail(webUI: URL, provider: YarnHistoryProvider): Unit = {
      expectGetToFailInHealthCheck(new URL(webUI, "/appId"), provider)
    }
    webUITest(expectGetToFailInHealthCheck)
  }

  /**
   * When the UI is disabled, the GET works but there's an error message
   * warning of the fact. The health check is <i>not</i> reached.
   */
  test("Probe Disabled UI") {
    def probeDisabledUI(webUI: URL, provider: YarnHistoryProvider): Unit = {
      val fp = provider.asInstanceOf[FailingYarnHistoryProvider]
      fp.setEnabled(false)
      probeEmptyWebUI(webUI, provider)
      val body = getHtmlPage(webUI,
          YarnHistoryProvider.TEXT_SERVICE_DISABLED :: Nil)
    }
    webUITest(probeDisabledUI)
  }


}
