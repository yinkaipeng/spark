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

import java.net.URL

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.integration.AbstractTestsWithHistoryServices
import org.apache.spark.deploy.history.yarn.rest.HttpRequestException

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
   * this is the probe for exceptions passed back to the caller.
   * @param webUI web UI
   * @param provider provider
   */
  def expectFailuresToPropagate(webUI: URL, provider: YarnHistoryProvider): Unit = {
    val connector = createUrlConnector()
    try {
      val outcome = connector.execHttpOperation("GET", webUI, null, "")
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

  test("WebUI propagates failures") {
    webUITest(expectFailuresToPropagate)
  }

}
