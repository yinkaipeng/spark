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

import java.io.IOException
import java.net.URL

import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.integration.AbstractTestsWithHistoryServices

class MockWebsiteFailure extends AbstractTestsWithHistoryServices
    with MockitoSugar {
  val text = "This is not the host you are looking for"

  /**
   * Create a history provider instance
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    MockProviderHelper.createFailingProvider()
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
    val Some((ex, date)) = provider.getLastException()

    // on the next attempt: the old stack is retained
    val secondAttempt = connector.execHttpOperation("GET", webUI, null, "")
    assertContains(secondAttempt.responseBody, FailingTimelineQueryClient.ERROR_TEXT)
  }

  /**
   * this is the probe for if exceptions are swallowed/handled
   * @param webUI web UI
   * @param provider provider
   */
  def expectFailuresToPropagate(webUI: URL, provider: YarnHistoryProvider): Unit = {
    val connector = createUrlConnector()
    try {
      val outcome = connector.execHttpOperation("GET", webUI, null, "")
      fail(s"Expected an exception, got $outcome")
    } catch {
      case ex:IOException =>
        if (!ex.toString.contains("500")) {
          throw ex
        }
    }
  }

/*
  test("WebUI swallows failures") {
    webUITest(expectFailuresToBeSwallowed)
  }
*/

  test("WebUI propagates failures") {
    webUITest(expectFailuresToPropagate)
  }

}
