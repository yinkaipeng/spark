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

import java.net.{NoRouteToHostException, URI}

import org.apache.hadoop.conf.Configuration
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.integration.AbstractTestsWithHistoryServices
import org.apache.spark.deploy.history.yarn.rest.{UnauthorizedRequestException, JerseyBinding}
import org.apache.spark.deploy.history.yarn.{YarnHistoryProvider, YarnHistoryService}

class TimelineQueryFailureSuite extends AbstractTestsWithHistoryServices {


  /**
   * Create the client and the app server
   * @param conf the hadoop configuration
   */
  override protected def startTimelineClientAndAHS(conf: Configuration): Unit = {

  }

  /**
   * Create a history provider instance
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
     FailingYarnHistoryProvider.createFailingProvider(conf, false)
  }

  /**
   * Verifies that failures are propagated
   */
  test("ClientGETFails") {
    val failingClient = FailingYarnHistoryProvider.createQueryClient()
    intercept[NoRouteToHostException] {
      failingClient.get(new URI("http://localhost:80/"),
                         (() => "failed"))
    }
  }

  test("ClientListFails") {
    val failingClient = FailingYarnHistoryProvider.createQueryClient()
    intercept[NoRouteToHostException] {
      failingClient.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    }
  }

  test("UnauthedClientListFails") {
    val failingClient = new ClientResponseTimelineQueryClient(
      401, "401",
      new URI("http://localhost:80/"),
      new Configuration(),
      JerseyBinding.createClientConfig())

    intercept[UnauthorizedRequestException] {
      failingClient.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    }
  }

  test("getTimelineEntity to fail") {
    describe("getTimelineEntity to fail")
    val provider = createHistoryProvider(new SparkConf()).asInstanceOf[FailingYarnHistoryProvider]
    provider.setHealthChecked(true)

    // not using intercept[] for better diagnostics on failure (i.e. rethrow the unwanted
    // exception
    try {
      val entity = provider.getTimelineEntity("app1")
      fail(s"Expected failure, got $entity")
    } catch {
      case ioe: NoRouteToHostException =>
        logInfo(s"expected exception caught: $ioe")
      case ex: TestFailedException =>
        throw ex
      case ex: Exception =>
        logError("Wrong exception: ", ex)
        throw ex
    } finally {
      provider.stop()
    }
  }

  test("getAppUI to recover") {
    describe("getTimelineEntity to recover")
    val provider = createHistoryProvider(new SparkConf())
        .asInstanceOf[FailingYarnHistoryProvider]
    // skip that initial health check
    provider.setHealthChecked(true)

    try {
      assertResult(None) {
        provider.getAppUI("app1")
      }
      val lastException = provider.getLastFailure()
      assertNotNull(lastException, "null last exception")
      lastException match {
        case Some((ex, date)) =>
          if (!ex.isInstanceOf[NoRouteToHostException]) {
            throw ex
          }
        case None =>
          fail("No logged exception")
      }
      // and getting the config returns it
      val config = provider.getConfig()

      assertMapValueContains(config,
                              YarnHistoryProvider.KEY_LAST_FAILURE,
                              FailingTimelineQueryClient.ERROR_TEXT)
    } finally {
      provider.stop()
    }

  }

}

