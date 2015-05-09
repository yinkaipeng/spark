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
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.exceptions.TestFailedException
import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.integration.AbstractTestsWithHistoryServices
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding
import org.apache.spark.deploy.history.yarn.{YarnHistoryProvider, YarnHistoryService}

class MockTimelineQueryFailure extends AbstractTestsWithHistoryServices
    with MockitoSugar {


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
     MockProviderHelper.createFailingProvider()
  }

  /**
   * Verifies that failures are propagated
   */
  test("ClientGETFails") {
    val failingClient = MockProviderHelper.createQueryClient()
    intercept[NoRouteToHostException] {
      failingClient.get(new URI("http://localhost:80/"),
                         (() => "failed"))
    }
  }

  test("ClientListFails") {
    val failingClient = MockProviderHelper.createQueryClient()
    intercept[NoRouteToHostException] {
      failingClient.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    }
  }

  test("getListing to fail") {
    val provider = createHistoryProvider(new SparkConf())
    // not using intercept[] for better diagnostics on failure (i.e. rethrow the unwanted
    // exception
    val queryClient = provider.getTimelineQueryClient().asInstanceOf[FailingTimelineQueryClient]

    try { {
      logDebug("Asking for a listing")
      val listing = provider.getListing()
      fail(s"Expected failure, got a listing $listing")
    }
    } catch {
      case ioe: NoRouteToHostException =>
        logInfo(s"expected exception caught: $ioe")
      case ex: TestFailedException =>
        throw ex
      case ex: Exception =>
        logError("Wrong exception: ", ex)
        throw ex;
    }
  }

  test("getTimelineEntity to fail") {
    describe("getTimelineEntity to fail")
    val provider = createHistoryProvider(new SparkConf())
    // not using intercept[] for better diagnostics on failure (i.e. rethrow the unwanted
    // exception
    try { {
      val entity = provider.getTimelineEntity("app1")
      fail(s"Expected failure, got $entity")
    }
    } catch {
      case ioe: NoRouteToHostException =>
        logInfo(s"expected exception caught: $ioe")
      case ex: TestFailedException =>
        throw ex
      case ex: Exception =>
        logError("Wrong exception: ", ex)
        throw ex;
    }
  }

  test("getAppUI to recover") {
    describe("getTimelineEntity to recover")
    val provider = createHistoryProvider(new SparkConf())
    assertResult(None) {
      provider.getAppUI("app1")
    }
    val lastException = provider.getLastException()
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

    assertMapContains(config, YarnHistoryProvider.KEY_LAST_EXCEPTION,
                       FailingTimelineQueryClient.ERROR_TEXT)
    assertMapContains(config, YarnHistoryProvider.KEY_LAST_EXCEPTION_STACK, "")
    assertMapContains(config, YarnHistoryProvider.KEY_LAST_EXCEPTION_DATE, "")

  }


  protected def assertMapContains(config: Map[String, String], key: String, text: String): Unit = {
    config.get(key) match {
      case Some(s) =>
        if (!text.isEmpty && !s.contains(text)) {
          fail(s"Did not find '$text' in key[$key] = '$s'")
        }
      case None =>
        fail(s"No entry for key $key")
    }
  }
}

/**
 * Some operations to help the mock failing tests
 */
object MockProviderHelper extends MockitoSugar {

  def createQueryClient(): FailingTimelineQueryClient = {
    new FailingTimelineQueryClient(new URI("http://localhost:80/"),
                                    new Configuration(),
                                    JerseyBinding.createClientConfig())
  }


  /**
   * This inner provider calls most of its internal methods.
   * @return
   */
  def createFailingProvider(): YarnHistoryProvider = {
    val failingClient = createQueryClient()
    val provider = mock[YarnHistoryProvider]
    doReturn(failingClient).when(provider).getTimelineQueryClient()
    doCallRealMethod().when(provider).getAppUI(anyString())
    doCallRealMethod().when(provider).getListing()
    doCallRealMethod().when(provider).getConfig()
    doCallRealMethod().when(provider).getTimelineEntity(anyString())
    doCallRealMethod().when(provider).getLastException()
    doReturn(new URI("http://localhost:80/")).when(provider).getRootURI()
    provider
  }
}
