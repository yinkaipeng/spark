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

import org.apache.spark.deploy.history.yarn.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.ui.SparkUI

class YarnHistoryProviderSuite extends AbstractTestsWithHistoryServices {

  var provider: YarnHistoryProvider = _

  /**
   * Teardown stops provider
   */
  override def teardown(): Unit = {
    if (provider != null) {
      provider.stop()
    }
    super.teardown()
  }

  test("Empty Provider List") {
    describe("Provider listing")
    provider = new YarnHistoryProvider(sparkCtx.conf)
    assertResult(Nil) {
      provider.getListing()
    }
  }

  test("getAppUi(unknown-app)") {
    describe("getAppUi(unknown-app) -> none")
    assertResult(None) {
      getAppUI("unknown-app")
    }
  }


  test("getAppUi(\"\")") {
    describe("getAppUi(\"\") -expect exception")
    intercept[IllegalArgumentException] {
      getAppUI("")
    }
  }

  /**
   * Create the provider, get the app UI.
   * <p>
   * The provider member variable will be set as a side-effect
   * @param appid
   * @return the result of <code>provider.getAppUI</code>
   */
  protected def getAppUI(appid: String): Option[SparkUI] = {
    provider = new YarnHistoryProvider(sparkCtx.conf)
    logInfo(s"GET appID =$appid")
    provider.getAppUI(appid)
  }
}
