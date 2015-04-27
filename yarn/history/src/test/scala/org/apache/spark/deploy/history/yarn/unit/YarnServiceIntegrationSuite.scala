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
package org.apache.spark.deploy.history.yarn.unit

import org.apache.hadoop.service.{ServiceStateException, Service}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{AbstractYarnHistoryTests, YarnHistoryService}
import org.apache.spark.scheduler.cluster.YarnServices

/**
 * Test the integration with [[YarnServices]]
 */
class YarnServiceIntegrationSuite extends AbstractYarnHistoryTests {

    test("Instantiate") {
      val services = new YarnServices
      assertResult(Nil, "non-nil service list") {
        services.getServices()
      }
      services.start(sparkCtx, appId)
      assertInState(services, Service.STATE.STARTED)
      services.close()
    }

    test("Non Rentrant") {
      val services = new YarnServices
      services.start(sparkCtx, appId)
      intercept[ServiceStateException] {
        services.start(sparkCtx, appId)
      }
      services.close()
    }

  test("Contains History Service") {
    val services = new YarnServices
    try {
      services.start(sparkCtx, appId)
      val serviceList = services.getServices()
      assert(serviceList.nonEmpty, "empty service list")
      val (history :: Nil) = serviceList
      val historyService = history.asInstanceOf[YarnHistoryService]
      assertInState(historyService, Service.STATE.STARTED)
      services.close()
      assertInState(historyService, Service.STATE.STOPPED)
    } finally {
      services.close()
    }
  }

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
  }
}
