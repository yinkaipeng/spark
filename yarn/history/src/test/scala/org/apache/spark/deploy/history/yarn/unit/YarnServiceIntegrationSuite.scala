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

import org.apache.hadoop.service.{Service, ServiceStateException}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.{ApplicationHistoryProvider, FsHistoryProvider}
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{AbstractYarnHistoryTests, YarnHistoryProvider, YarnHistoryService}
import org.apache.spark.scheduler.cluster.YarnExtensionServices

/**
 * Test the integration with [[YarnExtensionServices]]
 */
class YarnServiceIntegrationSuite extends AbstractYarnHistoryTests {

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER,
                   YarnHistoryProvider.YARN_HISTORY_PROVIDER_CLASS)
  }
  
  test("Instantiate") {
    val services = new YarnExtensionServices
    assertResult(Nil, "non-nil service list") {
      services.getServices()
    }
    services.start(sparkCtx, applicationId)
    assertInState(services, Service.STATE.STARTED)
    services.close()
  }

  test("Non Rentrant") {
    val services = new YarnExtensionServices
    services.start(sparkCtx, applicationId)
    intercept[ServiceStateException] {
      services.start(sparkCtx, applicationId)
    }
    services.close()
  }

  test("Contains History Service") {
    val services = new YarnExtensionServices
    try { {
      services.start(sparkCtx, applicationId)
      val serviceList = services.getServices()
      assert(serviceList.nonEmpty, "empty service list")
      val (history :: Nil) = serviceList
      val historyService = history.asInstanceOf[YarnHistoryService]
      assertInState(historyService, Service.STATE.STARTED)
      services.close()
      assertInState(historyService, Service.STATE.STOPPED)
    }
    } finally {
      services.close()
    }
  }


}
