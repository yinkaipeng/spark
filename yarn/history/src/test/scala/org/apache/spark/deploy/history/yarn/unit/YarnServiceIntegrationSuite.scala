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

import org.apache.hadoop.service.{AbstractService, Service, ServiceStateException}
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.yarn.api.records.ApplicationId

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{AbstractYarnHistoryTests, YarnHistoryProvider}
import org.apache.spark.scheduler.cluster.{YarnExtensionService, YarnExtensionServices}

/**
 * Test the integration with [[YarnExtensionServices]]
 */
class YarnServiceIntegrationSuite extends AbstractYarnHistoryTests {

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES,
                   "org.apache.spark.deploy.history.yarn.unit.SimpleService")
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
    try {
      services.start(sparkCtx, applicationId)
      val serviceList = services.getServices()
      assert(serviceList.nonEmpty, "empty service list")
      val (service :: Nil) = serviceList
      val simpleService = service.asInstanceOf[SimpleService]
      assertInState(simpleService, Service.STATE.STARTED)
      services.close()
      assertInState(simpleService, Service.STATE.STOPPED)
    } finally {
      services.close()
    }
  }

  /**
   * You aren't allowed to register a shutdown hook with 0 priority.
   */
  test("Shutdownhook zero priority") {
    val services = new YarnExtensionServices
    intercept[IllegalArgumentException] {
      // you need a property > 0 here
      services.start(sparkCtx, applicationId, true, 0)
    }
  }

  /**
   * Test shutdown hook registration. There's no way to test shutdown hook operation
   * except by stopping the JVM, at which point it's too late for assertions.
   * What can be tested is
   * -the registration of the hook
   * -that direct invocation of the hook stops the service
   * -that the service is unregistered afterwards
   */
  test("Shutdownhook") {
    val services = new YarnExtensionServices
    // register with a valid hook
    services.start(sparkCtx, applicationId, true, 10)
    val (service :: Nil) = services.getServices()
    val simpleService = service.asInstanceOf[SimpleService]
    assertInState(simpleService, Service.STATE.STARTED)

    val shutdownHookManager = ShutdownHookManager.get()
    assert(shutdownHookManager.hasShutdownHook(services.shutdownHook))
    services.shutdownHook.run()
    assertResult(Service.STATE.STOPPED) {
      services.getServiceState()
    }
    assert(!shutdownHookManager.hasShutdownHook(services.shutdownHook))
    assertInState(simpleService, Service.STATE.STOPPED)

  }


}

class SimpleService extends AbstractService("simple") with YarnExtensionService {
  /**
   * For Yarn services, SparkContext, and ApplicationId is the basic info required.
   * This operation must be called before `init`
   * @param sc spark context
   * @param appId YARN application ID
   */
  override def start(sc: SparkContext, appId: ApplicationId): Boolean = {
    init(sc.hadoopConfiguration)
    start()
    true
  }
}
