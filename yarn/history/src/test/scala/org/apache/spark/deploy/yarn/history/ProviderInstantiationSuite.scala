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
package org.apache.spark.deploy.yarn.history


import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.integration.AbstractTestsWithHistoryServices
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.scheduler.cluster.YarnExtensionServices

class ProviderInstantiationSuite extends AbstractTestsWithHistoryServices {

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

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    sparkConf.set(SPARK_HISTORY_PROVIDER, provider_name)
    sparkConf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
  }

  test("SimpleCtor") {
    new YarnHistoryProvider(new SparkConf())
  }

  private val provider_name = "org.apache.spark.deploy.yarn.history.YarnHistoryProvider"
  test("CreateViaConfig") {
    val conf = new SparkConf()
    conf.set(YarnExtensionServices.SPARK_YARN_SERVICES, YarnHistoryService.CLASSNAME)
    conf.set(SPARK_HISTORY_PROVIDER,
        provider_name)
    conf.set(SPARK_HISTORY_UI_PORT, findPort().toString)
    val providerName = conf.getOption("spark.history.provider")
        .getOrElse("org.apache.spark.deploy.history.FsApplicationHistoryInfo")
    val provider = Class.forName(providerName)
        .getConstructor(classOf[SparkConf])
        .newInstance(conf)
        .asInstanceOf[YarnHistoryProvider]
  }

  test("Empty Provider List") {
    describe("Provider listing")
    provider = new YarnHistoryProvider(sparkCtx.conf)
    assertResult(Nil) {
      provider.getListing()
    }
  }


}
