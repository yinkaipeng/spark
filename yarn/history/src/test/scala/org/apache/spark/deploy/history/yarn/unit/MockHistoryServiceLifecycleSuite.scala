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
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{ContextSetup, HistoryServiceListeningToSparkContext, YarnHistoryService}

class MockHistoryServiceLifecycleSuite
    extends AbstractMockHistorySuite
    with ContextSetup with HistoryServiceListeningToSparkContext {


  /**
   * Set the batch size to 2, purely so that we can trace its path through
   * the configuration system
   */
   override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf).set(YarnHistoryService.BATCH_SIZE, "2")
  }


    /*
     * Test service lifecycle ops and that close() is re-entrant
     */
  test("Service Lifecycle") {
    describe("service lifecycle operations")

    assertResult("2", s"batch size in context") {
      sparkCtx.getConf.get(YarnHistoryService.BATCH_SIZE)
    }

    assertResult("true", s"listening flag") {
      sparkCtx.getConf.get(YarnHistoryService.REGISTER_LISTENER)
    }


    val service = startHistoryService(sparkCtx)
    assertResult(Service.STATE.STARTED, "not started") {
      service.getServiceState
    }
    assertResult(2, s"batch size in $service") {
      service.getBatchSize
    }
    assertResult(true, s"listen flag in $service") {
      service.listening
    }

    service.close()
    assertResult(Service.STATE.STOPPED, "not stopped") {
      service.getServiceState
    }
    assertResult(0, "Outstanding queue entry") {
      service.getQueueSize
    }
    // and expect an attempt to start again to fail
    val thrown = intercept[ServiceStateException] {
      service.start(sparkCtx, applicationId)
    }
    // but a repeated close is harmless
    service.close()
  }

  test("ServiceStartArguments") {
    val service = new YarnHistoryService()
    intercept[IllegalArgumentException] {
      service.start(null, applicationId)
    }
    intercept[IllegalArgumentException] {
      service.start(sparkCtx, null)
    }
  }


}
