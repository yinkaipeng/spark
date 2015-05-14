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
package org.apache.spark.deploy.history.yarn

import org.apache.hadoop.service.Service
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * This is a base class for the YARN history test
 * suites, both mock and integration
 * <p>
 * Subclasses are expected to use traits and/or overriding of
 * <code>ContextSetup.setupConfiguration()</code>
 * to tune the configuration of the instantiated context.
 * <p>
 * To avoid any ambiguity about the ordering/invocation of
 * any before/after code, the operations are passed to
 * overriddeable <code>setup()</code> and <code>teardown()</code>
 * invocations.
 * <p>
 *
 */
abstract class AbstractYarnHistoryTests
    extends FunSuite with TimelineOptionsInContext with TimelineServiceDisabled
    with HistoryServiceNotListeningToSparkContext
    with BeforeAndAfter with Logging with ExtraAssertions with Matchers {

  protected var sparkCtx: SparkContext = _
  
  /*
   * Setup phase creates the spark context, a local ATS server and a client of it
   */
  before {
    setup()
  }

  /*
   * Teardown stops all services and the VM-wide spark context
   */
  after {
    teardown()
  }

  /*
  * Setup creates the spark context
  */
  protected def setup(): Unit = {
    val sparkConf = new SparkConf()
    sparkCtx = createSparkContext(sparkConf)
  }

  /**
   * Create the spark context
   * @param sparkConf configuration to extend
   */
  protected def createSparkContext(sparkConf: SparkConf): SparkContext = {
    sparkConf.setMaster("local").setAppName("AbstractYarnHistoryTests")
    new SparkContext(setupConfiguration(sparkConf))
  }

  /**
   * Teardown. This will stop the spark context
   */
  protected def teardown(): Unit = {
    stopSparkContext
  }


  protected def stopSparkContext: Unit = {
    if (sparkCtx != null) {
      sparkCtx.stop()
      sparkCtx = null
    }
  }

  /**
   * Create and start a history service.
   *
   * @param sc context
   * @return the instantiated service
   */
  protected def startHistoryService(sc: SparkContext): YarnHistoryService = {
    assertNotNull(sc, "Spark context")
    val service = new YarnHistoryService()
    assert(service.start(sc, applicationId), s"client start failed: $service")
    assert(service.isInState(Service.STATE.STARTED), s"wrong state: $service")
    service
  }

  /**
   * Create a history service, post the event sequence, then stop the service
   * @param sc context
   * @return the (now closed) history service
   */
  def postEvents(sc: SparkContext): (YarnHistoryService, Int) = {
    val service: YarnHistoryService = startHistoryService(sc)
    val listener = new YarnEventListener(sc, service)
    var eventsPosted = 0
    try {
      listener.onApplicationStart(applicationStart)
      listener.onEnvironmentUpdate(environmentUpdate)
      listener.onApplicationEnd(applicationEnd)
      eventsPosted = 3
    } finally {
      service.stop()
      // as this adds an event to the history, it is incremented
      eventsPosted += 1
    }
    (service, eventsPosted)
  }


}
