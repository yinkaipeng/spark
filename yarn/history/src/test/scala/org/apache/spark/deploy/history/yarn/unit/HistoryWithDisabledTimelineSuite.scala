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

import org.apache.hadoop.service.Service

import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{AbstractYarnHistoryTests, HandleSparkEvent, YarnHistoryService}

/**
 * Test that with the timeline service disabled, public operations degrade gracefully
 */
class HistoryWithDisabledTimelineSuite extends AbstractYarnHistoryTests {

    test("BasicLifecycle") {
      val service = new YarnHistoryService()
      // verify that the string operator does not fail
      service.toString()

      assert(!service.start(sparkCtx, applicationId), s"client start failed: $service")
      assertResult(Service.STATE.STARTED, s"not started : $service") {
        service.getServiceState
      }
      assert(!service.bondedToATS, s"service is bonded to ats $service")
      assert(!service.listening, s"service is listening $service")
      assertResult(null, s"service address : $service") {
        service.getTimelineServiceAddress()
      }
      intercept[Exception] {
        service.getTimelineClient
      }
      assert(!service.isPostThreadActive, s"service post thread active: $service")

      // verify that the string operator does not fail
      service.toString()
      service.close()
      assertResult(Service.STATE.STOPPED, "not stopped : $service") {
        service.getServiceState
      }
      // verify that the string operator does not fail
      service.toString()
    }

    test("QueueAndFlush") {
      val service = new YarnHistoryService()
      try {
        assert(!service.start(sparkCtx, applicationId), s"client start failed: $service")
        service.enqueue(new HandleSparkEvent(appStartEvent(), 1))
        service.enqueue(new HandleSparkEvent(appStopEvent(), 1))


        assertResult(0, "queue") {
          service.getQueueSize
        }
        assertResult(0, "queue") {
          service.getEventsQueued
        }

        service.asyncFlush()
        assertResult(0, "flush count") {
          service.getFlushCount()
        }

        service.stop()
        assertResult(0, "flush count") {
          service.getFlushCount()
        }
      } finally {
        service.stop()
      }
    }

}
