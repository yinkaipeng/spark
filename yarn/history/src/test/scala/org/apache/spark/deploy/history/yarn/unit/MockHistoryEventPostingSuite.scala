/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history.yarn.unit

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.mockito.Matchers._
import org.mockito.Mockito._

import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{YarnEventListener, YarnHistoryService}

/**
 * Mock event posting
 */
class MockHistoryEventPostingSuite extends AbstractMockHistorySuite {

  /*
   * Make sure the low-level stuff the other tests depend on is there
   */
  test("Timeline client") {
    describe("low-level timeline client test")

    assertResult(response, "low-level") {
     timelineClient.putEntities(new TimelineEntity)
    }
    verify(timelineClient).putEntities(any(classOf[TimelineEntity]))
  }


  test("Event Queueing") {
    describe("event queueing")
    val (events, eventsPosted) = postEvents(sparkCtx)
    assert(events.getEventsProcessed > 0, "No events were processed")
    assertResult(eventsPosted) {events.getEventsProcessed}
  }


  test("batch processing of Spark listener events") {
    val (events, eventsPosted) = postEvents(sparkCtx)
    verify(timelineClient, times(events.getEventsProcessed))
        .putEntities(any(classOf[TimelineEntity]))
  }


  test("PostEventsNoServiceStop") {
    describe("verify that events are pushed on any triggered flush," +
        " even before a service is stopped")
    val service: YarnHistoryService = startHistoryService(sparkCtx)
    try {
      val listener = new YarnEventListener(sparkCtx, service)
      listener.onApplicationStart(applicationStart)
      service.asyncFlush()
      awaitEventsProcessed(service, 1, 2000)
      verify(timelineClient, times(1)).putEntities(any(classOf[TimelineEntity]))
    } finally {
      service.stop();
    }
  }

}
