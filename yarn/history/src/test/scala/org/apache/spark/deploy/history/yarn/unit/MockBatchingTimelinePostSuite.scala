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
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._

import org.apache.spark.deploy.history.yarn.TimelineSingleEntryBatchSize
import org.apache.spark.deploy.history.yarn.YarnTestUtils._

/**
 * Mock tests with Batch size 1
 */
class MockBatchingTimelinePostSuite
    extends AbstractMockHistorySuite
    with TimelineSingleEntryBatchSize {
/*
  test("multiple Spark listener event batches") {
    describe("verify batching")
    val (events, eventsPosted) = postEvents(sparkCtx)

    val captor = ArgumentCaptor.forClass(classOf[TimelineEntity])
    verify(timelineClient, times(events.getEventsProcessed)).putEntities(captor.capture())

    val values = captor.getAllValues()
    assert(!values.isEmpty, "empty captor.getAllValues()")
    val update1 = values.get(0)
    val uploadedEnvUpdate = convertToSparkEvent(update1)
    uploadedEnvUpdate should be (environmentUpdate)

    val update2 = captor.getAllValues().get(1)
    val uploadedAppStart = convertToSparkEvent(update2)
    uploadedAppStart should be (applicationStart)
  // TODO: set up the filters so this section works
//    val appnameFilter = update2.getPrimaryFilters().get("appName")
//    assert(appnameFilter != null, "filter 'appName'")
//    appnameFilter.toSeq should be (Seq(applicationStart.appName))
//    val sparkUserFilter = update2.getPrimaryFilters().get("sparkUser")
//    assert(sparkUserFilter != null, "filter 'sparkUser'")
//    sparkUserFilter.toSeq should be (Seq(applicationStart.sparkUser))
//    update2.getStartTime() should be (applicationStart.time)

    val update3 = captor.getAllValues().get(2)
    val uploadedAppEnd = convertToSparkEvent(update3)
    uploadedAppEnd should be (applicationEnd)
  }
*/
  test("retry upload on failure") {
    describe("mock failures, verify retry count incremented")
    // timeline client to throw an RTE on the first put
    when(timelineClient.putEntities(any(classOf[TimelineEntity])))
        .thenThrow(new RuntimeException("triggered"))
        .thenReturn(response)

    val (_, eventsPosted) = postEvents(sparkCtx)
    verify(timelineClient, times(1 + eventsPosted)).putEntities(any(classOf[TimelineEntity]))
  }
}
