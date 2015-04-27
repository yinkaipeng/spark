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

import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.scheduler.SparkListenerEvent

class TimelinePostSuite extends AbstractTestsWithHistoryServices {

/*
  test("low-level post") {
    val entity = new TimelineEntity
    entity.setStartTime(now())
    entity.setEntityId("post")
    entity.setEntityType("test")
    putTimelineEntity(entity)
  }

  test("serialize and POST start") {
    postEvent(appStartEvent(), 100)
  }

  test("Round Trip App Start") {
    historyService = startHistoryService(sparkCtx, false)
    val sparkEvt = appStartEvent()
    val outcome = postEvent(sparkEvt, 100)
    assertEntityResolves(sparkEvt)
  }
*/

  protected def assertEntityResolves(sparkEvt: SparkListenerEvent): Unit = {
/*
    val retrievedEntities = lookupTimelineEntities()
    val entities = retrievedEntities.getEntities()
    assertNotNull(entities, "Null TimelineEntities.getEntities()")
    assertNotEmpty(entities, "retrieved timelineEntities")
    assertResult(sparkEvt) {
      convertToSparkEvent(entities.get(0))
    }
*/
  }

  test("Round Trip App Stop") {
    historyService = startHistoryService(sparkCtx)
    val sparkEvt = appStopEvent()
    val outcome = postEvent(sparkEvt, 100)
    assertEntityResolves(sparkEvt)
    historyService.stop()
    awaitEmptyQueue(historyService,1000)
  }


  test("App Start Via Event Listener") {
    historyService = startHistoryService(sparkCtx)
    val listener = new YarnEventListener(sparkCtx, historyService)
    val sparkEvt = appStartEvent()
    listener.onApplicationStart(sparkEvt)
    assertEntityResolves(sparkEvt)
  }


}
