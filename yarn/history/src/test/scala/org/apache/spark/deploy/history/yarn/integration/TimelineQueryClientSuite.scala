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

import java.io.IOException
import java.net.{URI, URL}

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.config.ClientConfig
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity

import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding._
import org.apache.spark.deploy.history.yarn.rest.{HttpRequestException, TimelineQueryClient}

class TimelineQueryClientSuite extends AbstractTestsWithHistoryServices {

  private var clientConfig: ClientConfig = _
  private var jerseyClient: Client = _
  private var timeline: URI = _
  var queryClient: TimelineQueryClient = _

  override def setup(): Unit = {
    super.setup()
    historyService = startHistoryService(sparkCtx)
    timeline = historyService.getTimelineServiceAddress()
    queryClient= createTimelineQueryClient()
  }

  test("About") {
    val response = queryClient.about()
    logInfo(s"$timeline/about => \n$response")
    assertNotNull(response, s"$queryClient about()")
    assertContains(response, "Timeline")
  }

  test("ListNoEntityTypes") {
    assertResult(Nil) {
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
    }
  }

  test("PostEntity") {
    describe("post an entity and then retrieve it")
    val te = new TimelineEntity
    te.setStartTime(now())
    te.setEntityId("SPARK-0001")
    te.setEntityType(SPARK_EVENT_ENTITY_TYPE)
    te.addPrimaryFilter(FILTER_APP_START, FILTER_APP_START_VALUE)

    val timelineClient = historyService.getTimelineClient
    timelineClient.putEntities(te)
    val timelineEntities: List[TimelineEntity] =
      queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE)
    assert(timelineEntities.size == 1, "empty TimelineEntity list")
    assertEquals(te, timelineEntities.head)

    val entity2 = queryClient.getEntity(SPARK_EVENT_ENTITY_TYPE, te.getEntityId() )
    assertEquals(te, entity2)

    val listing2 = queryClient.listEntities(SPARK_EVENT_ENTITY_TYPE,
                              primaryFilter = Some(FILTER_APP_START, FILTER_APP_START_VALUE))
    assertResult(1, s"filtering on $FILTER_APP_START:$FILTER_APP_START_VALUE") {
      listing2.size
    }
    //filtered query
    assertEquals(te, listing2.head)
  }

  def createTimelineClientRootPath: TimelineQueryClient = {
    val realTimelineEndpoint = historyService.getTimelineServiceAddress().toURL
    val rootTimelineServer = new URL(realTimelineEndpoint, "/").toURI
    new TimelineQueryClient(rootTimelineServer,
        sparkCtx.hadoopConfiguration,
        createClientConfig())
  }

  test("Client about() Against Wrong URL") {
    intercept[IOException] {
      val about = createTimelineClientRootPath.about()
    }
  }

  test("Client healthcheck() Against Wrong URL") {
    val client: TimelineQueryClient = createTimelineClientRootPath
    val ex = intercept[HttpRequestException] {
      client.healthCheck()
    }
    log.debug(s"GET $client", ex)
    assertContains(ex.toString(), "text/html")
  }

}


