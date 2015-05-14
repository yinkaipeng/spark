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

import java.io.FileNotFoundException
import java.net.{URI, URL}
import javax.ws.rs.core.MediaType

import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntities, TimelineEntity}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding
import org.apache.spark.deploy.history.yarn.{YarnHistoryService, YarnTimelineUtils}

class BasicTimelineGetSuite extends AbstractTestsWithHistoryServices {

  protected var timelineAddress: URL = _

  override def setup(): Unit = {
    super.setup()
    historyService = startHistoryService(sparkCtx)
    val tlUri: URI = historyService.getTimelineServiceAddress()
    timelineAddress = new URL(tlUri.toURL, YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
  }


  test("AssertNotListening") {
    assertNotListening(historyService)
  }

  test("GET ATS paths") {
    describe("GET operations against ATS at HttpConnection level")
    val connector = createUrlConnector()
    val outcome = connector.execHttpOperation("GET", timelineAddress, null, "")
    logInfo(s"$timelineAddress => $outcome")
    assertResult("application/json", s"content type of $outcome") {
      outcome.contentType
    }
    val body = outcome.responseBody
    logInfo(s"body = ${body}")
    // parse the JSON
    val json = parse(body)
    val entities = json \ "entities"
    assert(entities.isInstanceOf[JArray], s"wrong type for $entities from $body")
    val entityArr = entities.asInstanceOf[JArray]
    assert(entityArr.arr.isEmpty, s"non empty list of entries in $body")
  }


  test("Jersey Client GET /") {
    describe("GET operations against ATS via Jersey")
    val clientConfig = JerseyBinding.createClientConfig()
    val jerseyClient = JerseyBinding
        .createJerseyClient(sparkCtx.hadoopConfiguration, clientConfig)
    val timeline: URI = timelineAddress.toURI
    val resource = jerseyClient.resource(timeline)
    try {

      val body = resource.accept(MediaType.APPLICATION_JSON_TYPE).get(classOf[String])
      logInfo(s"body = ${body}")

      val entities = resource.accept(MediaType.APPLICATION_JSON_TYPE)
          .get(classOf[TimelineEntities])
      assertResult(0) {
        entities.getEntities.size()
      }
    } catch {
      case e: Exception =>
        throw JerseyBinding.translateException("GET", timeline, e)
    }
    // now ask for a URL that isn't there
    val applicationURI: URI = YarnTimelineUtils.applicationURI(timeline, "unknown")
    intercept[FileNotFoundException] {
      try {
        jerseyClient.resource(applicationURI)
            .accept(MediaType.APPLICATION_JSON)
            .get(classOf[TimelineEntity])
      } catch {
        case e: Exception =>
          throw JerseyBinding.translateException("GET", applicationURI, e)
      }
    }
  }



}


