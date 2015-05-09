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
package org.apache.spark.deploy.history.yarn.failures

import java.net.{NoRouteToHostException, URI}

import com.sun.jersey.api.client.config.ClientConfig
import org.apache.hadoop.conf.Configuration

import org.apache.spark.deploy.history.yarn.rest.TimelineQueryClient

/**
 * A timeline query client that always throws an exception
 */
class FailingTimelineQueryClient(timelineURI: URI, conf: Configuration, jerseyClientConfig: ClientConfig)
    extends TimelineQueryClient(timelineURI, conf, jerseyClientConfig) {

  /**
   * Throw the exception
   */
  override def innerExecAction[T](action: () => T): T = {
    throw new NoRouteToHostException(FailingTimelineQueryClient.ERROR_TEXT)
  }

  /**
   * toString method returns the URI of the timeline service
   * @return
   */
  override def toString: String = {
    "Failing "+ super.toString
  }

}

object FailingTimelineQueryClient {
  val ERROR_TEXT = "No-route-to-host"

}