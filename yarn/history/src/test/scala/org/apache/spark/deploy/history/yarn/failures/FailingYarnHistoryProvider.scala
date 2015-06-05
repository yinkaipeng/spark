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

import java.net.URI

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.YarnHistoryProvider
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, TimelineQueryClient}

/**
 * This is a YARN history provider that can be given
 * a (possibly failing) query client, and can be configured
 * as to whether to start with a health check
 * @param queryClient query client
 * @param healthAlreadyChecked should the initial health
 *                             check be skipped? It will if this
 *                             is true
 * @param endpoint URI of the service.
 */
class FailingYarnHistoryProvider(
    queryClient: TimelineQueryClient,
    healthAlreadyChecked: Boolean,
    endpoint: URI,
    sparkConf: SparkConf = new SparkConf()) extends YarnHistoryProvider(sparkConf) {

  private var _enabled: Boolean = true

  init()

  /**
   * Any initialization logic
   */
  private def init(): Unit = {
    getHealthFlag().set(healthAlreadyChecked)
  }


  /**
   * the current enabled flag value, which can be dynamically changed
   */
  override def enabled: Boolean = {
    _enabled
  }

  /**
   * Update the enabled flag
   * @param b new value
   */
  def setEnabled(b: Boolean): Unit = {
    _enabled = b
  }

  /**
   *
   * @return the endpoint
   */
  override def getEndpointURI(): URI = {
    endpoint
  }

  /**
   * @return the `queryClient` field.
   */
  override protected def createTimelineQueryClient(): TimelineQueryClient = {
    queryClient
  }

  /**
   * @return the `queryClient` field.
   */
  override def getTimelineQueryClient(): TimelineQueryClient = {
    queryClient
  }

  /**
   * Set the health checked flag to the desired value
   * @param b
   */
  def setHealthChecked(b: Boolean): Unit = {
    getHealthFlag().set(b)
  }

}

/**
 * Some operations to help the failure tests
 */
object FailingYarnHistoryProvider {

  def createQueryClient(): FailingTimelineQueryClient = {
    new FailingTimelineQueryClient(new URI("http://localhost:80/"),
                                    new Configuration(),
                                    JerseyBinding.createClientConfig())
  }


  /**
   * This inner provider calls most of its internal methods.
   * @return
   */
  def createFailingProvider(healthAlreadyChecked: Boolean = false): YarnHistoryProvider = {
    val failingClient = createQueryClient()
    new FailingYarnHistoryProvider(failingClient,
                                    healthAlreadyChecked,
                                    new URI("http://localhost:80/"))
  }
}