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

package org.apache.spark.deploy.history.yarn

import org.apache.spark.scheduler._
import org.apache.spark.util.{SystemClock, Clock}
import org.apache.spark.{Logging, SparkContext}

private[spark] class YarnEventListener(sc: SparkContext, service: YarnHistoryService)
  extends SparkListener with Logging {

  private val clock: Clock = new SystemClock()

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    enqueue(stageCompleted)
  }

  private def enqueue(sparkEvent: SparkListenerEvent): Unit = {
    service.enqueue(new HandleSparkEvent(sparkEvent, clock.getTimeMillis()))
  }

  /**
   * Called when a stage is submitted
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    enqueue(stageSubmitted)
  }

  /**
   * Called when a task starts
   */
  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    enqueue(taskStart)
  }

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    enqueue(taskGettingResult)
  }

  /**
   * Called when a task ends
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    enqueue(taskEnd)
  }

  /**
   * Called when a job starts
   */
  override def onJobStart(jobStart: SparkListenerJobStart) {
    enqueue(jobStart)
  }


  /**
   * Called when a job ends
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    enqueue(jobEnd)
  }

  /**
   * Called when environment properties have been updated
   */
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    enqueue(environmentUpdate)
  }

  /**
   * Called when a new block manager has joined
   */
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    enqueue(blockManagerAdded)
  }

  /**
   * Called when an existing block manager has been removed
   */
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    enqueue(blockManagerRemoved)
  }

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) {
    enqueue(unpersistRDD)
  }

  /**
   * Called when the application starts
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    enqueue(applicationStart)
  }

  /**
   * Called when the application ends
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    enqueue(applicationEnd)
  }

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    enqueue(executorMetricsUpdate)
  }
}
