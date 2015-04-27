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
import org.apache.spark.{Logging, SparkContext}

private[spark] class YarnEventListener(sc: SparkContext, service: YarnHistoryService)
  extends SparkListener with Logging {

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    service.enqueue(new HandleSparkEvent(stageCompleted, now()))
  }

  /**
   * Source of current time; may be overridden in tests
   * @return
   */
  protected def now(): Long = {
    System.currentTimeMillis
  }

  /**
   * Called when a stage is submitted
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    service.enqueue(new HandleSparkEvent(stageSubmitted, now()))
  }

  /**
   * Called when a task starts
   */
  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    service.enqueue(new HandleSparkEvent(taskStart, now()))
  }

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    service.enqueue(new HandleSparkEvent(taskGettingResult, now()))
  }

  /**
   * Called when a task ends
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    service.enqueue(new HandleSparkEvent(taskEnd, now()))
  }

  /**
   * Called when a job starts
   */
  override def onJobStart(jobStart: SparkListenerJobStart) {
    service.enqueue(new HandleSparkEvent(jobStart, now()))
  }


  /**
   * Called when a job ends
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    service.enqueue(new HandleSparkEvent(jobEnd, now()))
  }

  /**
   * Called when environment properties have been updated
   */
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    service.enqueue(new HandleSparkEvent(environmentUpdate, now()))
  }

  /**
   * Called when a new block manager has joined
   */
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    service.enqueue(new HandleSparkEvent(blockManagerAdded, now()))
  }

  /**
   * Called when an existing block manager has been removed
   */
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    service.enqueue(new HandleSparkEvent(blockManagerRemoved, now()))
  }

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) {
    service.enqueue(new HandleSparkEvent(unpersistRDD, now()))
  }

  /**
   * Called when the application starts
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    service.enqueue(new HandleSparkEvent(applicationStart, now()))
  }

  /**
   * Called when the application ends
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    service.enqueue(new HandleSparkEvent(applicationEnd, now()))
  }

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    service.enqueue(new HandleSparkEvent(executorMetricsUpdate, now()))
  }
}
