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
package org.apache.spark.deploy.history.yarn

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf

/**
 * A subclass of the history provider which uses the `time` field rather than
 * the current clock. This is needed to reliably test windowed operations
 * and other actions in which the clock is checked.
 * @param sparkConf configuration of the provider
 * @param t
 */
class TimeManagedHistoryProvider(sparkConf: SparkConf, var t:Long = 0L)
    extends YarnHistoryProvider(sparkConf){

  private val time = new AtomicLong(t)

  /**
   * Return the current time
   * @return
   */
  override def now(): Long = {
    time.get()
  }

  def setTime(t: Long): Unit = {
    time.set(t);
  }

  /**
   * Increase the time by one tick
   * @return the new value
   */
  def tick(): Long = {
    time.incrementAndGet()
  }

  def incrementTime(t: Long): Long = {
    time.addAndGet(t)
  }
}
