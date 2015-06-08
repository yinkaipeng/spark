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

import java.io.File

import org.apache.spark.deploy.history.yarn.YarnTestUtils._
import org.apache.spark.deploy.history.yarn.{HistoryServiceListeningToSparkContext, TimelineSingleEntryBatchSize}

/**
 * full hookup from spark context to timeline.
 */
class ContextEventPublishingSuite
    extends AbstractTestsWithHistoryServices
    with HistoryServiceListeningToSparkContext
    with TimelineSingleEntryBatchSize {

  test ("Publish events via Context") {
    describe("Publish events via Context")
    // hook up to spark context
    historyService = startHistoryService(sparkCtx)
    assert(historyService.listening, s"listening $historyService")
    assertResult(1, s"batch size in $historyService") {
      historyService.getBatchSize
    }
    assert(historyService.bondedToATS, s"not bonded to ATS: $historyService")
    // post in an app start
    var flushes = 0
    logDebug("posting app start")
    postEvent(appStartEvent(), now())
    flushes += 1
    awaitEmptyQueue(historyService, 5000)

    // add a local file
    val tempFile: File = File.createTempFile("test",".txt")
    sparkCtx.addFile(tempFile.toURI.toString)
    tempFile.delete()

    // closing context generates an application stop
    val endtime = now()
    logDebug("stopping context")

    sparkCtx.stop()
    awaitEmptyQueue(historyService, 5000)

    flushes += 1
    logDebug(s"status: $historyService")

    // seeing intermittent failure
    awaitFlushCount(historyService, 2, 5000)
    flushHistoryServiceToSuccess()
  }

}
