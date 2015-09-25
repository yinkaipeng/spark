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

package org.apache.spark.deploy.history.yarn.testtools

import java.util

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId, ApplicationResourceUsageReport, ApplicationReport, FinalApplicationStatus, Token, YarnApplicationState}

/**
 * This is a stub application report implementation for testing.
 * It only persists a few values, enough for the tests that use it.
 */
class StubApplicationReport extends ApplicationReport {


  private var _applicationId: ApplicationId = _

  override def getApplicationId: ApplicationId = { _applicationId }

  override def setApplicationId(applicationId: ApplicationId): Unit = {
    _applicationId = applicationId
  }

  private var _applicationAttemptId: ApplicationAttemptId = _

  override def setCurrentApplicationAttemptId(
      applicationAttemptId: ApplicationAttemptId): Unit = {
    _applicationAttemptId = applicationAttemptId
  }

  override def getCurrentApplicationAttemptId: ApplicationAttemptId = {
    _applicationAttemptId
  }

  private var _finalApplicationStatus: FinalApplicationStatus = _
  override def setFinalApplicationStatus(finishState: FinalApplicationStatus): Unit = {
    _finalApplicationStatus = finishState
  }
  override def getFinalApplicationStatus: FinalApplicationStatus = { _finalApplicationStatus }

  private var _applicationType: String = ""
  override def setApplicationType(applicationType: String): Unit = {
    _applicationType = applicationType
  }

  override def getApplicationType: String = { _applicationType }

  private var _name: String = _
  override def getName: String = { _name }
  override def setName(name: String): Unit = {_name = name}


  private var _yarnApplicationState: YarnApplicationState = _
  override def setYarnApplicationState(state: YarnApplicationState): Unit = {
    _yarnApplicationState = state
  }
  override def getYarnApplicationState: YarnApplicationState = { _yarnApplicationState }

  private var _finishTime: Long = 0L
  override def setFinishTime(finishTime: Long): Unit = { _finishTime = finishTime}
  override def getFinishTime: Long = { _finishTime}

  private var _startTime: Long = 0L
  override def setStartTime(startTime: Long): Unit = { _startTime = startTime}
  override def getStartTime: Long = { _startTime}

  override def setApplicationResourceUsageReport(
      appResources: ApplicationResourceUsageReport): Unit = {}

  override def getApplicationResourceUsageReport: ApplicationResourceUsageReport = { null }

  override def setOriginalTrackingUrl(url: String): Unit = {  }

  override def getDiagnostics: String = { "" }

  override def getApplicationTags: util.Set[String] = { null }

  override def getClientToAMToken: Token = { null }

  override def setQueue(queue: String): Unit = {}


  override def getUser: String = { "" }

  override def setProgress(progress: Float): Unit = {}

  override def getProgress: Float = { 0 }

  override def getHost: String = { "" }

  override def getTrackingUrl: String = { "" }

  override def getRpcPort: Int = { 0 }

  override def setUser(user: String): Unit = {}

  override def setApplicationTags(tags: util.Set[String]): Unit = {}

  override def getOriginalTrackingUrl: String = { "" }

  override def setDiagnostics(diagnostics: String): Unit = {}


  override def getAMRMToken: Token = { null }

  override def setRpcPort(rpcPort: Int): Unit = {}

  override def setClientToAMToken(clientToAMToken: Token): Unit = {}

  override def setHost(host: String): Unit = {}

  override def setAMRMToken(amRmToken: Token): Unit = {}

  override def getQueue: String = { "" }

  override def setTrackingUrl(url: String): Unit = {}
}
