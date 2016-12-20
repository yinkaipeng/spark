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

package org.apache.spark.sql.hive.thriftserver.server

import java.util.{Map => JMap}

import scala.collection.mutable.Map
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation}
import org.apache.hive.service.cli.operation.OperationManager
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.{ReflectionUtils, SparkExecuteStatementOperation}
import org.apache.spark.sql.hive.thriftserver.SparkExecuteRemoteStatementOperation
import org.apache.spark.sql.hive.thriftserver.rpc.RpcClient

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
private[thriftserver] class SparkSQLOperationManager()
  extends OperationManager with Logging {


  private var impersonationEnabled: Boolean = _
  private var clusterModeEnabled: Boolean = _

  private val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  private val sessionToActivePool = Map[SessionHandle, String]()
  private val sessionToContexts = Map[SessionHandle, HiveContext]()
  private val sessionToRpcClient = Map[SessionHandle, RpcClient]()

  def setClusterModeEnabled(clusterModeEnabled: Boolean): Unit = {
    this.clusterModeEnabled = clusterModeEnabled
  }

  def setImpersonationEnabled(impersonationEnabled: Boolean): Unit = {
    this.impersonationEnabled = impersonationEnabled
  }

  def runInCluster: Boolean = impersonationEnabled || clusterModeEnabled

  def addSessionContext(session: SessionHandle, context: HiveContext): Unit = {
    assert (runInCluster)
    require(! sessionToRpcClient.contains(session), "Session already has a rpc context")

    val prev = sessionToContexts.put(session, context)
    assert(prev.isEmpty || prev.get == context)
  }

  def addSessionRpcClient(session: SessionHandle, client: RpcClient): Unit = {
    assert (runInCluster)
    require(! sessionToContexts.contains(session), "Session already has a local context")

    val prev = sessionToRpcClient.put(session, client)
    assert(prev.isEmpty || prev.get == client)
  }

  def closeSession(sessionHandle: SessionHandle): Unit = {
    sessionToActivePool -= sessionHandle

    if (runInCluster) {
      val rpcOpt = sessionToRpcClient.remove(sessionHandle)
      if (rpcOpt.isDefined) {
        rpcOpt.get.stop(true)
      }
      assert (! sessionToContexts.contains(sessionHandle))
    } else {
      sessionToContexts.remove(sessionHandle)
      assert (! sessionToRpcClient.contains(sessionHandle))
    }
  }

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val state = SessionState.get()

    var operation: ExecuteStatementOperation = null

    if (runInCluster) {
      val rpcClientOpt = sessionToRpcClient.get(parentSession.getSessionHandle)
      if (rpcClientOpt.isEmpty) {
        throw new IllegalArgumentException("session not found for " +
          parentSession.getSessionHandle)
      }


      operation = new SparkExecuteRemoteStatementOperation(parentSession, statement, confOverlay,
        async)(rpcClientOpt.get)
      logDebug(s"Created Operation for $statement with session=$parentSession, " +
        s"runInBackground=$async")
    } else {
      val hiveContext = sessionToContexts(parentSession.getSessionHandle)
      val runInBackground = async && hiveContext.hiveThriftServerAsync
      operation = new SparkExecuteStatementOperation(parentSession, statement, confOverlay,
        runInBackground)(hiveContext, sessionToActivePool)
      logDebug(s"Created Operation for $statement with session=$parentSession, " +
        s"runInBackground=$runInBackground")
    }

    // parent (private) operation handleToOperation happens with lock on 'this'
    this.synchronized {
      handleToOperation.put(operation.getHandle, operation)
    }
    operation
  }
}
