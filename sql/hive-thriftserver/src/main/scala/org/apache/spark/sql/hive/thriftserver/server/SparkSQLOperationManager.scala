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

import org.apache.hadoop.hive.common.{HiveInterruptCallback, HiveInterruptUtils}

import scala.collection.mutable.{Map, Set}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation}
import org.apache.hive.service.cli.operation.OperationManager
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.{ReflectionUtils, SparkExecuteRemoteStatementOperation, SparkExecuteStatementOperation, SparkSQLEnv}
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

  private val sessionToRpcHandle = Map[SessionHandle, String]()
  private val rpcHandleToSessions = Map[String, Set[SessionHandle]]()
  private val rpcHandleToRpcClient = Map[String, RpcClient]()

  installSignalHandler()

  private def installSignalHandler() {
    HiveInterruptUtils.add(new HiveInterruptCallback {
      override def interrupt() {
        closeAll()
      }
    })
  }

  private def closeAll(): Unit = synchronized {
    sessionToContexts.clear()
    sessionToActivePool.clear()

    sessionToRpcHandle.clear()
    rpcHandleToSessions.clear()
    try {
      rpcHandleToRpcClient.foreach(v => {
        try {
          v._2.stop(true)
        } catch {
          case ex: Exception => logInfo("Exception stopping rpc session", ex)
        }
      })
    } finally {
      rpcHandleToRpcClient.clear()
    }
  }


  def setClusterModeEnabled(clusterModeEnabled: Boolean): Unit = {
    this.clusterModeEnabled = clusterModeEnabled
  }

  def setImpersonationEnabled(impersonationEnabled: Boolean): Unit = {
    this.impersonationEnabled = impersonationEnabled
  }

  def runInCluster: Boolean = impersonationEnabled || clusterModeEnabled

  def addSessionContext(session: SessionHandle, context: HiveContext): Unit = synchronized {
    assert (! runInCluster)
    require(! sessionToRpcHandle.contains(session), "Session already has a rpc context")

    val prev = sessionToContexts.put(session, context)
    assert(prev.isEmpty || prev.get == context)
  }

  def addSessionRpcClient(session: SessionHandle, rpcHandle: String,
      clientBuilder: () => RpcClient): Unit = synchronized {
    assert (runInCluster)
    require(! sessionToContexts.contains(session), "Session already has a local context")

    logInfo("addSessionRpcClient session = " + session)

    val existingRpcHandle = sessionToRpcHandle.get(session)

    // This typically should not happen, but exists for completeness sake.
    if (existingRpcHandle.isDefined) {
      assert(rpcHandleToRpcClient.contains(rpcHandle))
      logInfo("Exists for session = " + session)
      return
    }

    // Check if we can reuse an existing rpc based on session handle
    val existingRpcSessionOpt = rpcHandleToRpcClient.get(rpcHandle)
    if (existingRpcSessionOpt.isDefined) {

      // If existing rpc session is stale, then create a new one
      // (but dont rewire existing to this).
      val rpcClient = existingRpcSessionOpt.get
      if (! rpcClient.isClosed) {
        // register the new session handle with it.
        val sessionHandles = rpcHandleToSessions.get(rpcHandle)
        assert(sessionHandles.isDefined)
        sessionHandles.get.add(session)
        sessionToRpcHandle.put(session, rpcHandle)
        logInfo("Reusing existing with handle = " + rpcHandle + " for session = " + session)
        return
      } else {
        // suffix to indicate stale.
        val newStaleHandleKey = rpcHandle + ", rpc-state = stale"
        logInfo("Found stale/closed rpc connection. rewiring existing sessions")

        rpcHandleToRpcClient.remove(rpcHandle)

        // rewrite all to new rpc handle.
        // Note: session handle is untouched - but what it points to is modified.
        rpcHandleToRpcClient.put(newStaleHandleKey, rpcClient)

        rpcHandleToSessions.get(rpcHandle).foreach {
          set => {
            set.foreach {
              handle => {
                logInfo("Rewiring stale/closed rpc connection for handle = " + handle +
                  " to " + newStaleHandleKey)
                sessionToRpcHandle.put(handle, newStaleHandleKey)
              }
            }
            rpcHandleToSessions.remove(rpcHandle)
            rpcHandleToSessions.put(newStaleHandleKey, set)
          }
        }
      }
    }

    // Create and add new.
    val newRpcClient = clientBuilder()

    rpcHandleToRpcClient.put(rpcHandle, newRpcClient)
    rpcHandleToSessions.put(rpcHandle, Set(session))
    sessionToRpcHandle.put(session, rpcHandle)
    logInfo("New rpc session created for session = " + session + ", with handle = " + rpcHandle)
  }

  def closeSession(sessionHandle: SessionHandle): Unit = synchronized {
    logInfo("Closing session = " + sessionHandle)
    sessionToActivePool -= sessionHandle

    if (runInCluster) {
      val rpcHandleOpt = sessionToRpcHandle.remove(sessionHandle)
      if (rpcHandleOpt.isDefined) {
        val rpcHandle = rpcHandleOpt.get
        val sessions = rpcHandleToSessions.get(rpcHandle).
          map(set => { set.remove(sessionHandle); set })

        if (sessions.isEmpty || sessions.get.isEmpty) {
          logInfo("Closing rpc session for handle = " + rpcHandle)
          val rpcOpt = rpcHandleToRpcClient.get(rpcHandle)
          if (rpcOpt.isDefined) rpcOpt.get.stop(true)
          rpcHandleToRpcClient.remove(rpcHandle)
          rpcHandleToSessions.remove(rpcHandle)
        }
      }
      assert (! sessionToContexts.contains(sessionHandle))
    } else {
      sessionToContexts.remove(sessionHandle)
      assert (! sessionToRpcHandle.contains(sessionHandle))
    }
  }

  private def lookupRpcClientForSessionHandle(handle: SessionHandle): RpcClient = synchronized {
    assert(runInCluster)
    val rpcHandleOpt = sessionToRpcHandle.get(handle)
    if (rpcHandleOpt.isEmpty) {
      throw new IllegalArgumentException("session not found for " + handle)
    }

    val rpcClientOpt = rpcHandleToRpcClient.get(rpcHandleOpt.get)
    if (rpcClientOpt.isEmpty) {
      throw new IllegalArgumentException("Unable to retrieve rpc conn for session " + handle +
        " from rpc handle " + rpcHandleOpt.get)
    }

    assert (rpcHandleToSessions.contains(rpcHandleOpt.get))
    assert (rpcHandleToSessions(rpcHandleOpt.get).contains(handle))

    rpcClientOpt.get
  }

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val state = SessionState.get()

    var operation: ExecuteStatementOperation = null

    if (runInCluster) {
      val rpcClient = lookupRpcClientForSessionHandle(parentSession.getSessionHandle)


      operation = new SparkExecuteRemoteStatementOperation(parentSession, statement, confOverlay,
        async)(rpcClient)
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
