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

import java.lang.{Boolean => JBoolean}
import java.util.concurrent.TimeUnit
import java.util.{Map => JMap}

import com.cloudera.livy.JobHandle
import org.apache.hadoop.hive.common.{HiveInterruptCallback, HiveInterruptUtils}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation}
import org.apache.hive.service.cli.operation.OperationManager
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.hive.thriftserver.rpc.{RpcClient, RpcUtil}

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
private[thriftserver] class SparkSQLOperationManager()
  extends OperationManager with Logging {

  // Amount of time to wait for credential update to be propagated to rsc
  private val credentialUpdateTimeout = SparkSQLEnv.sparkContext.conf.getTimeAsMs(
    "spark.sql.hive.thriftServer.remote.credential.update.timeout", "30s"
  )

  private var impersonationEnabled: Boolean = _
  private var clusterModeEnabled: Boolean = _

  private val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  private val sessionToActivePool = HashMap[SessionHandle, String]()
  private val sessionToContexts = HashMap[SessionHandle, HiveContext]()

  // rpc handle identifies an rpc connection, while sessionId identifies a session
  // within the rpc session associated with the sessionhandle
  private val sessionToRpcHandleAndSessionId = HashMap[SessionHandle, (String, String)]()
  private val rpcHandleToSessions = HashMap[String, HashSet[SessionHandle]]()
  // We need the launch user and the rpc client for an rpc handle
  private val rpcHandleToUserAndRpcClient = HashMap[String, (String, RpcClient)]()

  installSignalHandler()

  private def installSignalHandler() {
    HiveInterruptUtils.add(new HiveInterruptCallback {
      override def interrupt() {
        closeAll()
      }
    })
  }

  private def closeAll(): Unit = {

    // close the sessions outside the synchronized block
    val rpcHandleAndClient = synchronized {
      logInfo("closing all contexts and clients")
      sessionToContexts.clear()
      sessionToActivePool.clear()

      sessionToRpcHandleAndSessionId.clear()
      rpcHandleToSessions.clear()

      val clients = rpcHandleToUserAndRpcClient.map(v => (v._1, v._2._2))
      rpcHandleToUserAndRpcClient.clear()
      clients
    }

    rpcHandleAndClient.foreach(v =>
      try {
        logInfo("Closing rpc connection for rpc handle = " + v._1)
        v._2.stop(true)
      } catch {
        case ex: Exception => logInfo("Exception stopping rpc session", ex)
      })
  }

  override def stop(): Unit = {
    closeAll()
    super.stop()
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
    require(! sessionToRpcHandleAndSessionId.contains(session), "Session already has a rpc ctx")

    val prev = sessionToContexts.put(session, context)
    assert(prev.isEmpty || prev.get == context)
  }

  // returns: was remote rsc session created
  def addSessionRpcClient(user: String, sessionHandle: SessionHandle, remoteSessionId: String,
      rpcHandle: String, clientBuilder: () => RpcClient): Boolean = synchronized {

    assert (runInCluster)
    require(! sessionToContexts.contains(sessionHandle), "Session already has a local context")

    logInfo("addSessionRpcClient sessionHandle = " + sessionHandle)

    val existingRpcHandleAndSessionIdOpt = sessionToRpcHandleAndSessionId.get(sessionHandle)

    // This typically should not happen, but exists for completeness sake.
    if (existingRpcHandleAndSessionIdOpt.isDefined) {
      val existingId = existingRpcHandleAndSessionIdOpt.get._2
      assert (rpcHandleToUserAndRpcClient.contains(rpcHandle))

      logInfo(s"Session already exists for sessionHandle = $sessionHandle with id = $existingId")
      // reuse rpc and session
      return false
    }

    // Check if we can reuse an existing rpc based on sessionHandle
    val existingRpcSessionOpt = rpcHandleToUserAndRpcClient.get(rpcHandle)
    if (existingRpcSessionOpt.isDefined) {

      // If existing rpc session is stale, then create a new one
      // (but dont rewire existing to this).
      val rpcClient = existingRpcSessionOpt.get._2
      if (! rpcClient.isClosed) {
        // register the new session handle with it.
        val sessionHandles = rpcHandleToSessions.get(rpcHandle)
        assert(sessionHandles.isDefined)
        sessionHandles.get.add(sessionHandle)
        sessionToRpcHandleAndSessionId.put(sessionHandle, (rpcHandle, remoteSessionId))
        logInfo(s"Reusing existing with handle = $rpcHandle for sessionHandle = $sessionHandle")

        // Reuse rpc
        return false
      } else {
        // suffix to indicate stale.
        val newStaleHandleKey = rpcHandle + ", rpc-state = stale"
        logInfo("Found stale/closed rpc connection. rewiring existing sessions")

        rpcHandleToUserAndRpcClient.remove(rpcHandle)

        // rewrite all to new rpc handle.
        // Note: sessionHandle is untouched - but what it points to is modified.
        rpcHandleToUserAndRpcClient.put(newStaleHandleKey, (user, rpcClient))

        rpcHandleToSessions.get(rpcHandle).foreach { set =>
          set.foreach { handle =>
            logInfo("Rewiring stale/closed rpc connection for handle = " + handle +
              " to " + newStaleHandleKey)
            val currentOpt = sessionToRpcHandleAndSessionId.get(handle)
            if (currentOpt.isDefined) {
              // No need to re-wire sessionId
              sessionToRpcHandleAndSessionId.put(handle, (newStaleHandleKey, currentOpt.get._2))
            }
          }
          rpcHandleToSessions.remove(rpcHandle)
          rpcHandleToSessions.put(newStaleHandleKey, set)
        }
      }
    }

    // Create and add new.
    val newRpcClient = clientBuilder()

    rpcHandleToUserAndRpcClient.put(rpcHandle, (user, newRpcClient))
    rpcHandleToSessions.put(rpcHandle, HashSet(sessionHandle))
    sessionToRpcHandleAndSessionId.put(sessionHandle, (rpcHandle, remoteSessionId))
    logInfo(s"New rpc session created for sessionHandle= $sessionHandle, with handle= $rpcHandle")

    // new rpc session
    true
  }

  def closeSession(sessionHandle: SessionHandle): Unit = {
    val sessionIdAndRpcToClose: Option[Either[(String, RpcClient), RpcClient]] = synchronized {
      logInfo("Closing session = " + sessionHandle)

      if (runInCluster) {
        assert (! sessionToActivePool.contains(sessionHandle))
        assert (! sessionToContexts.contains(sessionHandle))
        val removedRpcHandleAndSessionIdOpt = sessionToRpcHandleAndSessionId.remove(sessionHandle)

        if (removedRpcHandleAndSessionIdOpt.isDefined) {
          val rpcHandle = removedRpcHandleAndSessionIdOpt.get._1
          val removedSessionId = removedRpcHandleAndSessionIdOpt.get._2


          val sessions = rpcHandleToSessions.get(rpcHandle).
            map { set => {set.remove(sessionHandle); set } }

          // When there are no more sessions for this rpc we need to close the rpc session,
          // otherwise we need to remove only the session id from the remote application.
          if (sessions.isEmpty || sessions.get.isEmpty) {
            logInfo("Closing rpc session for handle = " + rpcHandle)
            rpcHandleToSessions.remove(rpcHandle)
            rpcHandleToUserAndRpcClient.remove(rpcHandle).map(rpcData => Right(rpcData._2))
          } else {
            rpcHandleToUserAndRpcClient.get(rpcHandle).map(
              rpcData => Left((removedSessionId, rpcData._2)))
          }
        } else {
          None
        }
      } else {
        sessionToActivePool -= sessionHandle
        sessionToContexts.remove(sessionHandle)
        assert (! sessionToRpcHandleAndSessionId.contains(sessionHandle))
        None
      }
    }

    // execute operation on rpc client outside of synchronized block
    sessionIdAndRpcToClose.foreach {
      case Left((sessionId, rpcClient)) => unregisterRemoteSessionId(sessionId, rpcClient)
      case Right(rpcClient) => rpcClient.stop(true)
    }
  }

  private def getSessionIdAndRpcClient(handle: SessionHandle): (String, RpcClient) = synchronized {

    assert(runInCluster)
    val rpcHandleAndSessionId = sessionToRpcHandleAndSessionId.get(handle)
    if (rpcHandleAndSessionId.isEmpty) {
      throw new IllegalArgumentException("session not found for " + handle)
    }

    val rpcHandle = rpcHandleAndSessionId.get._1
    val sessionId = rpcHandleAndSessionId.get._2

    val userAndRpcClientOpt = rpcHandleToUserAndRpcClient.get(rpcHandle)
    if (userAndRpcClientOpt.isEmpty) {
      throw new IllegalArgumentException("Unable to retrieve rpc conn for session " + handle +
        " from rpc handle " + rpcHandle)
    }

    assert (rpcHandleToSessions.contains(rpcHandle))
    assert (rpcHandleToSessions(rpcHandle).contains(handle))

    (sessionId, userAndRpcClientOpt.get._2)
  }

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = {

    val state = SessionState.get()

    val operation = {
      if (runInCluster) {
        val (parentSessionId, rpcClient) = getSessionIdAndRpcClient(parentSession.getSessionHandle)

        val op = new SparkExecuteRemoteStatementOperation(parentSession, parentSessionId,
          statement, confOverlay, async)(rpcClient)
        logDebug(s"Created Operation for $statement with session=$parentSession, " +
          s"runInBackground=$async")

        op
      } else {
        synchronized {
          val hiveContext = sessionToContexts(parentSession.getSessionHandle)
          val runInBackground = async && hiveContext.hiveThriftServerAsync
          val op = new SparkExecuteStatementOperation(parentSession, statement, confOverlay,
            runInBackground)(hiveContext, sessionToActivePool)
          logDebug(s"Created Operation for $statement with session=$parentSession, " +
            s"runInBackground=$runInBackground")
          op
        }
      }
    }

    // parent (private) operation handleToOperation happens with lock on 'this'
    synchronized {
      handleToOperation.put(operation.getHandle, operation)
    }
    operation
  }

  def registerRemoteSessionId(launcherUser: String, sessionHandle: SessionHandle,
      remoteSessionId: String): Unit = {
    assert(runInCluster)
    val (parentSessionId, rpcClient) = getSessionIdAndRpcClient(sessionHandle)
    assert (parentSessionId == remoteSessionId)

    rpcClient.executeRegisterSession(launcherUser, remoteSessionId).get()
  }

  private def unregisterRemoteSessionId(sessionId: String, rpcClient: RpcClient): Unit = {
    SparkCLIServices.invokeSafelyUnit(() => rpcClient.executeUnregisterSession(sessionId).get())
  }

  def updateAllRscTokens(loggedInUGI: UserGroupInformation, renewer: String): Unit = {

    assert (UserGroupInformation.isSecurityEnabled)

    // Get the creds outside synchronized block since it can be expensive
    // we dont want to block everyone on this.
    val userToRpcClients = {
      val userAndRpcArray = synchronized {
        rpcHandleToUserAndRpcClient.values.filter(!_._2.isClosed).toArray
      }

      // Minimize number of tokens we have to acquire by doing it once for each user
      // and not once per each rpc client
      val map = new HashMap[String, ArrayBuffer[RpcClient]]()
      userAndRpcArray.foreach { case (user, rpc) =>
        map.getOrElseUpdate(user, new ArrayBuffer[RpcClient]()) += rpc
      }
      map
    }

    def processUser(user: String, rpcClients: ArrayBuffer[RpcClient]): List[JobHandle[JBoolean]] = {
      try {
        logInfo(s"Processing credentials for $user")
        val proxyUgi = UserGroupInformation.createProxyUser(user, loggedInUGI)
        val creds = new Credentials()
        SparkCLIServices.fetchCredentials(proxyUgi, creds, renewer)

        val data = RpcUtil.serializeToBytes(creds)

        rpcClients.filter(! _.isClosed).
          map(rpc => SparkCLIServices.invokeSafely(() => rpc.updateCredentials(data), null)).
          filter(_ != null).toList
      } catch {
        case ex: Exception =>
          logInfo(s"Unable to fetch credentials for $user", ex)
          List()
      }
    }

    val userToHandles = userToRpcClients.map {
      case (user, rpcClients) => user -> processUser(user, rpcClients)
    }.filter(_._2.nonEmpty)

    logInfo(s"Waiting for ${userToHandles.map(_._2.length).sum} rpc's to respond, " +
      s"users = ${userToHandles.size}")

    val updateCount: Int = {
      userToHandles.flatMap(_._2).map { handle =>
        SparkCLIServices.invokeSafely(() => {
          handle.get(credentialUpdateTimeout, TimeUnit.MILLISECONDS)
          true
        }, false)
      }.count(v => v)
    }

    logInfo(s"Clients notifying of successful credential update within timeout = $updateCount")
  }
}
