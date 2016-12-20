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

package org.apache.spark.sql.hive.thriftserver

import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.RejectedExecutionException
import java.util.{Map => JMap}

import scala.util.control.NonFatal
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.Logging
import org.apache.spark.sql.hive.thriftserver.rpc.RpcClient


private[hive] class SparkExecuteRemoteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)
    (rpcClient: RpcClient)
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
  with Logging {

  private var statementId: String = _

  override def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)

    // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
    val maxRows = maxRowsL.toInt
    val res = rpcClient.fetchResult(maxRows).get()

    res
  }

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      execute()
    } else {
      val sparkServiceUGI = Utils.getUGI()

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException =>
                  setOperationException(e)
                  log.error("Error running hive query: ", e)
              }
            }
          }

          try {
            sparkServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              logError("Error running hive query as user : " +
                sparkServiceUGI.getShortUserName(), e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle =
          parentSession.getSessionManager().submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          logError(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          throw e
      }
    }
  }

  protected def execute(): Unit = {
    statementId = UUID.randomUUID().toString
    logInfo(s"Running query '$statement' with $statementId")

    setState(OperationState.RUNNING)

    try {
      rpcClient.executeSql(statement, statementId).get()
    } catch {
      case e: Throwable =>
        val currentState = getStatus().getState()
        logError(s"Error executing query, currentState $currentState, ", e)
        setState(OperationState.ERROR)
        throw new HiveSQLException(e.toString)
    }
    setState(OperationState.FINISHED)
  }

  def close(): Unit = {
    logInfo(s"close '$statement' with $statementId")
    // Setting state after rpc request is sent.
    rpcClient.closeOperation()
    cleanup(OperationState.CLOSED)
  }

  override def cancel(): Unit = {
    logInfo(s"Cancel '$statement' with $statementId")
    if (statementId != null) {
      rpcClient.cancelStatement(statementId)
    }
    cleanup(OperationState.CANCELED)
  }

  private def cleanup(state: OperationState) {
    setState(state)
  }

  def getResultSetSchema: TableSchema = {
    rpcClient.fetchResultSchema().get()
  }
}
