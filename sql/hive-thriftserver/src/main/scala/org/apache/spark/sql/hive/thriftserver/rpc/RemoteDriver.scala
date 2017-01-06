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

package org.apache.spark.sql.hive.thriftserver.rpc

import java.lang.{Boolean => JBoolean}
import java.sql.{Date, Timestamp}
import java.util.{UUID, Arrays => JArrays, HashMap => JHashMap}

import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.rsc.driver.{BypassJobWrapper, RSCDriver}
import com.cloudera.livy.{Job, JobContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hive.service.cli._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row => SparkRow}
import org.apache.spark.util.{Utils => SparkUtils}
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class RemoteDriver(sparkConf: SparkConf, rscConf: RSCConf)
  extends RSCDriver(sparkConf, rscConf)
    with Logging {

  import RemoteDriver._

  private val sessionIdToHiveContext = new JHashMap[String, HiveContext]()

  private val statementIdToDf = new JHashMap[String, DataFrame]()
  private val statementIdToResultIter = new JHashMap[String, Iterator[SparkRow]]()
  private val statementIdToDataTypes = new JHashMap[String, Array[DataType]]()

  private val tokenDistributer = new YarnDelegationTokenDistributer(sparkConf)


  override def initializeContext(): JavaSparkContext = {

    val ctx = super.initializeContext()

    // Always enqueue a job to force initialization of hive context.
    // Otherwise, the metastore connect happens on first user query.
    // Note, since initialization is not yet complete, this will always get
    // enqueued for processing once init is done.
    // Note: using BytepassJobWrapper instead of JobWrapper so that result is not
    // notified to the client - since this is a synthetic job not launched by client
    super.submit(new BypassJobWrapper(this, UUID.randomUUID().toString, new ForceHiveInitJob()))

    ctx
  }

  private def getResultSchema(statementId: String): TableSchema = {
    val result = getResultDf(statementId)

    if (result == null || result.queryExecution.analyzed.output.isEmpty) {
      new TableSchema(JArrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      logInfo(s"Result Schema: ${result.queryExecution.analyzed.output}")
      val schema = result.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }
      new TableSchema(schema.asJava)
    }
  }

  private def addNonNullColumnValue(statementId: String,
      from: SparkRow, to: ArrayBuffer[Any], ordinal: Int) {

    val dataType = getDataTypes(statementId)(ordinal)
    dataType match {
      case StringType =>
        to += from.getString(ordinal)
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType() =>
        to += from.getDecimal(ordinal)
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case DateType =>
        to += from.getAs[Date](ordinal)
      case TimestampType =>
        to +=  from.getAs[Timestamp](ordinal)
      case BinaryType | _: ArrayType | _: StructType | _: MapType =>
        val hiveString = HiveContext.toHiveString((from.get(ordinal),
          dataType))
        to += hiveString
    }
  }

  private def getResultDf(statementId: String): DataFrame = synchronized {
    statementIdToDf.get(statementId)
  }

  private def getDataTypes(statementId: String): Array[DataType] = synchronized {
    statementIdToDataTypes.get(statementId)
  }

  private def getResultIter(statementId: String): Iterator[SparkRow] = synchronized {
    statementIdToResultIter.get(statementId)
  }

  private def addResult(statementId: String, df: DataFrame,
      iter: Iterator[SparkRow], dataTypes: Array[DataType]): Unit = synchronized {

    statementIdToDf.put(statementId, df)
    statementIdToResultIter.put(statementId, iter)
    statementIdToDataTypes.put(statementId, dataTypes)
  }

  private def clearState(statementId: String): Unit = synchronized {
    statementIdToDf.remove(statementId)
    statementIdToResultIter.remove(statementId)
    statementIdToDataTypes.remove(statementId)
  }

  def updateCredentials(newCredentials: Credentials): Unit = {
    if (! SparkHadoopUtil.get.isYarnMode()) {
      // Should not be called in non-yarn mode. Not making it fatal to
      // help with code evolution
      logError("Ignoring credentials update when yarn mode = false")
      return
    }

    if (! UserGroupInformation.isSecurityEnabled) {
      logError("ugi security not enabled, ignoring credential update")
      return
    }

    // Add to ugi before distributing the tokens.
    UserGroupInformation.getCurrentUser.addCredentials(newCredentials)
    tokenDistributer.distributeTokens(newCredentials)
  }

  private def registerSession(userName: String, jobContext: JobContext, parentSessionId: String,
      duplicateAllowed: Boolean): Unit = synchronized {

    if (! sessionIdToHiveContext.containsKey(parentSessionId)) {
      val ctx = jobContext.hivectx().newSession()

      ctx.setUser(userName)
      ctx.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)

      sessionIdToHiveContext.put(parentSessionId, ctx)
    } else if (! duplicateAllowed) {
      logError(s"Duplicate registerSession for parentSessionId = $parentSessionId")
    }
  }

  private def unregisterSession(jobContext: JobContext, parentSessionId: String,
      defaultSession: Boolean): Unit = synchronized {

    // Do not remove default session
    if (! defaultSession) {
      val removed = sessionIdToHiveContext.remove(parentSessionId)
      if (null == removed) {
        logError(s"Unable to remove registered session for parentSessionId = $parentSessionId")
      }
    }
  }

  private def getHiveContext(parentSessionId: String): HiveContext = synchronized {
    sessionIdToHiveContext.get(parentSessionId)
  }
}

object RemoteDriver extends Logging {

  val SHARED_SINGLE_SESSION = "shared-single-session"

  private val jobGroupLock = new java.lang.Object()

  private val jobContextImplClazz =
    SparkUtils.classForName("com.cloudera.livy.rsc.driver.JobContextImpl")
  private val driverField = jobContextImplClazz.getDeclaredField("driver")
  driverField.setAccessible(true)

  private val emptyArray = new Array[Byte](0)

  // DelegationTokenDistributer is available only in yarn module - so use reflection to create
  // and invoke its methods.
  private class YarnDelegationTokenDistributer(sparkConf: SparkConf) {
    // Must be called only in yarn mode - else the reflection based code will fail
    assert (SparkHadoopUtil.get.isYarnMode())

    private val distributerInstance = {
      val cls = SparkUtils.classForName("org.apache.spark.deploy.yarn.DelegationTokenDistributer")
      cls.getConstructor(classOf[SparkConf], classOf[Configuration]).newInstance(
        sparkConf, SparkHadoopUtil.get.newConfiguration(sparkConf))
    }

    private val distributeTokensMethod = distributerInstance.getClass.getMethod(
      "distributeTokens", classOf[Credentials])

    def distributeTokens(creds: Credentials): Unit = {
      distributeTokensMethod.invoke(distributerInstance, creds)
    }
  }

  class ForceHiveInitJob extends Job[Array[Byte]]() {
    override def call(jobContext: JobContext): Array[Byte] = {
      logInfo("Force initialization of hive context eagerly in a noop job")
      jobContext.hivectx()
      emptyArray
    }
  }

  private def getDriver(jobContext: JobContext): RemoteDriver =
    driverField.get(jobContext).asInstanceOf[RemoteDriver]


  def createSqlStatementRequest(parentSessionId: String, statementId: String,
      statement: String): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug(s"SqlStatementRequest. statementId = $statementId, statement = $statement")
        jobGroupLock.synchronized {
          jobContext.sc().setJobGroup(statementId, statement)
        }

        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        try {
          val hiveContext = driver.getHiveContext(parentSessionId)
          if (null == hiveContext) {
            logError(s"Unable to find hive context for $parentSessionId")
            throw new IllegalStateException(s"parent session for $parentSessionId not found")
          }
          val result = hiveContext.sql(statement)
          logDebug(s"Query dataframe result: $result , ${result.queryExecution.toString()}")

          val iter = {
            val useIncrementalCollect = hiveContext.getConf(
              "spark.sql.thriftServer.incrementalCollect", "false").toBoolean
            if (useIncrementalCollect) {
              result.rdd.toLocalIterator
            } else {
              result.collect().iterator
            }
          }
          val dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray

          driver.addResult(statementId, result, iter, dataTypes)
        } catch {
          case e: Throwable =>
            logError(s"Error executing query", e)
            // rethrow so that remote (requesting) client will see 'why' the failure.
            throw e
        }

        true
      }
    }
  }

  def createRegisterSessionRequest(userName: String, parentSessionId: String): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug(s"RegisterSessionRequest. parentSessionId = $parentSessionId")

        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        driver.registerSession(userName, jobContext, parentSessionId,
          RemoteDriver.SHARED_SINGLE_SESSION.equals(parentSessionId))

        true
      }
    }
  }

  def createUnregisterSessionRequest(parentSessionId: String): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug(s"UnregisterSessionRequest. parentSessionId = $parentSessionId")

        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        driver.unregisterSession(jobContext, parentSessionId,
          RemoteDriver.SHARED_SINGLE_SESSION.equals(parentSessionId))

        true
      }
    }
  }


  def createFetchQueryOutputRequest(statementId: String,
      maxRows: Int): Job[ResultSetWrapper] = {

    new Job[ResultSetWrapper] {
      override def call(jobContext: JobContext): ResultSetWrapper = {

        logDebug(s"FetchQueryOutputRequest. maxRows = $maxRows")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        val iter = driver.getResultIter(statementId)

        if (null == iter) {
          // Previous query execution failed.
          throw new NoSuchElementException("No successful query executed for output")
        }

        val schema = driver.getResultSchema(statementId)
        val resultRowSet = ResultSetWrapper.create(schema)

        if (!iter.hasNext) {
          resultRowSet
        } else {
          var curRow = 0
          while (curRow < maxRows && iter.hasNext) {
            val sparkRow = iter.next()
            val row = ArrayBuffer[Any]()
            var curCol = 0
            while (curCol < sparkRow.length) {
              if (sparkRow.isNullAt(curCol)) {
                row += null
              } else {
                driver.addNonNullColumnValue(statementId, sparkRow, row, curCol)
              }
              curCol += 1
            }
            resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
            curRow += 1
          }
          resultRowSet
        }
      }
    }
  }

  def createFetchResultSchemaRequest(statementId: String): Job[TableSchema] = {
    new Job[TableSchema] {
      override def call(jobContext: JobContext): TableSchema = {

        logDebug("FetchResultSchemaRequest")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        driver.getResultSchema(statementId)
      }
    }
  }

  def createCancelStatementRequest(statementId: String): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug(s"CancelStatementRequest. statementId = $statementId")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        jobGroupLock.synchronized {
          jobContext.sc().cancelJobGroup(statementId)
          clearJobGroup(jobContext, statementId)
        }

        // clear state also ?
        driver.clearState(statementId)
        true
      }
    }
  }

  private def clearJobGroup(jobContext: JobContext, statementId: String): Unit = {
    jobGroupLock.synchronized {
      // Clear job group only if current job group is same as expected job group.
      if (statementId == jobContext.sc().getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)) {
        jobContext.sc().clearJobGroup()
      }
    }
  }


  def createCloseOperationRequest(statementId: String): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug("CloseOperationRequest")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        // RDDs will be cleaned automatically upon garbage collection.
        clearJobGroup(jobContext, statementId)

        // Also reset state - so that we dont hold references to rdd indirectly via the iterator
        // or dataframe
        driver.clearState(statementId)
        true
      }
    }
  }

  def createUpdateTokensRequest(serializedCreds: Array[Byte]): Job[JBoolean] = {
    new Job[JBoolean] {
      override def call(jobContext: JobContext): JBoolean = {
        logInfo("Updating credentials through UpdateTokensRequest")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = getDriver(jobContext)

        val newCredentials = new Credentials()
        RpcUtil.deserializeFromBytes(newCredentials, serializedCreds)

        driver.updateCredentials(newCredentials)

        JBoolean.TRUE
      }
    }
  }
}

