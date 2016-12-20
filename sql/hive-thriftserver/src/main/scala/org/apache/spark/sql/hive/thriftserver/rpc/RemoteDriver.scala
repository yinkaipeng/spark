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

import java.sql.{Date, Timestamp}
import java.util.{Arrays, UUID}

import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.rsc.driver.{BypassJobWrapper, JobWrapper, RSCDriver}
import com.cloudera.livy.{Job, JobContext}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hive.service.cli._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row => SparkRow}
import org.apache.spark.util.{Utils => SparkUtils}
import org.apache.spark.{Logging, SparkConf}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class RemoteDriver(sparkConf: SparkConf, rscConf: RSCConf)
  extends RSCDriver(sparkConf, rscConf)
    with Logging {

  import RemoteDriver._

  private var result: DataFrame = _
  private var iter: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _


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

  private def getResultSchema: TableSchema = {
    if (result == null || result.queryExecution.analyzed.output.isEmpty) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      logInfo(s"Result Schema: ${result.queryExecution.analyzed.output}")
      val schema = result.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }
      new TableSchema(schema.asJava)
    }
  }

  private def addNonNullColumnValue(from: SparkRow, to: ArrayBuffer[Any], ordinal: Int) {
    dataTypes(ordinal) match {
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
        val hiveString = HiveContext.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  private def clearState(): Unit = {
    result = null
    iter = null
    dataTypes = null
  }
}

object RemoteDriver extends Logging {

  private val jobContextImplClazz =
    SparkUtils.classForName("com.cloudera.livy.rsc.driver.JobContextImpl")
  private val driverField = jobContextImplClazz.getDeclaredField("driver")
  driverField.setAccessible(true)

  private val emptyArray = new Array[Byte](0)

  class ForceHiveInitJob extends Job[Array[Byte]]() {
    override def call(jobContext: JobContext): Array[Byte] = {
      logInfo("Force initialization of hive context eagerly in a noop job")
      jobContext.hivectx()
      emptyArray
    }
  }


  def createSqlStatementRequest(statement: String, statementId: String): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug(s"SqlStatementRequest. statementId = $statementId, statement = $statement")
        jobContext.sc().setJobGroup(statementId, statement)

        assert(jobContext.getClass == jobContextImplClazz)
        val driver = driverField.get(jobContext).asInstanceOf[RemoteDriver]

        try {
          val result = jobContext.hivectx().sql(statement)
          driver.result = result
          logDebug(s"Query dataframe result: $result , ${result.queryExecution.toString()}")

          driver.iter = {
            val useIncrementalCollect = jobContext.hivectx().getConf(
              "spark.sql.thriftServer.incrementalCollect", "false").toBoolean
            if (useIncrementalCollect) {
              result.rdd.toLocalIterator
            } else {
              result.collect().iterator
            }
          }

          driver.dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
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


  def createFetchQueryOutputRequest(maxRows: Int): Job[RowBasedSet] = {
    new Job[RowBasedSet] {
      override def call(jobContext: JobContext): RowBasedSet = {

        logDebug(s"FetchQueryOutputRequest. maxRows = $maxRows")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = driverField.get(jobContext).asInstanceOf[RemoteDriver]

        if (null == driver.iter) {
          // Previous query execution failed.
          throw new NoSuchElementException("No successful query executed for output")
        }

        val schema = driver.getResultSchema
        val resultRowSet: RowBasedSet = new RowBasedSet(schema)

        val iter = driver.iter

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
                driver.addNonNullColumnValue(sparkRow, row, curCol)
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

  def createFetchResultSchemaRequest(): Job[TableSchema] = {
    new Job[TableSchema] {
      override def call(jobContext: JobContext): TableSchema = {

        logDebug("FetchResultSchemaRequest")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = driverField.get(jobContext).asInstanceOf[RemoteDriver]

        driver.getResultSchema
      }
    }
  }

  def createCancelStatementRequest(statementId: String): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug(s"CancelStatementRequest. statementId = $statementId")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = driverField.get(jobContext).asInstanceOf[RemoteDriver]

        jobContext.sc().cancelJobGroup(statementId)
        // clear state also ?
        driver.clearState()
        true
      }
    }
  }

  def createCloseOperationRequest(): Job[Boolean] = {

    new Job[Boolean] {
      override def call(jobContext: JobContext): Boolean = {

        logDebug("CloseOperationRequest")
        assert(jobContext.getClass == jobContextImplClazz)
        val driver = driverField.get(jobContext).asInstanceOf[RemoteDriver]

        // RDDs will be cleaned automatically upon garbage collection.
        jobContext.sc().clearJobGroup()

        // Also reset state - so that we dont hold references to rdd indirectly via the iterator
        // or dataframe
        driver.clearState()
        true
      }
    }
  }
}

