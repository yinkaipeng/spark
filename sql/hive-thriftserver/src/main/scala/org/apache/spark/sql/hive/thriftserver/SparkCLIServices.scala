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

import java.lang.{Object => JObject}
import java.security.PrivilegedExceptionAction

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hive.service.cli.CLIService
import org.apache.hive.service.cli.thrift.{TOpenSessionReq, TOpenSessionResp, TProtocolVersion}
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.thriftserver.rpc.ResultSetWrapper

/**
  * Subclasses for http/thrift cli service, where we also explicitly set the version.
  */
private[hive] object SparkCLIServices extends Logging {

  val version = {
    if (ResultSetWrapper.enableColumnSet) {
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6
    } else {
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5
    }
  }

  private val hadoopUtil = SparkHadoopUtil.get

  // Force spark yarn mode
  private val freshHadoopConf = hadoopUtil.getConfBypassingFSCache(
    SparkSQLEnv.sparkContext.hadoopConfiguration,
    // Assuming that STS and all users have the same FileSystem on which applications
    // are launched from
    new Path(".").toUri.getScheme)

  private def setProtocolVersion(session: TOpenSessionResp): TOpenSessionResp = {
    session.setServerProtocolVersion(version)
    session
  }

  class SparkThriftHttpCLIService(cliService: CLIService)
    extends ThriftHttpCLIService(cliService) {

    override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
      setProtocolVersion(super.OpenSession(req))
    }
  }

  class SparkThriftBinaryCLIService(cliService: CLIService)
    extends ThriftBinaryCLIService(cliService) {

    override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
      setProtocolVersion(super.OpenSession(req))
    }
  }

  // Util method to fetch required credentials
  def fetchCredentials(ugi: UserGroupInformation, creds: Credentials, renewer: String): Boolean = {

    if (! hadoopUtil.isYarnMode()) {
      logInfo("Not in yarn mode, skipping updating credentials")
      return false
    }

    ugi.doAs(new PrivilegedExceptionAction[Void] {
      // Get a copy of the credentials
      override def run(): Void = {
        val sparkConf = SparkSQLEnv.sparkContext.conf

        fetchAllTokens(creds, sparkConf, renewer)
        null
      }
    })

    true
  }

  // Use reflection to invoke YarnSparkHadoopUtil methods - since we dont have direct access
  // to that class from yarn module
  private def invokeMethod[R](obj: AnyRef, methodName: String,
      argClasses: List[Class[_]], args: JObject*): R = {

    val method = obj.getClass.getMethod(methodName, argClasses: _*)
    method.invoke(obj, args: _*).asInstanceOf[R]
  }

  private def fetchAllTokens(creds: Credentials, sparkConf: SparkConf, renewer: String): Unit = {

    val nns = invokeMethod[Set[Path]](hadoopUtil, "getNameNodesToAccess",
      List(classOf[SparkConf]), sparkConf) +
      // Assuming that STS and all users have the same FileSystem on which
      // applications are launched from.
      new Path(".")

    invokeMethod[Void](hadoopUtil, "obtainTokensForNamenodes",
      List(classOf[Set[Path]], classOf[Configuration], classOf[Credentials],
        // optional arguments must also be specified with the default expicitly
        classOf[Option[String]]),
      nns, freshHadoopConf, creds, Some(renewer))

    invokeMethod[Void](hadoopUtil, "obtainTokenForHiveMetastore",
      List(classOf[SparkConf], classOf[Configuration], classOf[Credentials]),
      sparkConf, freshHadoopConf, creds)

    invokeMethod[Void](hadoopUtil, "obtainTokenForHBase",
      List(classOf[SparkConf], classOf[Configuration], classOf[Credentials]),
      sparkConf, freshHadoopConf, creds)
  }

  def invokeSafelyUnit(block: () => Unit): Unit = {
    invokeSafely(() => block(), Unit)
  }

  def invokeSafely[R](block: () => R, failValue: R): R = {
    try {
      block()
    } catch {
      case NonFatal(t) =>
        logInfo("Unexpected non-fatal exception", t)
        failValue
    }
  }
}
