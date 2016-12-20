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

import java.io.File
import java.net.URI
import java.util.{Map => JMap}
import java.util.concurrent.Executors

import scala.collection.mutable.HashMap
import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.server.SparkSQLOperationManager
import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.rsc.RSCClient
import com.cloudera.livy.rsc.RSCConf
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.sql.hive.thriftserver.rpc.RpcClient


private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2, hiveContext: HiveContext)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService with Logging {

  import SparkSQLSessionManager._

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()

  // For both cluster mode and impersonation, we launch in a separate remote application.
  private var impersonationEnabled: Boolean = _
  private var clusterModeEnabled: Boolean = _

  override def init(hiveConf: HiveConf): Unit = this.synchronized {
    setSuperField(this, "hiveConf", hiveConf)

    // Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      invoke(classOf[SessionManager], this, "initOperationLogRootDir")
    }


    setSuperField(this, "operationManager", sparkSqlOperationManager)
    addService(sparkSqlOperationManager)

    // This is what spins up the idle session, operation checks
    invoke(classOf[SessionManager], this, "createBackgroundOperationPool")

    initCompositeService(hiveConf)

    impersonationEnabled = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)
    clusterModeEnabled = SparkSQLEnv.sparkContext.conf.getBoolean(
      clusterModeEnabledKey, defaultValue = false)
    sparkSqlOperationManager.setImpersonationEnabled(impersonationEnabled)
    sparkSqlOperationManager.setClusterModeEnabled(clusterModeEnabled)
  }

  override def openSession(
      protocol: TProtocolVersion,
      username: String,
      passwd: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {

    val sessionHandle =
      super.openSession(protocol, username, passwd, ipAddress, sessionConf, withImpersonation,
          delegationToken)
    val session = super.getSession(sessionHandle)
    HiveThriftServer2.listener.onSessionCreated(
      session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)

    // TODO: session caching ?
    if (clusterModeEnabled || impersonationEnabled) {
      val builder = new LivyClientBuilder()
      val confMap = createConfCopy()

      // Override what we need.
      confMap.put(RSCConf.Entry.DRIVER_CLASS.key(),
          "org.apache.spark.sql.hive.thriftserver.rpc.RemoteDriver")

      if (impersonationEnabled) {
        confMap.put(RSCConf.Entry.PROXY_USER.key(), username)
      }
        // Should it be hardcoded to yarn-custer or overridable ?
      confMap.put("spark.master", "yarn-cluster")

      setDummyLivyRscConf(confMap)
      addHiveSiteAndJarsToConfig(confMap)

      // Set application name
      builder.setConf("spark.app.name", getApplicationName(sessionHandle,
        if (impersonationEnabled) username else session.getUsername))
      // required ?
      builder.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)

      builder.setURI(new URI("rsc:/"))
      // Set all values in config to builder's conf.
      confMap.foreach { case (k, v) => builder.setConf(k, v) }
      val client = builder.build().asInstanceOf[RSCClient]
      val rpcClient = new RpcClient(client)

      sparkSqlOperationManager.addSessionRpcClient(sessionHandle, rpcClient)
    } else {
      val ctx = if (hiveContext.hiveThriftServerSingleSession) {
        hiveContext
      } else {
        hiveContext.newSession()
      }
      ctx.setUser(session.getUsername)
      ctx.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)
      sparkSqlOperationManager.addSessionContext(sessionHandle, ctx)
    }
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle) {
    HiveThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    super.closeSession(sessionHandle)

    sparkSqlOperationManager.closeSession(sessionHandle)
  }
}

private[hive] object SparkSQLSessionManager extends Logging {

  // spark conf key's which are application specific and must be ignored
  // when copying over config to livy builder
  private val appSpecificConfigs = Set(
    "spark.app.id",
    "spark.app.name",
    "spark.driver.host",
    "spark.driver.port",
    "spark.eventLog.dir",
    "spark.executor.id",
    "spark.externalBlockStore.folderName",

    // required ?
    "spark.master",
    "spark.submit.deployMode",

    "spark.yarn.keytab",
    "spark.yarn.principal"
  )

  // Should user query be launched in a separate cluster, and not inline within STS.
  private val clusterModeEnabledKey = "spark.sql.thriftServer.clusterMode.enabled"


  private def getHiveSitePath: Option[String] = {
    val hiveSiteXmlFile = new File(System.getenv("SPARK_HOME") + File.separator + "conf" +
      File.separator + "hive-site.xml")

    if (hiveSiteXmlFile.exists()) Some(hiveSiteXmlFile.getAbsolutePath) else None
  }

  private def mergeConfValue(confMap: HashMap[String, String],
      key: String, valueOpt: Option[String],
      sep: String = File.pathSeparator,
      append: Boolean = true): Unit = {

    if (valueOpt.isEmpty) return

    val value = valueOpt.get
    val current = confMap.get(key)

    if (current.isDefined) {
      val newValue = {
        if (append) {
          current.get + sep + value
        } else {
          value + sep + current.get
        }
      }
      confMap.put(key, newValue)
    } else {
      confMap.put(key, value)
    }
  }


  // Livy is bundled within spark assembly itself. Set LIVY_JARS to a dummy file,
  // so that livy wont complain. Note this gets added to distributed cache - so ensure
  // it is a valid file
  private def setDummyLivyRscConf(confMap: HashMap[String, String]): Unit = {
    // Create a dummy file and upload it ...
    val file = File.createTempFile("dummy_livy_jars_", "marker")
    file.deleteOnExit()
    logInfo(s"Setting ${RSCConf.Entry.LIVY_JARS.key()} to a dummy file = ${file.getAbsolutePath}")
    confMap.put(RSCConf.Entry.LIVY_JARS.key(), file.getAbsolutePath)
  }


  private def addHiveSiteAndJarsToConfig(confMap: HashMap[String, String]): Unit = {

    // Merge with existing values configured in dist.files
    mergeConfValue(confMap, "spark.yarn.dist.files", getHiveSitePath, sep = ",", append = false)
  }

  // Copy over all config's set in spark conf over for the builder : omitting
  // app specific config's. This ensures configured properties in spark-thrift-sparkconf
  private def createConfCopy(): HashMap[String, String] = {
    val confMap = new HashMap[String, String]()

    def addAllProperties(conf: SparkConf): Unit = {
      conf.getAll.
        filter(kv => ! SparkSQLSessionManager.appSpecificConfigs.contains(kv._1)).
        foreach(kv => confMap.put(kv._1, kv._2))
    }

    addAllProperties(SparkSQLEnv.sparkContext.conf)
    confMap
  }

  // The name for the launched livy spark application
  private def getApplicationName(sessionHandle: SessionHandle, username: String): String = {
    (if (null != username) "User " else "") + "SparkThriftServerApp"
      // + ", id: " + sessionHandle.getSessionId
  }
}
