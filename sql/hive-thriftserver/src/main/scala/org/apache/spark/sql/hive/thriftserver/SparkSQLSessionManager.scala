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
import java.util.{UUID, Collections => JCollections, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.{HiveSession, SessionManager}
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.server.SparkSQLOperationManager
import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.rsc.RSCClient
import com.cloudera.livy.rsc.RSCConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.{Utils => SparkUtils}
import org.apache.spark.sql.hive.thriftserver.rpc.{RemoteDriver, RpcClient}


private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2, hiveContext: HiveContext)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService with Logging {

  import SparkSQLSessionManager._

  private val hiveVarPattern = "set:hivevar:(.*)".r

  // doAs or cluster mode:
  // Users can explicitly name their connections and reconnect to them using this hiveconf variable
  // All connections with the same connectionId will connect to the same remote app.
  //
  // If it is not set, per user there is a single AM app (not bound to any connection id).
  //
  // In both cases, within an app a user session will have the same hivecontext or a new hivecontext
  // session depending on spark.sql.hive.thriftServer.singleSession (def: false)
  // Note that all hive contexts in an app share the same spark context.
  private val connectionIdConfigKey = "set:hiveconf:spark.sql.thriftServer.connectionId"

  // Allows for customizing spark conf variables through connection url - prefix the
  // spark config variable with 'sparkconf.' to propagate it to remote session when doAs=true or
  // cluster mode is enabled. Note - this is used/propagated only when a new rpc session is created
  // and not when an existing session is reused.
  private val overrideSparkConfigKey = "set:hiveconf:sparkconf.(.*)".r

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()

  // For both cluster mode and impersonation, we launch in a separate remote application.
  private var impersonationEnabled: Boolean = _
  private var clusterModeEnabled: Boolean = _

  override def init(hiveConf: HiveConf): Unit = synchronized {
    setSuperField(this, "hiveConf", hiveConf)

    // Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      invoke(classOf[SessionManager], this, "initOperationLogRootDir")
    }


    impersonationEnabled = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)
    clusterModeEnabled = SparkSQLEnv.sparkContext.conf.getBoolean(
      clusterModeEnabledKey, defaultValue = false)

    sparkSqlOperationManager.setImpersonationEnabled(impersonationEnabled)
    sparkSqlOperationManager.setClusterModeEnabled(clusterModeEnabled)

    logInfo("impersonationEnabled = " + impersonationEnabled)
    logInfo("clusterModeEnabled = " + clusterModeEnabled)

    setSuperField(this, "operationManager", sparkSqlOperationManager)
    addService(sparkSqlOperationManager)

    // This is what spins up the idle session, operation checks
    invoke(classOf[SessionManager], this, "createBackgroundOperationPool")

    initCompositeService(hiveConf)
  }

  private def processSessionConf(sessionHandle: SessionHandle, sessionConf: JMap[String, String]):
      (List[String], Map[String, String], Option[String]) = {

    if (null != sessionConf && ! sessionConf.isEmpty) {
      val hiveSession = ReflectionUtils.getSuperField[
        JMap[SessionHandle, HiveSession]](this, "handleToSession").get(sessionHandle)

      var connectionId: Option[String] = None
      val customSparkConfMap = new HashMap[String, String]()

      val statements = sessionConf.asScala.map {
        case (key, value) =>
          // Based on org.apache.hive.service.cli.session.HiveSessionImpl.configureSession
          key match {
            case hiveVarPattern(confKey) => s"set ${confKey.trim}=$value"
            case v if v.startsWith("use:") => "use " + value
            case v if v == connectionIdConfigKey =>
              connectionId = Some(value)
              null
            case overrideSparkConfigKey(sparkKey) =>
              customSparkConfMap += sparkKey -> value
              null
            case _ =>
              logInfo("Ignoring key = " + key + " = '" + value + "'")
              null
          }
      }.filter(_ != null).toList

      (statements, customSparkConfMap.toMap, connectionId)
    } else {
      (List(), Map(), None)
    }
  }

  private def registerRemoteSession(launchUser: String, handle: SessionHandle,
      remoteSessionId: String): Unit = {
    sparkSqlOperationManager.registerRemoteSessionId(launchUser, handle, remoteSessionId)
  }

  private def executeStatements(sessionHandle: SessionHandle, statements: List[String]): Unit = {

    if (statements.nonEmpty) {
      val hiveSession = ReflectionUtils.getSuperField[
        JMap[SessionHandle, HiveSession]](this, "handleToSession").get(sessionHandle)

      statements.foreach { stmt =>
        logInfo(s"Executing session conf stmt = $stmt")
        val operation = sparkSqlOperationManager.newExecuteStatementOperation(
          parentSession = hiveSession,
          statement = stmt,
          confOverlay = JCollections.emptyMap(),
          async = false)

        try {
          operation.run()
        } catch {
          case ex: Exception => logInfo("Unable to execute statement = '" + stmt, ex)
        } finally {
          operation.close()
        }
      }
    }
  }

  private def createRpcClient(confMap: HashMap[String, String])(): RpcClient = {
    val builder = new LivyClientBuilder()

    builder.setURI(new URI("rsc:/"))
    setDummyLivyRscConf(confMap)
    addHiveSiteAndJarsToConfig(confMap)

    // Set all values in config to builder's conf.
    confMap.foreach { case (k, v) => builder.setConf(k, v) }

    new RpcClient(builder.build().asInstanceOf[RSCClient])
  }

  private def createRpcSessionHandle(userName: String, connectionIdOpt: Option[String]): String = {
    if (connectionIdOpt.isDefined) {
      s"""username='$userName', connectionId="${connectionIdOpt.get}", default_connection=false"""
    } else {
      s"username='$userName', default_connection=true"
    }
  }

  /**
    *
    * @param sessionHandle handle for the new session created
    * @param launchUserName User the session is being launched as
    * @param customSparkConf Custom spark config specified by user
    * @return spark conf for the remote spark session
    */
  private def createLivyConf(sessionHandle: SessionHandle, launchUserName: String,
      customSparkConf: Map[String, String], connIdOpt: Option[String]): HashMap[String, String] = {

    val confMap = createConfCopy()

    customSparkConf.foreach { case (k, v) => confMap.put(k, v) }

    // Override what we need.
    confMap.put(RSCConf.Entry.DRIVER_CLASS.key(),
      "org.apache.spark.sql.hive.thriftserver.rpc.RemoteDriver")

    if (impersonationEnabled) {
      confMap.put(RSCConf.Entry.PROXY_USER.key(), launchUserName)
    }

    confMap.put("spark.master", "yarn-cluster")
    // External update to credentials - pushed from STS via rsc
    confMap.put("spark.yarn.credentials.external.update", "true")

    // Set application name
    confMap.put("spark.app.name", getApplicationName(sessionHandle,
      Option(launchUserName), connIdOpt))

    confMap.put("spark.sql.hive.version", HiveContext.hiveExecutionVersion)

    confMap
  }

  private def getSessionLaunchUser(sessionUser: String): String = {
    if (impersonationEnabled) {
      sessionUser
    } else {
      // If not in doAs, we launch the STS application as user 'hive'
      SparkUtils.getCurrentUserName()
    }
  }

  // (shared, sessionId)
  private def generateRemoteSessionId(connectionIdOpt: Option[String]): (Boolean, String) = {
    if (hiveContext.hiveThriftServerSingleSession) {
      (true, RemoteDriver.SHARED_SINGLE_SESSION)
    } else {
      (false, UUID.randomUUID().toString)
    }
  }

  override def openSession(
      protocol: TProtocolVersion,
      sessionUsername: String,
      passwd: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {

    val sessionHandle = super.openSession(protocol, sessionUsername, passwd, ipAddress,
      sessionConf, withImpersonation, delegationToken)

    val session = super.getSession(sessionHandle)
    HiveThriftServer2.listener.onSessionCreated(session.getIpAddress,
      sessionHandle.getSessionId.toString, session.getUsername)

    if (clusterModeEnabled || impersonationEnabled) {

      // hive_conf_list not supported
      val (initStatements, customSparkConf, connectionIdOpt) = processSessionConf(
        sessionHandle, sessionConf)

      // If failed, we need to close rpc session
      var failed = true
      try {
        val launchUserName = getSessionLaunchUser(sessionUsername)

        // the custom spark conf will become applicable only if a new remote session
        // is getting created
        val confMap = createLivyConf(sessionHandle, launchUserName, customSparkConf,
          connectionIdOpt)

        val rpcSessionHandle = createRpcSessionHandle(launchUserName, connectionIdOpt)
        val (sharedSession, remoteSessionId) = generateRemoteSessionId(connectionIdOpt)

        val rpcCreated = sparkSqlOperationManager.addSessionRpcClient(launchUserName,
          sessionHandle, remoteSessionId, rpcSessionHandle, createRpcClient(confMap))

        // If new rpc or a new remote session (not shared) was created then register remote session
        // with rsc session and execute initial statements (from the connection url)
        if (rpcCreated || ! sharedSession) {
          registerRemoteSession(launchUserName, sessionHandle, remoteSessionId)
          executeStatements(sessionHandle, initStatements)
        }
        failed = false
      } finally {
        if (failed) {
          SparkCLIServices.invokeSafelyUnit(
            () => sparkSqlOperationManager.closeSession(sessionHandle))
        }
      }
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

  def updateAllRscTokens(keytabLoggedInUGI: UserGroupInformation, renewer: String): Unit = {
    sparkSqlOperationManager.updateAllRscTokens(keytabLoggedInUGI, renewer)
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
    "spark.yarn.principal",
    "spark.yarn.credentials.file"
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
  private def getApplicationName(sessionHandle: SessionHandle, userOpt: Option[String],
      connIdOpt: Option[String]): String = {

    userOpt.map(v => "User ") + "SparkThriftServerApp" + connIdOpt.map(c => s" connection id = $c")
      // + ", id: " + sessionHandle.getSessionId
  }
}
