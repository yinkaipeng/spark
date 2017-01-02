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

import java.io.IOException
import java.util.concurrent.{TimeUnit, ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory}
import java.util.{List => JList}
import javax.security.auth.login.LoginException
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.server.HiveServer2
import org.apache.hive.service.{AbstractService, Service, ServiceException}
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

private[hive] class SparkSQLCLIService(hiveServer: HiveServer2, hiveContext: HiveContext)
  extends CLIService(hiveServer)
  with ReflectedCompositeService {

  private var securityUpdateRenewerFuture: ScheduledFuture[_] = _
  private val securityThreadPool = new ScheduledThreadPoolExecutor(1,
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val th = new Thread(r)
        th.setDaemon(true)
        th
      }
    })
  securityThreadPool.setRemoveOnCancelPolicy(true)

  private def updateSecurity(enableLog: Boolean): Unit = synchronized {
    val LOG = getSuperField[Log](this, "LOG")
    val hiveConf = getSuperField[HiveConf](this, "hiveConf")

    try {
      HiveAuthFactory.loginFromKeytab(hiveConf)
      val sparkServiceUGI = Utils.getUGI()
      setSuperField(this, "serviceUGI", sparkServiceUGI)
    } catch {
      case e @ (_: IOException | _: LoginException) =>
        throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
    }

    // Also try creating a UGI object for the SPNego principal
    val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL)
    val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB)
    if (principal.isEmpty || keyTabFile.isEmpty) {
      if (enableLog) {
        LOG.info("SPNego httpUGI not created, spNegoPrincipal: " + principal +
          ", ketabFile: " + keyTabFile)
      }
    }
    else {
      try {
        val httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf)
        setSuperField(this, "httpUGI", httpUGI)
        if (enableLog) {
          LOG.info("SPNego httpUGI successfully created.")
        }
      } catch {
        case e: Exception => {
          if (enableLog) {
            LOG.warn("SPNego httpUGI creation failed: ", e)
          }
        }
      }
    }
  }

  private def startSecurityUpdateThread(): Unit = synchronized {
    if (null != securityUpdateRenewerFuture) return

    val LOG = getSuperField[Log](this, "LOG")

    // Running it at 80% of the expiry time.
    val renewalInterval = (0.8 * SparkSQLEnv.sparkContext.conf.getLong(
      "spark.sql.hive.thriftServer.token.renewal.interval", (24 hours).toMillis)).toLong

    securityUpdateRenewerFuture = securityThreadPool.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          LOG.info("Running security update thread")
          try {
            updateSecurity(false)
          } catch {
            case ex: Exception =>
            // log and forget, so that we dont end up with some uncaught thread handling
            LOG.info("Uncaught exception updating security", ex)
          }
        }
      },
      renewalInterval, renewalInterval, TimeUnit.MILLISECONDS)

  }

  private def stopSecurityUpdateThread(): Unit = synchronized {
    if (null == securityUpdateRenewerFuture) return

    val future = securityUpdateRenewerFuture
    securityUpdateRenewerFuture = null
    future.cancel(false)
  }

  override def init(hiveConf: HiveConf): Unit = synchronized {
    setSuperField(this, "hiveConf", hiveConf)

    val sparkSqlSessionManager = new SparkSQLSessionManager(hiveServer, hiveContext)
    setSuperField(this, "sessionManager", sparkSqlSessionManager)
    addService(sparkSqlSessionManager)

    val LOG = getSuperField[Log](this, "LOG")

    LOG.info("UGI security enabled = " + UserGroupInformation.isSecurityEnabled)

    if (UserGroupInformation.isSecurityEnabled) {
      updateSecurity(true)
      startSecurityUpdateThread()
    }

    // Required ?
    // creates connection to HMS and thus *must* occur after kerberos login above
    try {
      invoke(classOf[CLIService], this, "applyAuthorizationConfigPolicy",
        classOf[HiveConf] -> hiveConf)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Error applying authorization policy on hive configuration: " +
          e.getMessage, e)
    }
    // Is this required ? Looks hive specific
    // invoke(classOf[CLIService], this, "setupBlockedUdfs")
    initCompositeService(hiveConf)
  }

  override def stop(): Unit = synchronized {
    stopSecurityUpdateThread()
    super.stop()
  }

  override def openSessionWithImpersonation(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      configuration: JMap[String, String],
      delegationToken: String): SessionHandle = {
    val sessionHandle = getSessionManager().openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5, username, password, null, configuration,
      true, delegationToken);

    sessionHandle;
  }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(hiveContext.sparkContext.version)
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }
}

private[thriftserver] trait ReflectedCompositeService { this: AbstractService =>
  def initCompositeService(hiveConf: HiveConf): Unit = this.synchronized {
    // Emulating `CompositeService.init(hiveConf)`
    val serviceList = getAncestorField[JList[Service]](this, 2, "serviceList")
    serviceList.asScala.foreach(_.init(hiveConf))

    // Emulating `AbstractService.init(hiveConf)`
    invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.NOTINITED)
    setAncestorField(this, 3, "hiveConf", hiveConf)
    invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.INITED)
    getAncestorField[Log](this, 3, "LOG").info(s"Service: $getName is inited.")
  }
}
