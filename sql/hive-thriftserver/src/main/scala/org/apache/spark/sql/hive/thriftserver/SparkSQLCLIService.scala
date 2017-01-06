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
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import java.util.{List => JList}
import javax.security.auth.login.LoginException
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.server.HiveServer2
import org.apache.hive.service.{AbstractService, Service, ServiceException}
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

private[hive] class SparkSQLCLIService(hiveServer: HiveServer2, hiveContext: HiveContext)
  extends CLIService(hiveServer)
  with ReflectedCompositeService with Logging {

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

  private def updateSparkSecurity(hiveConf: HiveConf, loginUser: UserGroupInformation): Unit = {
    val LOG = getSuperField[Log](this, "LOG")

    val hivePrincipal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
    val principal = SecurityUtil.getServerPrincipal(hivePrincipal, "0.0.0.0")
    val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB)

    LOG.info(s"Attempting to login to KDC using principal: $principal")
    val keytabLoggedInUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      principal, keyTabFile)

    val tempCreds = keytabLoggedInUGI.getCredentials
    if (SparkCLIServices.fetchCredentials(keytabLoggedInUGI, tempCreds, principal)) {

      // Add the temp credentials back to the original ones.
      LOG.info("Updating credentials to ugi")
      UserGroupInformation.getCurrentUser.addCredentials(tempCreds)

      val sparkSqlSessionManager = getSuperField[SessionManager](
        this, "sessionManager").asInstanceOf[SparkSQLSessionManager]

      sparkSqlSessionManager.updateAllRscTokens(keytabLoggedInUGI, principal)
    }
  }

  private def updateSecurity(initializeSecurity: Boolean): Unit = synchronized {
    val LOG = getSuperField[Log](this, "LOG")
    val hiveConf = getSuperField[HiveConf](this, "hiveConf")

    assert (UserGroupInformation.isSecurityEnabled)

    def logLoginUserDetails(which: String, lu: UserGroupInformation): Unit = {
      if (null == lu) {
        logDebug(which + " ... null ugc")
        return
      }

      logDebug(which + " loginUser.isFromKeytab = " + lu.isFromKeytab)
      logDebug(which + " loginUser.authmethod = " + lu.getAuthenticationMethod.getAuthMethod)
      logDebug(which + " loginUser.hasKerberosCredentials = " + lu.hasKerberosCredentials)
      logDebug(s"$which loginUser = $lu, hc = ${System.identityHashCode(lu)}")
    }

    if (initializeSecurity) {
      logLoginUserDetails("initial", UserGroupInformation.getLoginUser)

      try {
        HiveAuthFactory.loginFromKeytab(hiveConf)
        val sparkServiceUGI = Utils.getUGI()
        setSuperField(this, "serviceUGI", sparkServiceUGI)
      } catch {
        case e@(_: IOException | _: LoginException) =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }
      // Also try creating a UGI object for the SPNego principal
      val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL)
      val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB)
      if (principal.isEmpty || keyTabFile.isEmpty) {
        LOG.info("SPNego httpUGI not created, spNegoPrincipal: " + principal +
          ", ketabFile: " + keyTabFile)
      }
      else {
        try {
          val httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf)
          setSuperField(this, "httpUGI", httpUGI)
          LOG.info("SPNego httpUGI successfully created.")
        } catch {
          case e: Exception => {
            LOG.warn("SPNego httpUGI creation failed: ", e)
          }
        }
      }
    }

    val loginUser = UserGroupInformation.getLoginUser
    logLoginUserDetails("before", UserGroupInformation.getLoginUser)
    // TODO: change from reloginFromKeytab to checkTGTAndReloginFromKeytab when done testing
    // SparkCLIServices.invokeSafelyUnit(() => loginUser.checkTGTAndReloginFromKeytab())
    SparkCLIServices.invokeSafelyUnit(() => loginUser.reloginFromKeytab())
    logLoginUserDetails("after", UserGroupInformation.getLoginUser)

    // Spark specific security method
    updateSparkSecurity(hiveConf, loginUser)
  }

  private def startSecurityUpdateThread(): Unit = synchronized {
    if (null != securityUpdateRenewerFuture) return

    val LOG = getSuperField[Log](this, "LOG")

    // Running it at 70% of the expiry time. It is lower than spark's 75% since we have to acquire
    // and update tokens for a (potentially) large number of cluster jobs and update all of them
    // via rsc. Note that in case this is too conservative, it is possible for admin to further
    // lower token renewal interval for STS apps by customizing
    // 'spark.sql.hive.thriftServer.token.renewal.interval' to a lower value
    val renewalInterval = (0.7 * SparkSQLEnv.sparkContext.conf.getLong(
      "spark.sql.hive.thriftServer.token.renewal.interval",
      // If unset, default to spark.yarn.token.renewal.interval
      SparkSQLEnv.sparkContext.conf.getLong(
        "spark.yarn.token.renewal.interval", (24 hours).toMillis))).toLong

    securityUpdateRenewerFuture = securityThreadPool.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          LOG.info("Running security update thread")
          try {
            updateSecurity(initializeSecurity = false)
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
      updateSecurity(initializeSecurity = true)
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
    val sessionHandle = getSessionManager.openSession(
      SparkCLIServices.version, username, password, null, configuration,
      true, delegationToken)

    sessionHandle
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
