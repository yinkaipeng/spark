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
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import java.util.{ArrayList => JArrayList}

import org.apache.commons.logging.Log

import scala.util.control.NonFatal
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.{HiveMetaStore, RawStore}
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode
import org.apache.hadoop.hive.thrift.{DBTokenStore, HadoopThriftAuthBridge}
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes
import org.apache.hive.service.cli.CLIService
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.hive.service.cli.thrift.ThriftCLIService
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.client.ClientWrapper
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.{TServerSocket, TTransportException}

/**
  */
private[hive] object SparkCLIServices extends Logging {

  private val hadoopUtil = SparkHadoopUtil.get

  // Force spark yarn mode
  private val freshHadoopConf = hadoopUtil.getConfBypassingFSCache(
    SparkSQLEnv.sparkContext.hadoopConfiguration,
    // Assuming that STS and all users have the same FileSystem on which applications
    // are launched from
    new Path(".").toUri.getScheme)

  class SparkThriftBinaryCLIService(cliService: CLIService)
    extends ThriftBinaryCLIService(cliService) {

    // Copied from ThriftBinaryCLIService and modified so that we customize creation of
    // HiveAuthFactory instead creating a new one here.
    // This method is highly specific to the implementation of HiveAuthFactory that STS depends on
    override def run() {

      val LOG = ReflectionUtils.getAncestorField[Log](this, 2, "LOG")

      try {
        // Server thread pool
        val threadPoolName = "HiveServer2-Handler-Pool"
        val executorService = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads,
          workerKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue[Runnable],
          new ThreadFactoryWithGarbageCleanup(threadPoolName))

        // --- This is the change we have introduced --
        // Prevent saslserver (created from within HiveAuthFactory) from doing a ugi login where
        // relevant. This is done to prevent global static loginUser from changing in UGI
        ThriftCLIService.hiveAuthFactory = createHiveAuthFactory(hiveConf)
        // --- end change --

        val transportFactory = ThriftCLIService.hiveAuthFactory.getAuthTransFactory
        val processorFactory = ThriftCLIService.hiveAuthFactory.getAuthProcFactory(this)
        var serverSocket: TServerSocket = null
        val sslVersionBlacklist = new JArrayList[String]()
        for (sslVersion <- hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",")) {
          sslVersionBlacklist.add(sslVersion)
        }

        if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
          serverSocket = HiveAuthFactory.getServerSocket(hiveHost, portNum)
        }
        else {
          val keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim
          if (keyStorePath.isEmpty) throw new IllegalArgumentException(
            ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname + " Not configured for SSL connection")
          val keyStorePassword = ShimLoader.getHadoopShims.getPassword(hiveConf,
            HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname)
          serverSocket = HiveAuthFactory.getServerSSLSocket(hiveHost, portNum, keyStorePath,
            keyStorePassword, sslVersionBlacklist)
        }

        // Server args
        val maxMessageSize = hiveConf.getIntVar(
          HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE)
        val requestTimeout = hiveConf.getTimeVar(
          HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT, TimeUnit.SECONDS).toInt
        val beBackoffSlotLength = hiveConf.getTimeVar(
          HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH,
          TimeUnit.MILLISECONDS).toInt
        val sargs = new TThreadPoolServer.Args(serverSocket).processorFactory(processorFactory).
          transportFactory(transportFactory).protocolFactory(
          new TBinaryProtocol.Factory).inputProtocolFactory(new TBinaryProtocol.Factory(
          true, true, maxMessageSize, maxMessageSize)).requestTimeout(requestTimeout).
          requestTimeoutUnit(TimeUnit.SECONDS).beBackoffSlotLength(beBackoffSlotLength).
          beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS).executorService(executorService)
        // TCP Server
        server = new TThreadPoolServer(sargs)
        server.setServerEventHandler(serverEventHandler)
        val msg = "Starting " + classOf[ThriftBinaryCLIService].getSimpleName + " on port " +
          portNum + " with " + minWorkerThreads + "..." + maxWorkerThreads + " worker threads"
        LOG.info(msg)
        server.serve()
      } catch {
        case t: Throwable =>
          LOG.fatal("Error starting HiveServer2: could not start " +
            classOf[ThriftBinaryCLIService].getSimpleName, t)
          System.exit(-1)
      }
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

  /*
   * Hive expects an instance of HiveAuthFactory to be set to ThriftCLIService.hiveAuthFactory. but
   * the constructor of HiveAuthFactory always does a login to ugc when in kerberos mode - which
   * causes a large number of issues due to multiple loginUser's in the VM. To workaround this,
   * we fudge behavior by overriding the hiveConf passed in so that we get an instance which does
   * not do kerberos auth - and then explicitly set the internal state via reflection to point to
   * use the ugi already configured in the system.
   * This method is highly specific to the implementation of HiveAuthFactory that STS depends on
   */
  private def createHiveAuthFactory(conf: HiveConf): HiveAuthFactory = {
    // If in http or not in kerberos mode or we dont need ugi login, use 'normal' means
    // to create HiveAuthFactory
    val transportMode = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
    val authTypeStr = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION)
    val principal = conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
    val keytab = conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB)

    if ("http".equalsIgnoreCase(transportMode) ||
      ! AuthTypes.KERBEROS.getAuthName.equalsIgnoreCase(authTypeStr) ||
      // If ugi's client user does not match the hive conf, then fallback to default auth factory
      // since we need to do a login
      ClientWrapper.needUgiLogin(UserGroupInformation.getCurrentUser,
        SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keytab)) {
      return new HiveAuthFactory(conf)
    }

    val fudgedHiveConf = new HiveConf(conf)
    // Fudge the hive conf so that HiveAuthFactory created does not create a sasl server
    fudgedHiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NONE.getAuthName)
    val authFactory = new HiveAuthFactory(fudgedHiveConf)

    // within synchronized block to force a memory barrier to mirror object construction semantics
    authFactory.synchronized {
      // Override the specific fields with actual values (we fudged initially with)
      ReflectionUtils.setAncestorField(authFactory, 0, "conf", conf)
      ReflectionUtils.setAncestorField(authFactory, 0, "transportMode", transportMode)
      ReflectionUtils.setAncestorField(authFactory, 0, "authTypeStr", authTypeStr)

      // Directly create the sasl server - the default constructor uses ugc.currentUser which is
      // what we want
      val saslServer = new HadoopThriftAuthBridge.Server()
      ReflectionUtils.setAncestorField(authFactory, 0, "saslServer", saslServer)

      // Remaining code is copied from HiveAuthFactory and duplicate that functionality - starting
      // from after sasl server is created.

      // start delegation token manager
      try {
        // rawStore is only necessary for DBTokenStore
        var rawStore: RawStore = null
        val tokenStoreClass = conf.getVar(
          HiveConf.ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS)
        if (tokenStoreClass == classOf[DBTokenStore].getName) {
          val baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", conf, true)
          rawStore = baseHandler.getMS
        }
        saslServer.startDelegationTokenSecretManager(conf, rawStore, ServerMode.HIVESERVER2)
      } catch {
        case e: Any =>
          throw new TTransportException("Failed to start token manager", e)
      }
    }

    authFactory
  }
}
