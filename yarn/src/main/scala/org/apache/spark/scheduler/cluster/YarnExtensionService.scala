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

package org.apache.spark.scheduler.cluster

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.service.AbstractService
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.yarn.api.records.ApplicationId

import org.apache.spark.{Logging, SparkContext}

/**
 * An extension sservice that can be loaded into a Spark YARN application.
 * A Service that can be started and closed (==stop).
 *
 * The `close()` operation MUST be idempotent, and succeed even if `start()` was
 * never invoked.
 */
trait YarnExtensionService extends Closeable {

  /**
   * For Yarn services, SparkContext, and ApplicationId is the basic info required.
   * This operation must be called before `init`
   * @param sc spark context
   * @param appId YARN application ID
   */
  def start(sc: SparkContext, appId: ApplicationId): Boolean
}


/**
 * Class which loads child Yarn extension Services from the configuration,
 * and closes them all when closed/stopped itself.
 *
 * It extends YARN's `AbstractService` to pick up its state model, and adds an alternate entry
 * point for actually configuring and starting the service.
 *
 * An instance may be configured to register a shutdown hook. This addresses the issue in
 * which a spark context is not always closed in an application, especially a `yarn-client`
 * application. The shutdown hook allows services an opportunity to perform any final
 * actions their `Service.stop` invocations trigger.
 */
private[spark] class YarnExtensionServices extends AbstractService("YarnExtensionServices")
    with Logging {
  private var services: List[YarnExtensionService] = Nil
  private var sparkContext: SparkContext = _
  private var appId: ApplicationId = _
  private val started = new AtomicBoolean(false)
  private var registerShutdownHook = false
  private var shutdownHookPriority = 0

  /**
   * This is the routine to optionally be registered as a shutdown hook.
   * It logs the invocation and then calls the `close()` method to stop the
   * service instance. It is public to aid in testing.
   */
  val shutdownHook = new Runnable {
    override def run(): Unit = {
      logInfo(s"In shutdown hook for $this")
      try {
        close()
      }
      catch {
        case e: Exception =>
          logInfo(s"During shutdown: $e", e)
      }
    }
  }


  /**
   * Binding operation will load the named services and call bind on them too; the
   * entire set of services are then ready for init & start calls
   * @param context spark context
   * @param appId application ID
   * @param registerShutdownHook should the services register as a hadoop shutdown hook?
   * @param shutdownHookPriority and if so, what priority
   */
  def start(context: SparkContext, appId: ApplicationId,
      registerShutdownHook: Boolean = false,
      shutdownHookPriority: Int = 0): Unit = {
    require(context != null, "Null context parameter")
    require(appId != null, "Null appId parameter")
    require(!registerShutdownHook || shutdownHookPriority > 0,
           "if registerShutdownHook is set, shutdownHookPriority must be greater than zero")
    // this operation will trigger the state change checks
    super.init(context.hadoopConfiguration)
    this.sparkContext = context
    this.appId = appId
    this.registerShutdownHook = registerShutdownHook
    this.shutdownHookPriority = shutdownHookPriority
    super.start()
  }

  /**
   * Loads a comma separated list of yarn services
   */
  override def serviceStart(): Unit = {
    val sNames = sparkContext.getConf.getOption(YarnExtensionServices.SPARK_YARN_SERVICES)
    sNames match {
      case Some(names) =>
        logInfo(s"Spark YARN services: $names")
        val sClasses = names.split(",")
        services = sClasses.flatMap {
          sClass => {
            val clazz = sClass.trim
            try {
              if (clazz.nonEmpty) {
                val instance = Class.forName(clazz)
                    .newInstance()
                    .asInstanceOf[YarnExtensionService]
                // bind this service
                instance.start(sparkContext, appId)
                logInfo("Service " + sClass + " started")
                Some(instance)
              } else {
                None
              }
            } catch {
              case e: Exception =>
                logWarning(s"Cannot start service ${clazz} ", e)
                None
            }
          }
        }.toList
      case _ =>
        logDebug("No Spark YARN services declared")
    }
    if (registerShutdownHook) {
      // register the service shutdown hook
      logDebug(s"Registering shutdown hook for $this")
      ShutdownHookManager.get().addShutdownHook(shutdownHook, shutdownHookPriority)
    }
  }

  /**
   * Get the list of services
   * @return a list of services; Nil until the service is started
   */
  def getServices(): List[YarnExtensionService] = {
    services
  }


  override def serviceStop(): Unit = {
    logDebug(s"Stopping $this")
    services.foreach(_.close)
    if (registerShutdownHook) {
      // unregister the service shutdown hook, ignoring any complaints if it happens
      // during shutdown
      try {
        ShutdownHookManager.get().removeShutdownHook(shutdownHook)
        logDebug(s"Unregistered shutdown hook for $this")
      }
      catch {
        case _: IllegalStateException => // ignored
      }
    }
  }
}

private[spark] object YarnExtensionServices {
  /**
   * Configuration option to contain a list of comma separated classes to instantiate in the AM
   */
  val SPARK_YARN_SERVICES = "spark.yarn.services"
}
