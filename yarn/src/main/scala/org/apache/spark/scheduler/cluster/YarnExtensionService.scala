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
 * <p>
 * It extends YARN's AbstractService to have a robust state model for start/stop
 * re-entrancy.
 */
private[spark] class YarnExtensionServices extends AbstractService("YarnExtensionServices")
    with YarnExtensionService
    with Logging {
  var services: List[YarnExtensionService] = Nil
  var sparkContext: SparkContext = _
  var appId: ApplicationId = _
  val started = new AtomicBoolean(false)


  /**
   * Binding operation will load the named services and call bind on them too; the
   * entire set of services are then ready for init & start calls
   * @param context spark context
   * @param appId application ID
   */
  override def start(context: SparkContext, appId: ApplicationId): Boolean = {
    require(context != null, "Null context parameter")
    require(appId != null, "Null appId parameter")
    super.init(context.hadoopConfiguration)

    this.sparkContext = context
    this.appId = appId
    super.start()
    true
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
            try {
              val clazz = sClass.trim
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
                logWarning("Cannot start service ${clazz} ", e)
                None
            }
          }
        }.toList
      case _ =>
        logDebug("No Spark YARN services declared")
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
    services.foreach(_.close)
  }

}

private[spark] object YarnExtensionServices {
  /**
   * Configuration option to contain a list of comma separated classes to instantiate in the AM
   */
  val SPARK_YARN_SERVICES = "spark.yarn.services"
}
