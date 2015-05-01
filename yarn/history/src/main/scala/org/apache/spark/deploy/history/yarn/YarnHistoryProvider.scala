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

package org.apache.spark.deploy.history.yarn

import java.io.FileNotFoundException

import scala.collection.JavaConversions._

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, TimelineQueryClient}
import org.apache.spark.deploy.history.{HistoryServer, ApplicationHistoryInfo, ApplicationHistoryProvider}
import org.apache.spark.scheduler.{ApplicationEventListener, SparkListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.{Logging, SecurityManager, SparkConf}

/**
 * A  History provider which reads in the history from
 * the YARN Application Timeline Service
 * @param sparkConf configuration of the provider
 */
private[spark] class YarnHistoryProvider(sparkConf: SparkConf)
  extends ApplicationHistoryProvider with Logging {

  /**
   * The configuration here is a YarnConfiguration built off the spark configuration
   * supplied in the constructor; this operation ensures that `yarn-default.xml`
   * and `yarn-site.xml` are pulled in. Options in the spark conf will override
   * those in the -default and -site XML resources which are not marked as final.
   */
  private val yarnConf = {
    new YarnConfiguration(SparkHadoopUtil.get.newConfiguration(sparkConf))
  }
  private val NOT_STARTED = "<Not Started>"

  /**
   * Timeline root URI
   */
  private val timelineUri = rootTimelineUri(yarnConf)

  /**
   * The Jersey client used for HTTP operations
   */
  private val jersey = {
    JerseyBinding.createJerseyClient(yarnConf, JerseyBinding.createClientConfig())
  }

  /**
   * The post-side `TimelineClient` handles delegation token renewal
   */

  /**
   * The timeline query client which uses the `jersey`
   * Jersey instance to talk to a timeline service running
   * at `timelineUri`, and creates a timeline (write) client instance
   * to handle token renewal
   *
   */
  private val timelineQueryClient = {
    val timelineClient = TimelineClient.createTimelineClient()
    timelineClient.init(yarnConf)
    timelineClient.start();

    new TimelineQueryClient(timelineUri, yarnConf, JerseyBinding.createClientConfig())
  }

  /**
   * List applications. This currently finds completed applications only.
   * @return List of all known applications.
   */
  override def getListing(): Seq[ApplicationHistoryInfo] = {
    logDebug(s"getListing from: $timelineUri")
    val timelineEntities = timelineQueryClient.listEntities(
         YarnHistoryService.SPARK_EVENT_ENTITY_TYPE,
         primaryFilter = Some(FILTER_APP_END, FILTER_APP_END_VALUE))

    val listing = timelineEntities.flatMap { en =>
      try { {
        val historyInfo = toApplicationHistoryInfo(en)
        Some(toApplicationHistoryInfo(en))
      }
      } catch {
        case e: Exception =>
          logInfo(s"Failed to parse entity. ${YarnTimelineUtils.describeEntity(en)}" , e)
          // skip this result
          None
      }
    }
    logDebug(s"Listed ${listing.size} applications")
    listing

  }

  /**
   * Look up the timeline entity
   * @param appId application ID
   * @return the entity associated with the given application
   * @throws FileNotFoundException if no entry was found
   */
  private def getTimelineEntity(appId: String): TimelineEntity = {
    timelineQueryClient.getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, appId)
  }

  /**
   * Build the application UI for an application
   * @param appId The application ID.
   * @return The application's UI, or None if the application is not found.
   */
  override def getAppUI(appId: String): Option[SparkUI] = {
    logDebug(s"Request UI with appId $appId")
    try { {
      val entity = getTimelineEntity(appId)

      if (log.isDebugEnabled) {
        logDebug(describeEntity(entity))
      }
      val bus = new SparkListenerBus() {}
      val appListener = new ApplicationEventListener()
      bus.addListener(appListener)

      val ui = {
        val conf = this.sparkConf.clone()
        val appSecManager = new SecurityManager(conf)
        SparkUI.createHistoryUI(conf, bus, appSecManager, appId,
                                 HistoryServer.UI_PATH_PREFIX + s"/${appId }")
      }
      val events = entity.getEvents

      events.reverse.foreach { event =>
        val sparkEvent = toSparkEvent(event)
        logDebug(s" event ${sparkEvent.toString }")
        bus.postToAll(sparkEvent)
      }
      ui.setAppName(s"${appListener.appName.getOrElse(NOT_STARTED) } ($appId)")

      val uiAclsEnabled = sparkConf.getBoolean("spark.history.ui.acls.enable", false)
      ui.getSecurityManager.setAcls(uiAclsEnabled)
      // make sure to set admin acls before view acls so they are properly picked up
      ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
      ui.getSecurityManager.setViewAcls(appListener.sparkUser.getOrElse(NOT_STARTED),
                                         appListener.viewAcls.getOrElse(""))
      Some(ui)
    }
    } catch {
      case e: FileNotFoundException =>
        logInfo(s"Unknown application $appId", e)
        None
    }
  }

  /**
   * Get configuration information for the Web UI
   * @return A map with the configuration data. Data is show in the order returned by the map.
   */
  override def getConfig(): Map[String, String] = {
    val timelineURI = timelineUri.resolve("/")
    logDebug(s"getConfig $timelineURI")
    Map(
      YarnHistoryProvider.KEY_PROVIDER_NAME ->
          "Apache Hadoop YARN Timeline Service",
      YarnHistoryProvider.KEY_SERVICE_URL ->
          s"${timelineURI}"
       )
  }

  /**
   * Stop the service. After this point operations will fail.
   */
  override def stop(): Unit =  {
    logDebug(s"Stopping $this")
    jersey.destroy()
    timelineQueryClient.close()
  }

  override def toString: String = {
    s"YarnHistoryProvider bound to history server at $timelineUri"
  }
}

object YarnHistoryProvider {

  /**
   * Default port
   */
  val SPARK_HISTORY_UI_PORT_DEFAULT = 18080
  /**
   * Name of the class to use in configuration strings
   */
  val YARN_HISTORY_PROVIDER_CLASS = classOf[YarnHistoryProvider].getName()

  /**
   * Key used when listing the URL of the ATS instance
   */
  val KEY_SERVICE_URL = "Timeline Service Location"
  /**
   * Key used to identify the history provider
   */
  val KEY_PROVIDER_NAME = "History Provider"

  /**
   * Value of the [[KEY_PROVIDER_NAME]] entry
   */
  val KEY_PROVIDER_DETAILS = "Apache Hadoop YARN Timeline Service"
}
