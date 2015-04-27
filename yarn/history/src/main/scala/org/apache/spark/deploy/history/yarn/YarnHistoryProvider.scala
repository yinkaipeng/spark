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
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, TimelineQueryClient}
import org.apache.spark.deploy.history.{ApplicationHistoryInfo, ApplicationHistoryProvider}
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
   * The Jersey client used for operations
   */
  private val client = {
    JerseyBinding.createJerseyClient(yarnConf, JerseyBinding.createClientConfig())
  }

  /**
   * A timeline query client which uses the `client`
   * Jersey instance to talk to a timeline service running
   * at `timelineUri`
   *
   */
  private val timelineQueryClient =
    new TimelineQueryClient(timelineUri, client)

  /**
   * List applications. This currently finds completed applications only.
   * @return List of all known applications.
   */
  override def getListing(): Seq[ApplicationHistoryInfo] = {
    logInfo(s"getListing with Uri: $timelineUri")
    val timelineEntities = timelineQueryClient.listEntities(
         YarnHistoryService.ENTITY_TYPE,
         primaryFilter = Some(FILTER_APP_END, FILTER_APP_END_VALUE))

    timelineEntities.flatMap { en =>
      try {
        Some(toApplicationHistoryInfo(en))
      } catch {
        case e: Exception =>
          logInfo(s"Failed to parse entity. ${YarnTimelineUtils.describeEntity(en)}" , e)
          None
      }
    }
  }

  /**
   * Look up the timeline entity
   * @param appId application ID
   * @return the entity associated with the given application
   * @throws FileNotFoundException if no entry was found
   */
  private def getTimelineEntity(appId: String): TimelineEntity = {
    timelineQueryClient.getEntity(YarnHistoryService.ENTITY_TYPE, appId)
  }

  /**
   * Build the application UI for an application
   * @param appId The application ID.
   * @return The application's UI, or None if the application is not found.
   */
  override def getAppUI(appId: String): Option[SparkUI] = {
    logInfo(s"Request UI with appId $appId")
    try {
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
        SparkUI.createHistoryUI(conf, bus, appSecManager, appId, "/history/" + appId)
      }
      val events = entity.getEvents

      events.reverse.foreach { event =>
        val sparkEvent = toSparkEvent(event)
        logDebug(s" event ${sparkEvent.toString}")
        bus.postToAll(sparkEvent)
      }
      ui.setAppName(s"${appListener.appName.getOrElse(NOT_STARTED)} ($appId)")

      val uiAclsEnabled = sparkConf.getBoolean("spark.history.ui.acls.enable", false)
      ui.getSecurityManager.setAcls(uiAclsEnabled)
      // make sure to set admin acls before view acls so they are properly picked up
      ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
      ui.getSecurityManager.setViewAcls(appListener.sparkUser.getOrElse(NOT_STARTED),
                                         appListener.viewAcls.getOrElse(""))
      Some(ui)
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
    logInfo("getConfig ...:" +  timelineUri.resolve("/").toString())
    Map(
      ("Yarn Application History Server URI" -> timelineQueryClient.timelineURL.toString),
      ("Yarn Application History Server" -> timelineQueryClient.about()))
  }

  /**
   * Stop the service. After this point operations will fail.
   */
  override def stop(): Unit =  {
    logDebug(s"Stopping $this")
    client.destroy()
  }

  override def toString: String = {
    s"YarnHistoryProvider bound to history server at $timelineUri"
  }
}
