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
import java.net.URI
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConversions._

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, TimelineQueryClient}
import org.apache.spark.deploy.history.{ApplicationHistoryInfo, ApplicationHistoryProvider, HistoryServer}
import org.apache.spark.scheduler.{ApplicationEventListener, SparkListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.{Logging, SecurityManager, SparkConf}

/**
 * A  History provider which reads in the history from
 * the YARN Timeline Service.
 * <p>
 * The service is a remote HTTP service, so failure modes are
 * different from simple file IO.
 * <p>
 * The [[getLastException()]] call will return the last exception
 * or `None`. It is shared across threads so is primarily there for
 * tests and basic diagnostics.
 * <p>
 * If the timeline is  not enabled, the API calls used by the web UI
 * downgrade gracefully (returning empty entries), rather than fail.
 * 
 *
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
   * Timeline endpoint URI
   */
  protected val timelineEndpoint = createTimelineEndpoint

  /**
   * The Jersey client used for HTTP operations
   */
  protected val jersey = {
    JerseyBinding.createJerseyClient(yarnConf, JerseyBinding.createClientConfig())
  }

  /**
   * The timeline query client which uses the `jersey`
   * Jersey instance to talk to a timeline service running
   * at [[timelineEndpoint]], and creates a timeline (write) client instance
   * to handle token renewal
   *
   */
  protected val timelineQueryClient = {
    createTimelineQueryClient
  }


  /**
   * Override point: create the timeline endpoint
   * @return a URI to the timeline web service
   */
  protected def createTimelineEndpoint: URI = {
    getTimelineEndpoint(yarnConf)
  }

  /**
   * Override point: create the timeline query client.
   * This is called during instance creation.
   * @return a timeline query client ot use for the duration
   *         of this instance
   */
  protected def createTimelineQueryClient: TimelineQueryClient = {
    new TimelineQueryClient(timelineEndpoint, yarnConf, JerseyBinding.createClientConfig())
  }

  /**
   * Last exception seen and when
   */
  protected var lastException: Option[(Exception, Date)] = None

  /**
   * Health marker
   */
  private val healthy = new AtomicBoolean(false)

  /**
   * Enabled flag
   */
  private val _enabled = timelineServiceEnabled(yarnConf)

  /**
   * Initialize the provider
   */
  init()

  /**
   * Check the configuration and log whether or not it is enabled;
   * if it is enabled then the URL is logged too.
   */
  private def init(): Unit = {
    if (!enabled) {
      logError(YarnHistoryProvider.TEXT_SERVICE_DISABLED)
    } else {
      logInfo(YarnHistoryProvider.TEXT_SERVICE_ENABLED)
      logInfo(YarnHistoryProvider.KEY_SERVICE_URL
          + ": " + timelineEndpoint)
    }
  }

  /**
   * Is the timeline service (and therefore this provider) enabled.
   * (override point for tests)
   * @return true if the provider/YARN configuration enables the timeline
   *         service.
   */
  def enabled: Boolean = {
    _enabled
  }
  
  /**
   * Get the timeline query client. Used internally to ease testing
   * @return the client.
   */
  def getTimelineQueryClient(): TimelineQueryClient = {
    timelineQueryClient
  }

  /**
   * Set the last exception
   * @param ex exception seen
   */
  private def setLastException(ex: Exception): Unit = {
    this.synchronized {
      lastException = Some(ex, new Date())
    }
  }

  /**
   * Get the last exception
   * @return the last exception or  null
   */
  def getLastException(): Option[(Exception, Date)] = {
    this.synchronized {
      lastException
    }
  }

  def isHealthy(): Boolean = {
    healthy.get()
  }

  /**
   * Get that the health flag itself. This allows test code to initialize it properly.
   * Also: if accessed and set to false, it will trigger another health chek.
   * @return
   */
  protected def getHealthFlag(): AtomicBoolean = {
    healthy;
  }

  /**
   * Health check to call before any other operation is attempted
   */
  private def maybeCheckHealth(): Unit = {
    val h = getHealthFlag();
    if (!h.getAndSet(true)) {
      val client = getTimelineQueryClient()
      try {
        client.healthCheck()
      } catch {
        case e: Exception =>
          // failure
          logWarning(s"Health check of $client failed", e)
          setLastException(e)
          // reset health so another caller may attempt it.
          h.set(false)
          // propagate the failure
          throw e;
      }
    }
  }

  /**
   * List applications. This currently finds completed applications only.
   * <p>
   * If the timeline is  not enabled, returns an empty list
   * @return List of all known applications.
   */
  override def getListing(): Seq[ApplicationHistoryInfo] = {
    if (!enabled) {
      // Timeline is disabled: return nothing
      return Nil
    }
    maybeCheckHealth()
    val client = getTimelineQueryClient()
    logInfo(s"getListing from: $client")
    val timelineEntities = try {
      client.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to list entities from $timelineEndpoint", e)
        setLastException(e)
        // choice of two actions: swallow or rethrow
        // Nil
        throw e
    }

    val listing = timelineEntities.flatMap { en =>
      try {
        val historyInfo = toApplicationHistoryInfo(en)
        Some(toApplicationHistoryInfo(en))
      } catch {
        case e: Exception =>
          logWarning(s"Failed to parse entity. ${YarnTimelineUtils.describeEntity(en)}" , e)
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
  def getTimelineEntity(appId: String): TimelineEntity = {
    logDebug(s"GetTimelineEntity $appId")
    maybeCheckHealth()
    getTimelineQueryClient().getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, appId)
  }

  /**
   * Build the application UI for an application
   * <p>
   * If the timeline is  not enabled, returns `None`
   * @param appId The application ID.
   * @return The application's UI, or `None` if application is not found.
   */
  override def getAppUI(appId: String): Option[SparkUI] = {
    logDebug(s"Request UI with appId $appId")
    if (!enabled) {
      // Timeline is disabled: return nothing
      return None
    }
    maybeCheckHealth()
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
    } catch {
      case e: FileNotFoundException =>
        logInfo(s"Unknown application $appId", e)
        setLastException(e)
        None
      case e: Exception =>
        logWarning(s"Failed to get attempt information for $appId", e)
        setLastException(e)
        None
    }
  }

  /**
   * Get configuration information for the Web UI
   * @return A map with the configuration data. Data is show in the order returned by the map.
   */
  override def getConfig(): Map[String, String] = {
      val timelineURI = getEndpointURI()
      logDebug(s"getConfig $timelineURI")
      Map(
       YarnHistoryProvider.KEY_PROVIDER_NAME -> "Apache Hadoop YARN Timeline Service",
       YarnHistoryProvider.KEY_SERVICE_URL -> s"$timelineURI",
       YarnHistoryProvider.KEY_ENABLED ->
           (if (enabled) YarnHistoryProvider.TEXT_SERVICE_ENABLED
            else YarnHistoryProvider.TEXT_SERVICE_DISABLED)
         )
  }

  def getEndpointURI(): URI = {
    timelineEndpoint.resolve("/")
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
    s"YarnHistoryProvider bound to history server at $timelineEndpoint"
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
   * is the service enabled?
   */
  val KEY_ENABLED = "Timeline Service"

  /**
   * Key used to identify the history provider
   */
  val KEY_PROVIDER_NAME = "History Provider"

  /**
   * Value of the [[KEY_PROVIDER_NAME]] entry
   */
  val KEY_PROVIDER_DETAILS = "Apache Hadoop YARN Timeline Service"

  val TEXT_SERVICE_ENABLED = "Timeline service is enabled"
  val TEXT_SERVICE_DISABLED =
    "Timeline service is disabled: application history cannot be retrieved"
}
