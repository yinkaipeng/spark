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
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}

import scala.collection.JavaConversions._

import org.apache.hadoop.security.UserGroupInformation
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
 *
 * The service is a remote HTTP service, so failure modes are
 * different from simple file IO.
 *
 * 1. Application listings are asynchronous, and made on a schedule, though
 * they can be forced (and the schedule disabled).
 * 2. The results are cached and can be retrieved with [[getApplications()]].
 * 3. The most recent failure of any operation is stored,
 * The [[getLastFailure()]] call will return the last exception
 * or `None`. It is shared across threads so is primarily there for
 * tests and basic diagnostics.
 * 4. Listing the details of a single application in [[getAppUI()]]
 * is synchronous and *not* cached.
 * 5. the [[maybeCheckHealth()]] call performs a health check as the initial
 * binding operation of this instance. This call invokes [[TimelineQueryClient.healthCheck()]]
 * for better diagnostics on binding failures -particularly configuration problems.
 * 6. Every REST call, synchronous or asynchronous, will invoke [[maybeCheckHealth()]] until
 * the health check eventually succeeds.
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

  /**
   * UI ACL option
   */
  private val uiAclsEnabled = sparkConf.getBoolean("spark.history.ui.acls.enable", false)

  private val detailedInfo = sparkConf.getBoolean(YarnHistoryProvider.OPTION_DETAILED_INFO, false)
  private val NOT_STARTED = "<Not Started>"

  // Interval between each check for event log updates
  private val refreshInterval = sparkConf.getInt(YarnHistoryProvider.OPTION_UPDATE_INTERVAL,
    YarnHistoryProvider.DEFAULT_UPDATE_INTERVAL_SECONDS)

  /**
   * Window limit in milliseconds
   */
  private val windowLimitMs = sparkConf.getLong(YarnHistoryProvider.OPTION_WINDOW_LIMIT,
    YarnHistoryProvider.DEFAULT_WINDOW_LIMIT) * 1000

  val serviceStartTime = System.currentTimeMillis()

  /**
   * Timeline endpoint URI
   */
  protected val timelineEndpoint = createTimelineEndpoint()

  /**
   * The timeline query client which uses the `jersey`
   * Jersey instance to talk to a timeline service running
   * at [[timelineEndpoint]], and creates a timeline (write) client instance
   * to handle token renewal
   *
   */
  protected val timelineQueryClient = {
    createTimelineQueryClient()
  }


  /**
   * Override point: create the timeline endpoint
   * @return a URI to the timeline web service
   */
  protected def createTimelineEndpoint(): URI = {
    getTimelineEndpoint(yarnConf)
  }

  /**
   * Override point: create the timeline query client.
   * This is called during instance creation.
   * @return a timeline query client ot use for the duration
   *         of this instance
   */
  protected def createTimelineQueryClient(): TimelineQueryClient = {
    new TimelineQueryClient(timelineEndpoint, yarnConf, JerseyBinding.createClientConfig())
  }

  /**
   * The empty listing, with a timestamp to indicate that the listing
   * has never taken place.
   */
  private val emptyListing = new ApplicationListingResults(0, Nil, None)

  /**
   * List of applications. Initial result is empty
   */
  private var applications: ApplicationListingResults = emptyListing
  
  /**
   * Last exception seen and when
   */
  protected var lastFailureCause: Option[(Throwable, Date)] = None

  private val refreshCount = new AtomicLong(0)
  private val refreshFailedCount = new AtomicLong(0)

  /**
   * Health marker
   */
  private val healthy = new AtomicBoolean(false)

  /**
   * Enabled flag
   */
  private val _enabled = timelineServiceEnabled(yarnConf)

  /**
   * Atomic boolean used to signal to the refresh thread that it
   * must exit its loop.
   */
  private val stopRefresh = new AtomicBoolean(false)

  /**
   * refresh thread
   */
  private var refreshThread: Option[Thread] = None

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
      logInfo(YarnHistoryProvider.KEY_SERVICE_URL + ": " + timelineEndpoint)
      logDebug(sparkConf.toDebugString)
      // get the thread time
      logInfo(s"refresh interval $refreshInterval seconds")
      if (refreshInterval <= 0) {
        throw new Exception(YarnHistoryProvider.TEXT_INVALID_UPDATE_INTERVAL +
            s": $refreshInterval")
      }
      startRefreshThread(1000 * refreshInterval)
    }
  }


  /**
   * Stop the service. After this point operations will fail.
   */
  override def stop(): Unit = {
    logDebug(s"Stopping $this")
    // attempt to stop the refresh thread
    if (!stopRefreshThread()) {
      // and otherwise, stop the query client
      logDebug("Stopping Timeline client")
      timelineQueryClient.close()
    }

  }

  /**
   * Is the timeline service (and therefore this provider) enabled.
   * (override point for tests).
   *
   * Important: this is called during construction, so test-time subclasses
   * will be invoked before their own construction has taken place.
   * Code appropriately.
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
  private def setLastFailure(ex: Throwable): Unit = {
    setLastFailure(ex, now())
  }

  /**
   * Set the last exception
   * @param ex exception seen
   * @param timestamp the timestamp of the failure
   */
  private def setLastFailure(ex: Throwable, timestamp: Long): Unit = {
    this.synchronized {
      lastFailureCause = Some(ex, new Date(timestamp))
    }
  }

  /**
   * Reset the failure info
   */
  private def resetLastFailure(): Unit = {
    this.synchronized {
      lastFailureCause = None
    }
  }

  /**
   * Get the last exception
   * @return the last exception or  null
   */
  def getLastFailure(): Option[(Throwable, Date)] = {
    this.synchronized {
      lastFailureCause
    }
  }

  /**
   * Query for the connection being healthy
   * @return
   */
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
   * Thread safe accessor to application list
   * @return
   */
  def getApplications(): ApplicationListingResults = {
    this.synchronized(applications)
  }

  /**
   * Thread safe call to update the application results
   * @param newVal new value
   */
  protected def setApplications(newVal: ApplicationListingResults): Unit = {
    this.synchronized {
      applications = newVal
    }
  }

  /**
   * Health check to call before any other operation is attempted.
   * This is atomic, using the `healthy` flag to check.
   * If the endpoint is considered unhealthy then the healthy flag
   * is reset to false and an exception thrown.
   * @return true if the health check took place
   */
  protected def maybeCheckHealth(): Boolean = {
    val h = getHealthFlag();
    if (!h.getAndSet(true)) {
      val client = getTimelineQueryClient()
      try {
        client.healthCheck()
        true
      } catch {
        case e: Exception =>
          // failure
          logWarning(s"Health check of $client failed", e)
          setLastFailure(e)
          // reset health so another caller may attempt it.
          h.set(false)
          // propagate the failure
          throw e;
      }
    } else {
      false
    }
  }

  /**
   * Start the refresh thread with the given interval.
   *
   * When this thread exits, it will close the `timelineQueryClient`
   * instance
   * @param interval sleep interval, must be greater than zero
   */
  def startRefreshThread(interval: Long): Unit = {
    require(interval > 0,
      s"Interval must be greater than zero, not $interval")
    logInfo(s"Starting timeline refresh thread with interval $interval millis")
    val thread = new Thread(s"YarnHistoryProvider Refresh Thread") {
      override def run(): Unit = {
        while (!stopRefresh.get()) {
          try {
            listAndCacheApplications()
            if (!stopRefresh.get()) {
              // before sleeping, check the current status
              Thread.sleep(interval)
            }
          } catch {
            case e: InterruptedException =>
              // interrupted; if the `stopRefreshThread` flag is set, this will trigger an exit
              logInfo(s"Refresh thread interrupted")
            case e: Exception =>
              if (!stopRefresh.get()) {
                // if the stop process has started, don't bother
                // complaining as it will only confuse people looking
                // at logs (especially test logs)
                logWarning(s"In refresh: $e", e)
              }
          } finally {
            timelineQueryClient.close()
          }
        }
      }
    }
    refreshThread = Some(thread)
    thread.setDaemon(true)
    thread.start()
  }

  /**
   * Stop the refresh thread if there is one.
   *
   * This does not guarantee an immediate halt to the thread.
   * @return true if there was a refresh thread to stop
   */
  def stopRefreshThread(): Boolean = {
    refreshThread match {
      case Some(refresher) =>
        logDebug("Trigger Async Refresh Thread stop")
        stopRefresh.set(true)
        refresher.interrupt()
        true
      case None =>
        // nothing to stop
        false
    }
  }

  /**
   * Probe for the refresh thread running
   * @return true if the refresh thread has been created and is still alive
   */
  def isRefreshThreadRunning(): Boolean = {
    refreshThread match {
      case Some(refresher) =>
        refresher.isAlive()
      case None =>
        // no thread created
        false
    }
  }

  def getRefreshCount(): Long = { refreshCount.get() }
  def getRefreshFailedCount(): Long = { refreshFailedCount.get() }

  /**
   * List applications.
   * <p>
   * If the timeline is not enabled, returns an empty list
   * @return  the result of the last successful listing operation,
   *          or the `emptyListing` result if no listing has yet been successful
   */
   def listApplications(limit: Option[Long] = None,
      windowStart: Option[Long] = None,
      windowEnd: Option[Long] = None): ApplicationListingResults = {
    if (!enabled) {
      // Timeline is disabled: return the empty listing
      return emptyListing
    }
    try {
      maybeCheckHealth()
      val client = getTimelineQueryClient()
      logInfo(s"getListing from: $client")
      // get the timestamp after any health check
      val timestamp = now()
      val timelineEntities =
        client.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE,
          windowStart = windowStart,
          windowEnd = windowEnd,
          limit = limit)

      val listing = timelineEntities.flatMap { en =>
        try {
          val historyInfo = toApplicationHistoryInfo(en)
          logDebug(s"${YarnTimelineUtils.describeApplicationHistoryInfo(historyInfo)}")
          Some(historyInfo)
        } catch {
          case e: Exception =>
            logWarning(s"Failed to parse entity. ${YarnTimelineUtils.describeEntity(en) }", e)
            // skip this result
            None
        }
      }
      logInfo(s"Found ${listing.size } applications")
      new ApplicationListingResults(timestamp, listing.sortWith(compareAppInfo), None)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to list entities from $timelineEndpoint", e)
        new ApplicationListingResults(now(), Nil, Some(e))
    }
  }

  /**
   * List applications. 
   *
   * Also updates the cached values of the listing/last failure, depending
   * upon the outcome
   * If the timeline is  not enabled, returns an empty list
   * @return List of all known applications.
   */
  def listAndCacheApplications(): ApplicationListingResults = {
    refreshCount.incrementAndGet()
    val results = listApplications()
    this.synchronized {
      if (results.succeeded) {
        // on a success, the applications are updated
        setApplications(results)
        resetLastFailure()
      } else {
        refreshFailedCount.incrementAndGet()
        // on a failure, the failure cause is update
        setLastFailure(results.failureCause.get, results.timestamp)
      }
    }
    results
  }

  /**
   * List applications. This currently finds completed applications only.
   * 
   * If the timeline is  not enabled, returns an empty list
   * @return List of all known applications.
   */
  override def getListing(): Seq[ApplicationHistoryInfo] = {
    applications.applications
  }

  /**
   * Return the current time
   * @return
   */
  def now(): Long = {
    System.currentTimeMillis()
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
      logInfo(s"App $appId history contains ${events.size()} events")

      events.reverse.foreach { event =>
        val sparkEvent = toSparkEvent(event)
        logDebug(s" event ${sparkEvent.toString }")
        bus.postToAll(sparkEvent)
      }
      ui.setAppName(s"${appListener.appName.getOrElse(NOT_STARTED) } ($appId)")

      ui.getSecurityManager.setAcls(uiAclsEnabled)
      // make sure to set admin acls before view acls so they are properly picked up
      ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
      ui.getSecurityManager.setViewAcls(appListener.sparkUser.getOrElse(NOT_STARTED),
                                         appListener.viewAcls.getOrElse(""))
      Some(ui)
    } catch {
      case e: FileNotFoundException =>
        logInfo(s"Unknown application $appId", e)
        setLastFailure(e)
        None
      case e: Exception =>
        logWarning(s"Failed to get attempt information for $appId", e)
        setLastFailure(e)
        None
    }
  }

  /**
   * Get configuration information for the Web UI
   * @return A map with the configuration data. Data is shown in the order returned by the map.
   */
  override def getConfig(): Map[String, String] = {
    val timelineURI = getEndpointURI()
    logDebug(s"getConfig $timelineURI")
    this.synchronized {
      val applications = getApplications()
      val failure = getLastFailure()
      var state = Map(
        YarnHistoryProvider.KEY_PROVIDER_NAME -> "Apache Hadoop YARN Timeline Service",
        YarnHistoryProvider.KEY_START_TIME ->
            humanDateCurrentTZ(serviceStartTime, "(not started)"),
        YarnHistoryProvider.KEY_SERVICE_URL -> s"$timelineURI",
        YarnHistoryProvider.KEY_ENABLED ->
           (if (enabled) YarnHistoryProvider.TEXT_SERVICE_ENABLED
            else YarnHistoryProvider.TEXT_SERVICE_DISABLED),
        YarnHistoryProvider.KEY_LAST_UPDATED -> applications.updated
      )
      // in a secure cluster, list the user name
      if (UserGroupInformation.isSecurityEnabled) {
        state = state +
            (YarnHistoryProvider.KEY_USERNAME -> UserGroupInformation.getCurrentUser.getUserName)

      }

      // on a failure, add failure specifics to the operations
      failure match {
        case Some((ex , date)) =>
          state = state ++
            Map(
              YarnHistoryProvider.KEY_LAST_FAILURE_TIME ->
                humanDateCurrentTZ(date.getTime, YarnHistoryProvider.TEXT_NEVER_UPDATED),
              YarnHistoryProvider.KEY_LAST_FAILURE -> ex.toString)
        case None =>
          // nothing
      }
      // add detailed information if enabled
      if (detailedInfo) {
        state = state + (YarnHistoryProvider.KEY_TOKEN_RENEWAL ->
          humanDateCurrentTZ(timelineQueryClient.lastTokenRenewal,
            YarnHistoryProvider.TEXT_NEVER_UPDATED ))
        state = state +
            (YarnHistoryProvider.KEY_TOKEN_RENEWAL_COUNT ->
                timelineQueryClient.tokenRenewalCount.toString)
        state = state +
            (YarnHistoryProvider.KEY_TO_STRING -> s"$this")


      }
      state
    }

  }

  def getEndpointURI(): URI = {
    timelineEndpoint.resolve("/")
  }


  override def toString(): String = {
    s"YarnHistoryProvider bound to history server at $timelineEndpoint," +
    s" enabled = $enabled;" +
    s" refresh thread active = ${isRefreshThreadRunning()} with interval ${refreshInterval};" +
    s" refresh count = ${getRefreshCount()}; failed count = ${getRefreshFailedCount()};" +
    s" last update ${applications.updated};" +
    s" history size ${applications.size};"
  }

  /**
   * Comparison function that defines the sort order for the application listing.
   *
   * @return Whether `i1` should precede `i2`.
   */
  private def compareAppInfo(
      i1: ApplicationHistoryInfo,
      i2: ApplicationHistoryInfo): Boolean = {
    if (i1.endTime != i2.endTime) i1.endTime >= i2.endTime else i1.startTime >= i2.startTime
  }

}


/**
 * (Immutable) results of a list operation
 * @param timestamp timestamp
 * @param applications application listing. These must be pre-sorted
 * @param failureCause exception raised (implies operation was a failure)
 */
private[spark] class ApplicationListingResults(
    val timestamp: Long,
    val applications: Seq[ApplicationHistoryInfo],
    val failureCause: Option[Throwable]) {

  /**
   * Predicate which is true if the listing failed; that there
   * is a failure cause value
   * @return true if the listing failed
   */
  def failed: Boolean = { failureCause.isDefined }

  def succeeded: Boolean = { !failed }

  /**
   * Get an updated time for display
   * @return a date time or "never"
   */
  def updated: String = {
    humanDateCurrentTZ(timestamp, YarnHistoryProvider.TEXT_NEVER_UPDATED)
  }

  def size: Int = {
    applications.size
  }
}

private[spark] object YarnHistoryProvider {

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

  val KEY_START_TIME = "Service Started"
  val KEY_LAST_UPDATED = "Last Updated"
  val KEY_LAST_FAILURE = "Last Operation Failure"
  val KEY_LAST_FAILURE_TIME = "Last Operation Failed"

  /**
   * Value of the [[KEY_PROVIDER_NAME]] entry
   */
  val KEY_PROVIDER_DETAILS = "Apache Hadoop YARN Timeline Service"

  val TEXT_SERVICE_ENABLED = "Timeline service is enabled"
  val TEXT_SERVICE_DISABLED =
    "Timeline service is disabled: application history cannot be retrieved"

  val TEXT_NEVER_UPDATED = "Never"
  val TEXT_INVALID_UPDATE_INTERVAL = s"Invalid update interval defined in $OPTION_UPDATE_INTERVAL"
  
  
  val KEY_LISTING_REFRESH_INTERVAL = "Update Interval"

  /**
   * Option for the interval for listing timeline refreshes. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val OPTION_UPDATE_INTERVAL = "spark.history.yarn.updateInterval"
  val DEFAULT_UPDATE_INTERVAL_SECONDS = 60

  /**
   * Maximum timeline of the window when getting updates.
   * If set to zero, there's no limit
   */
  val OPTION_WINDOW_LIMIT = "spark.history.yarn.window.limit"
  val DEFAULT_WINDOW_LIMIT = 60L * 60 * 24

  val OPTION_DETAILED_INFO = "spark.history.yarn.diagnostics"

  val KEY_TOKEN_RENEWAL = "Token Renewed"
  val KEY_TOKEN_RENEWAL_COUNT = "Token Renewal Count"
  val KEY_TO_STRING = "Internal State"
  val KEY_USERNAME = "User"

}
