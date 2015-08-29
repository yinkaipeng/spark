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
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import java.util.zip.ZipOutputStream

import scala.collection.JavaConversions._

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.rest.{JerseyBinding, TimelineQueryClient}
import org.apache.spark.deploy.history.{ApplicationHistoryInfo, ApplicationHistoryProvider, HistoryServer}
import org.apache.spark.scheduler.{ApplicationEventListener, SparkListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{SystemClock, Clock}
import org.apache.spark.{SparkException, Logging, SecurityManager, SparkConf}

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
 * 5. the [[maybeCheckEndpoint()]] call performs an endpoint check as the initial
 * binding operation of this instance. This call invokes [[TimelineQueryClient.endpointCheck()]]
 * for better diagnostics on binding failures -particularly configuration problems.
 * 6. Every REST call, synchronous or asynchronous, will invoke [[maybeCheckEndpoint()]] until
 * the endpoint check eventually succeeds.
 * <p>
 * If the timeline is not enabled, the API calls used by the web UI
 * downgrade gracefully (returning empty entries), rather than fail.
 * 
 *
 * @param sparkConf configuration of the provider
 */
private[spark] class YarnHistoryProvider(sparkConf: SparkConf)
  extends ApplicationHistoryProvider with Logging {

  // import all the constants
  import org.apache.spark.deploy.history.yarn.YarnHistoryProvider._

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

  private val detailedInfo = sparkConf.getBoolean(OPTION_DETAILED_INFO, false)
  private val NOT_STARTED = "<Not Started>"

  /* minimum interval in milliseconds between each check for event log updates */
  val refreshInterval = sparkConf.getTimeAsSeconds(OPTION_MIN_REFRESH_INTERVAL,
    s"${DEFAULT_MIN_REFRESH_INTERVAL_SECONDS}s") * 1000

  /**
   * Window limit in milliseconds
   */
  private val windowLimitMs = sparkConf.getTimeAsSeconds(OPTION_WINDOW_LIMIT,
    DEFAULT_WINDOW_LIMIT) * 1000

  /**
   * Number of events to get
   */
  private val eventFetchLimit = sparkConf.getLong(OPTION_EVENT_FETCH_LIMIT,
    DEFAULT_EVENT_FETCH_LIMIT)

  private val eventFetchOption: Option[Long] =
    if (eventFetchLimit > 0) Some(eventFetchLimit) else None

  /**
   * Clock. Subclasses may override for testing
   */
  @VisibleForTesting
  protected var clock: Clock = new SystemClock()

  /**
   * Start time.
   */
  val serviceStartTime = clock.getTimeMillis()

  /**
   * Timeline endpoint URI
   */
  @VisibleForTesting
  protected val timelineEndpoint = createTimelineEndpoint()

  /**
   * The timeline query client which uses the `jersey`
   * Jersey instance to talk to a timeline service running
   * at [[timelineEndpoint]], and creates a timeline (write) client instance
   * to handle token renewal
   *
   */
  @VisibleForTesting
  protected val timelineQueryClient = {
    createTimelineQueryClient()
  }
  
  /**
   * Override point: create the timeline endpoint
   * @return a URI to the timeline web service
   */
  @VisibleForTesting
  protected def createTimelineEndpoint(): URI = {
    getTimelineEndpoint(yarnConf)
  }

  /**
   * Create the timeline query client.
   * This is called during instance creation.
   * Tests may override this
   * @return a timeline query client for use for the duration
   *         of this instance
   */
  @VisibleForTesting
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
   * flag to indicate an endpoint diagnostics probe should take place
   */
  @VisibleForTesting
  protected val endpointCheckExecuted = new AtomicBoolean(false)

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
   * refresher
   */
  val refresher = new Refresher()

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
      logError(TEXT_SERVICE_DISABLED)
    } else {
      logInfo(TEXT_SERVICE_ENABLED)
      logInfo(KEY_SERVICE_URL + ": " + timelineEndpoint)
      logDebug(sparkConf.toDebugString)
      // get the thread time
      logInfo(s"refresh interval $refreshInterval milliseconds")
      if (refreshInterval < 0) {
        throw new IllegalArgumentException(YarnHistoryProvider.TEXT_INVALID_UPDATE_INTERVAL +
            s": ${refreshInterval/1000}")
      }
      startRefreshThread()
    }
  }


  /**
   * Stop the service. After this point operations will fail.
   */
  override def stop(): Unit = {
    logDebug(s"Stopping $this")
    // attempt to stop the refresh thread
    if (!stopRefreshThread()) {
      closeQueryClient()
    }

  }

  /**
   * Close the query client
   */
  private def closeQueryClient(): Unit = {
    logDebug("Stopping Timeline client")
    timelineQueryClient.close()
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
    synchronized {
      lastFailureCause = None
    }
  }

  /**
   * Get the last exception
   * @return the last exception or  null
   */
  def getLastFailure(): Option[(Throwable, Date)] = {
    synchronized {
      lastFailureCause
    }
  }


  /**
   * Thread safe accessor to application list
   * @return
   */
  def getApplications(): ApplicationListingResults = synchronized {
    applications
  }

  /**
   * Thread safe call to update the application results
   * @param newVal new value
   */
  private def setApplications(newVal: ApplicationListingResults): Unit = synchronized {
    applications = newVal
  }

  /**
   * Reachability check to call before any other operation is attempted.
   * This is atomic, using the `shouldCheckEndpoint` flag to check.
   * If the endpoint check failes then the
   * `endpointCheckExecuted`  flag.
   * is reset to false and an exception thrown.
   * @return true if the check took place
   */
  protected def maybeCheckEndpoint(): Boolean = {
    if (!endpointCheckExecuted.getAndSet(true)) {
      val client = getTimelineQueryClient()
      try {
        client.endpointCheck()
        true
      } catch {
        case e: Exception =>
          // failure
          logWarning(s"Endpoint check of $client failed", e)
          setLastFailure(e)
          // reset probe so another caller may attempt it.
          endpointCheckExecuted.set(false)
          // propagate the failure
          throw e
      }
    } else {
      false
    }
  }

  /**
   * Query for the endpoint check being successful.
   * Note: also true if the check is in progress.
   * @return true if the check has succeeded, or it is actually ongoing
   *         (it may fail)
   */
  def endpointCheckSuccess(): Boolean = {
    endpointCheckExecuted.get()
  }

  /**
   * Start the refresh thread with the given interval.
   *
   * When this thread exits, it will close the `timelineQueryClient`
   * instance
   */
  def startRefreshThread(): Unit = {
    logInfo(s"Starting timeline refresh thread")
    val thread = new Thread(refresher, s"YarnHistoryProvider Refresher")
    thread.setDaemon(true)
    refresher.start(thread)
  }

  /**
   * Stop the refresh thread if there is one.
   *
   * This does not guarantee an immediate halt to the thread.
   * @return true if there was a refresh thread to stop
   */
  private def stopRefreshThread(): Boolean = {
    refresher.stopRefresher()
  }

  /**
   * Probe for the refresh thread running
   * @return true if the refresh thread has been created and is still alive
   */
  def isRefreshThreadRunning(): Boolean = {
    refresher.isRunning()
  }

  def getRefreshCount(): Long = {
    refreshCount.get()
  }

  def getRefreshFailedCount(): Long = {
    refreshFailedCount.get()
  }

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
      maybeCheckEndpoint()
      val client = getTimelineQueryClient()
      logInfo(s"getListing from: $client")
      // get the timestamp after any endpoint check
      val timestamp = now()
      val timelineEntities =
        client.listEntities(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE,
          windowStart = windowStart,
          windowEnd = windowEnd,
          limit = limit)

      val listing = timelineEntities.flatMap { en =>
        try { {
          val historyInfo = toApplicationHistoryInfo(en)
          logDebug(s"${YarnTimelineUtils.describeApplicationHistoryInfo(historyInfo) }")
          Some(historyInfo)
        }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to parse entity. ${YarnTimelineUtils.describeEntity(en) }", e)
            // skip this result
            None
        }
      }
      val incomplete = countIncompleteApplications(listing)
      logInfo(s"Found ${listing.size } applications: " +
          s"${listing.size - incomplete } complete and $incomplete incomplete")
      new ApplicationListingResults(timestamp, listing, None)
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
   * @param startup a flag to indicate this is the startup retrieval with different window policy
   * @return List of all known applications.
   */
  def listAndCacheApplications(startup: Boolean): ApplicationListingResults = {
    refreshCount.incrementAndGet()
    val history = getApplications().applications

    val current = now()
    // work out the (exclusive) start of the new window
    val nextWindowStart = findStartOfWindow(history) map { (h) =>
        // inclusive on the one retrieved last time.
        // Why? we need to include the oldest incomplete entry in our range
        val inclusiveWindow = startTime(h) - 1
        // sanity check on window size
        val earliestWindow = if (windowLimitMs > 0) current - windowLimitMs else 0
        Math.max(earliestWindow, inclusiveWindow)
    }

    val results = listApplications(windowStart = nextWindowStart)
    this.synchronized {
      if (results.succeeded) {
        // on a success, the existing application list is merged
        // creating a new aggregate application list
        logDebug(s"Listed application count: ${results.size}")
        val merged = combineResults(history, results.applications)
        logDebug(s"Existing count: ${history.size}; merged = ${merged.size} ")
        val sorted = sortApplicationsByStartTime(merged)
        // and a final result
        setApplications(new ApplicationListingResults(
          results.timestamp,
          sorted,
          None))
        resetLastFailure()
      } else {
        // on a failure, the failure cause is updated
        setLastFailure(results.failureCause.get, results.timestamp)
        // and the failure counter
        refreshFailedCount.incrementAndGet()
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
    // get the current list
    val listing = getApplications().applications
    // and queue another refresh
    triggerRefresh()
    listing
  }

  /**
   * Trigger a refresh
   */
  def triggerRefresh(): Unit = {
    refresher.refresh(now())
  }

  /**
   * Return the current time
   * @return
   */
  @VisibleForTesting
  def now(): Long = {
    clock.getTimeMillis()
  }

  /**
   * Get the last refresh attempt (Which may or may not be successful)
   * @return the last refresh time
   */
  def getLastRefreshAttemptTime(): Long = {
    refresher.lastRefreshAttemptTime
  }
  
  /**
   * Look up the timeline entity
   * @param appId application ID
   * @return the entity associated with the given application
   * @throws FileNotFoundException if no entry was found
   */
  def getTimelineEntity(appId: String): TimelineEntity = {
    logDebug(s"GetTimelineEntity $appId")
    maybeCheckEndpoint()
    getTimelineQueryClient().getEntity(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE, appId)
  }


  /**
   * Returns the Spark UI for a specific application.
   * <p>
   * If the timeline is  not enabled, returns `None`
   * @param appId The application ID.
   * @param attemptId The application attempt ID (or `None` if there is no attempt ID).
   * @return The application's UI, or `None` if application is not found.
   */
  override def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI] = {
    getAppUI(appId)
  }

  /**
   * Build the application UI for an application
   * <p>
   * If the timeline is  not enabled, returns `None`
   * @param appId The application ID.
   * @return The application's UI, or `None` if application is not found.
   */
  def getAppUI(appId: String): Option[SparkUI] = {
    logDebug(s"Request UI with appId $appId")
    if (!enabled) {
      // Timeline is disabled: return nothing
      return None
    }
    maybeCheckEndpoint()
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
          HistoryServer.UI_PATH_PREFIX + s"/${appId }", entity.getStartTime)
      }
      val events = entity.getEvents
      logInfo(s"App $appId history contains ${events.size() } events")

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
        KEY_PROVIDER_NAME -> "Apache Hadoop YARN Timeline Service",
        KEY_START_TIME -> humanDateCurrentTZ(serviceStartTime, "(not started)"),
        KEY_SERVICE_URL -> s"$timelineURI",
        KEY_ENABLED -> (if (enabled) TEXT_SERVICE_ENABLED else TEXT_SERVICE_DISABLED),
        KEY_LAST_UPDATED -> applications.updated,
        KEY_CURRENT_TIME -> humanDateCurrentTZ(now(), "unknown")
      )
      // in a secure cluster, list the user name
      if (UserGroupInformation.isSecurityEnabled) {
        state += (KEY_USERNAME -> UserGroupInformation.getCurrentUser.getUserName)
      }

      // on a failure, add failure specifics to the operations
      failure foreach {
        case (ex , date) =>
          state = state ++
            Map(
              KEY_LAST_FAILURE_TIME -> humanDateCurrentTZ(date.getTime, TEXT_NEVER_UPDATED),
              KEY_LAST_FAILURE -> ex.toString)
      }
      // add detailed information if enabled
      if (detailedInfo) {
        state = state ++ Map(
          KEY_TOKEN_RENEWAL -> humanDateCurrentTZ(timelineQueryClient.lastTokenRenewal, TEXT_NEVER_UPDATED),
          KEY_TOKEN_RENEWAL_COUNT -> timelineQueryClient.tokenRenewalCount.toString,
          KEY_TO_STRING -> s"$this",
          KEY_MIN_REFRESH_INTERVAL -> refreshInterval.toString,
          KEY_EVENT_FETCH_LIMIT -> eventFetchLimit.toString
        )
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
    s" refresh count = ${getRefreshCount()}; failed count = ${getRefreshFailedCount()};" +
    s" last update ${applications.updated};" +
    s" history size ${applications.size};" +
    s" ${refresher}"
  }

  /**
   * Comparison function that defines the sort order for the application listing.
   *
   * @return Whether `i1` should precede `i2`.
   */
  private def compareAppInfo(
      i1: ApplicationHistoryInfo,
      i2: ApplicationHistoryInfo): Boolean = {
    val a1 = i1.attempts.head
    val a2 = i2.attempts.head
    if (a1.endTime != a2.endTime) a1.endTime >= a2.endTime else a1.startTime >= a2.startTime
  }
  
  /**
   * This is the implementation of the triggered refresh logic.
   * It awaits events
   */

  private[yarn] class Refresher extends Runnable {

    private[yarn] sealed trait RefreshActions

    /** start the refresh **/
    private[yarn] case class Start() extends RefreshActions

    /** refresh requested at the given time */
    private[yarn] case class RefreshRequest(time: Long) extends RefreshActions

    /** stop */
    private[yarn] case class StopExecution() extends RefreshActions

    private val queue = new LinkedBlockingQueue[RefreshActions]()
    private val running = new AtomicBoolean(false)
    private var self: Thread = _
    private val _lastRefreshAttemptTime = new AtomicLong(0)
    private val _messagesProcessed = new AtomicLong(0)
    private val _refreshesExecuted = new AtomicLong(0)

    /**
     * Bond to the thread then start it
     * @param t thread
     */
    def start(t: Thread) {
      this.synchronized {
        self = t
        running.set(true)
        queue.add(Start())
        t.start()
      }
    }

    /**
     * Request a refresh. If the request queue is empty, a refresh request
     * is queued.
     * @param time time request was made
     */
    def refresh(time: Long): Unit = {
      if (queue.isEmpty) {
        queue.add(RefreshRequest(time))
      }
    }

    /**
     * Stop operation.
     * @return true if the stop was scheduled
     */
    def stopRefresher(): Boolean = {
      this.synchronized {
        if (isRunning()) {
          // yes, more than one stop may get issued. but it will
          // replace the previous one.
          queue.clear()
          queue.add(StopExecution())
          self.interrupt()
          true
        } else {
          false
        }
      }
    }

    /**
     * Thread routine
     */
    override def run(): Unit = {
      try {
        var stopped = false
        while (!stopped) {
          take match {
            case StopExecution() =>
              // stop: exit the loop
              stopped = true
            case Start() =>
              // initial read; may be bigger
              doRefresh(true)
            case RefreshRequest(time) =>
              // requested refresh operation
              doRefresh(false)
          }
          // it is only after processing the
          // message that the message process counter
          // is incremented
          _messagesProcessed.incrementAndGet()

        }
      } finally {
        closeQueryClient()
        running.set(false)
      }
    }

    /**
     * Do the real refresh.
     * This contains the decision making as when to refresh, which consists of
     * 1. the refresh interval == 0 (always)
     * 2. the last refresh was outside the window.
     *
     * There isn't a special check for "never updated", as this
     * would only be inside the window in test cases with a small
     * simulated clock.
     *
     * @param startup a flag to indicate this is the startup retrieval with different window policy
     */
    private def doRefresh(startup: Boolean): Unit = {
      val current = now()
      if (refreshInterval == 0
          || ((now() - _lastRefreshAttemptTime.get()) >= refreshInterval )) {
        logDebug("refresh triggered")
        listAndCacheApplications(true)
        _lastRefreshAttemptTime.set(now())
        _refreshesExecuted.incrementAndGet()
      }
    }

    /**
     * Get the next action; increment the [[_messagesProcessed]]
     * counter after
     * @return the next action
     */
    def take(): Refresher.this.RefreshActions = {
      queue.take()
    }

    /**
     * Flag to indicate the refresher thread is running
     * @return
     */
    def isRunning(): Boolean  = {
      running.get()
    }

    /**
     * Get the last refresh time
     * @return the last refresh time
     */
    def lastRefreshAttemptTime: Long = {
      _lastRefreshAttemptTime.get()
    }

    /**
     * Get count of messages processed.
     * This will be at least equal to
     * the number of refreshes executed
     * @return
     */
    def messagesProcessed: Long = {
      _messagesProcessed.get()
    }

    /**
     * The number of actual refreshes triggered
     * @return a count of refreshes
     */
    def refreshesExecuted: Long = {
      _refreshesExecuted.get()
    }

    override def toString: String = {
      s"Refresher running = $isRunning queue size = ${queue.size()};" +
        s" window = $refreshInterval mS;" +
        s" processed = $messagesProcessed;" +
        s" refreshes executed = ${_refreshesExecuted.get()};" +
        s" last refresh attempt = " + timeShort(lastRefreshAttemptTime, "never") + ";"
    }
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

  /**
   * Size of applications in the list
   * @return
   */
  def size: Int = {
    applications.size
  }
}

/**
 * Constants to go with the hstory provider.
 *
 * 1. Any with the prefix `KEY_` are for configuration (key, value) pairs, so can be
 * searched for after scraping the History server web page.
 *
 * 2. Any with the prefix `OPTION_` are options from the configuration.
 *
 * 3. Any with the prefix `DEFAULT_` are the default value of options
 *
 * 4. Any with the prefix `TEXT_` are text messages which may appear in web pages
 * and other messages (and so can be scanned for in tests)
 */
private[spark] object YarnHistoryProvider {

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
  val TEXT_INVALID_UPDATE_INTERVAL = s"Invalid update interval defined in $OPTION_MIN_REFRESH_INTERVAL"
  
  
  val KEY_LISTING_REFRESH_INTERVAL = "Update Interval"

  /**
   * Option for the interval for listing timeline refreshes. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val OPTION_MIN_REFRESH_INTERVAL = "spark.history.yarn.min-refresh-interval"
  val DEFAULT_MIN_REFRESH_INTERVAL_SECONDS = 60

  /**
   * Option for the number of events to retrieve
   */
  val OPTION_EVENT_FETCH_LIMIT = "spark.history.yarn.event-fetch-limit"
  val DEFAULT_EVENT_FETCH_LIMIT = 1000

  /**
   * Maximum timeline of the window when getting updates.
   * If set to zero, there's no limit
   */
  val OPTION_WINDOW_LIMIT = "spark.history.yarn.window.limit"
  val DEFAULT_WINDOW_LIMIT = "24h"

  val OPTION_DETAILED_INFO = "spark.history.yarn.diagnostics"

  val KEY_TOKEN_RENEWAL = "x-Token Renewed"
  val KEY_TOKEN_RENEWAL_COUNT = "x-Token Renewal Count"
  val KEY_TO_STRING = "x-Internal State"
  val KEY_USERNAME = "User"
  val KEY_MIN_REFRESH_INTERVAL = "x-Minimum refresh interval"
  val KEY_CURRENT_TIME = "Current Time"
  val KEY_EVENT_FETCH_LIMIT = "x-" + OPTION_EVENT_FETCH_LIMIT
}
