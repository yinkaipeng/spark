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

import java.net.URI
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable

import com.codahale.metrics.{Counter, Counting, Metric, MetricRegistry}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEntityGroupId, TimelineEvent}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.publish.{EntityConstants, EventPublisher}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockUpdated, SparkListenerEvent, SparkListenerExecutorMetricsUpdate}
import org.apache.spark.scheduler.cluster.{SchedulerExtensionService, SchedulerExtensionServiceBinding}

/**
 * A Yarn Extension Service to post lifecycle events to a registered YARN Timeline Server.
 *
 * Posting algorithm
 *
 * 1. The service subscribes to all events coming from the Spark Context.
 * 1. These events are serialized into JSON objects for publishing to the timeline service through
 * HTTP(S) posts.
 * 1. Events are buffered into `pendingEvents` until a batch is aggregated into a
 * [[TimelineEntity]] for posting.
 * 1. That aggregation happens when a lifecycle event (application start/stop) takes place,
 * or the number of pending events in a running application exceeds the limit set in
 * `spark.hadoop.yarn.timeline.batch.size`.
 * 1. Posting operations take place in a separate thread from the spark event listener.
 * 1. If an attempt to post to the timeline server fails, the service sleeps and then
 * it is re-attempted after the retry period defined by
 * `spark.hadoop.yarn.timeline.post.retry.interval`.
 * 1. If the number of events buffered in the history service exceeds the configured limit set,
 * then further events other than application start/stop are dropped.
 * 1. When the service is stopped, it will make a best-effort attempt to post all queued events.
 * the call of [[stop()]] can block up to the duration of
 * `spark.hadoop.yarn.timeline.shutdown.waittime` for this to take place.
 * 1. No events are posted until the service receives a [[SparkListenerApplicationStart]] event.
 *
 * If the spark context has a metrics registry, then the internal counters of queued entities,
 * post failures and successes, and the performance of the posting operation are all registered
 * as metrics.
 *
 * The shutdown logic is somewhat convoluted, as the posting thread may be blocked on HTTP IO
 * when the shutdown process begins. In this situation, the thread continues to be blocked, and
 * will be interrupted once the wait time has expired. All time consumed during the ongoing
 * operation will be counted as part of the shutdown time period.
 */
private[spark] class YarnHistoryService extends SchedulerExtensionService with Logging
  with TimeSource {

  import org.apache.spark.deploy.history.yarn.YarnHistoryService._

  /** Simple state model implemented in an atomic integer. */
  private val _serviceState = new AtomicInteger(CreatedState)

  /** Get the current state. */
  def serviceState: Int = {
    _serviceState.get()
  }

  /**
   * Atomic operatin to enter a new state, returning the old one.
   * There are no checks on state model.
    *
    * @param state new state
   * @return previous state
   */
  private def enterState(state: Int): Int = {
    logDebug(s"Entering state $state from $serviceState")
    _serviceState.getAndSet(state)
  }

  /** Spark context; valid once started. */
  private var sparkContext: SparkContext = _

  /** YARN configuration from the spark context. */
  private var config: YarnConfiguration = _

  private[yarn] var applicationInfo: Option[AppAttemptDetails] = None

  /** Application ID. */
  private[yarn] def applicationId: ApplicationId = {
    if (applicationInfo.isDefined) {
      applicationInfo.get.appId
    } else {
      null
    }
  }

  /** Attempt ID -this will be null if the service is started in yarn-client mode. */
  private def attemptId: Option[ApplicationAttemptId] = {
    applicationInfo.flatMap(_.attemptId)
  }

  /** YARN timeline client. */
  private var _timelineClient: Option[TimelineClient] = None

  /** Does the the timeline server support v 1.5 APIs? */
  private var timelineVersion1_5 = false

  /** Registered event listener. */
  private var listener: Option[YarnEventListener] = None

  private var sparkAttemptDetails: SparkAppAttemptDetails = _

  /** Application ID received from a [[SparkListenerApplicationStart]]. */
  private var sparkApplicationId: Option[String] = None

  /** Optional Attempt ID string from [[SparkListenerApplicationStart]]. */
  private var sparkApplicationAttemptId: Option[String] = None

  /** Start time of the application, as received in the start event. */
  private var startTime: Long = _

  /** Start time of the application, as received in the end event. */
  private var endTime: Long = _

  /** Number of events to batch up before posting. */
  private[yarn] var batchSize = DEFAULT_BATCH_SIZE

  /** Limit on the total number of events permitted. */
  private var postQueueLimit = DEFAULT_POST_EVENT_LIMIT

  /** List of events which will be pulled into a timeline entity when created. */
  private var pendingEvents = new mutable.MutableList[TimelineEvent]()

  /** The received application started event; `None` if no event has been received. */
  private var applicationStartEvent: Option[SparkListenerApplicationStart] = None

  /** The received application end event; `None` if no event has been received. */
  private var applicationEndEvent: Option[SparkListenerApplicationEnd] = None

  /** Has a start event been processed? */
  private val appStartEventProcessed = new AtomicBoolean(false)

  /** Has the application event event been processed? */
  private val appEndEventProcessed = new AtomicBoolean(false)

  /** Counter of events processed -that is have been through `handleEvent()`. */
  val eventsProcessedCounter = new Counter()
  
  /** Domain ID for entities: may be null. */
  private var domainId: Option[String] = None

  /** URI to timeline web application -valid after [[start()]]. */
  private[yarn] var timelineWebappAddress: URI = _

  /** Metric fields. Used in tests as well as metrics infrastructure. */
  val metrics = new HistoryMetrics()

  /**
   * A map of metrics for registration and local lookup
   */
  private val metricsMap = mutable.Map[String, Metric]()

  /**
   * A counter incremented every time a new entity is created. This is included as an "other"
   * field in the entity information -so can be used as a probe to determine if the entity
   * has been updated since a previous check.
   */
  private val entityVersionCounter = new AtomicLong(1)

  /**
   * Create a timeline client and start it. This does not update the
   * `_timelineClient` field, though it does verify that the field
   * is unset.
   *
   * The method is private to the package so that tests can access it, which
   * some of the mock tests do to override the timeline client creation.
   *
   * @return the timeline client
   */
  private[yarn] def createTimelineClient(): TimelineClient = {
    require(_timelineClient.isEmpty, "timeline client already set")
    YarnTimelineUtils.createYarnTimelineClient(sparkContext.hadoopConfiguration)
  }

  /**
   * Get the timeline client.
   *
   * @return the client
   * @throws Exception if the timeline client is not currently running
   */
  def timelineClient: TimelineClient = {
    synchronized { _timelineClient.get }
  }

  /**
   * Event publisher
   */
  var publisher: Option[EventPublisher] = None

  /**
   * Get the configuration of this service.
   *
   * @return the configuration as a YarnConfiguration instance
   */
  def yarnConfiguration: YarnConfiguration = config

  /** Counter of events queued. */
  val sparkEventsQueued = new Counter()

  /** The number of events which were dropped as the backlog of pending posts was too big. */
  val eventsDropped = new Counter()

  /** How many flushes have taken place? */
  val flushCount = new Counter()

  /**
   * Get the total number of processed events, those handled in the back-end thread without
   * being rejected.
   *
   * @return counter of events processed
   */
  def eventsProcessed: Long = eventsProcessedCounter.getCount

  /**
   * Get the total number of events queued.
   *
   * @return the total event count
   */
  def eventsQueued: Long = sparkEventsQueued.getCount

  /**
    * Get the current size of the posting queue in terms of outstanding actions.
    *
    * @return the current queue length
    */
  def postQueueActionSize: Int = {
    publisher.map(_.postingQueueSize).getOrElse(0)
  }

  /**
   * Get the number of events in the posting queue.
   *
   * @return a counter of outstanding events
   */
  def postQueueEventSize: Long = {
    publisher.map(_.postQueueEventSize).getOrElse(0)
  }

  /**
   * Query the counter of attempts to post entities to the timeline service.
   *
   * @return the current value
   */
  def postAttempts: Long = {
    publisher.map(_.entityPostAttempts.getCount).getOrElse(0)
  }

  /**
   * Get the total number of failed post operations.
   *
   * @return counter of timeline post operations which failed
   */
  def postFailures: Long = {
    publisher.map(_.entityPostFailures.getCount).getOrElse(0)
  }

  /**
   * Query the counter of successful post operations (this is not the same as the
   * number of events posted).
   *
   * @return the number of successful post operations.
   */
  def postSuccesses: Long = {
    publisher.map(_.entityPostSuccesses.getCount).getOrElse(0)
  }

  /**
   * Is the asynchronous posting thread active?
   *
   * @return true if the post thread has started; false if it has not yet/ever started, or
   *         if it has finished.
   */
  def isPostThreadActive: Boolean = {
    publisher.map(_.isPostThreadActive).getOrElse(false)
  }

  /**
   * Reset the timeline client. Idempotent.
   *
   * 1. Stop the timeline client service if running.
   * 2. set the `timelineClient` field to `None`
   */
  def stopTimelineClient(): Unit = {
    synchronized {
      _timelineClient.foreach(_.stop())
      _timelineClient = None
    }
  }

  /**
   * Create the timeline domain.
   *
   * A Timeline Domain is a uniquely identified 'namespace' for accessing parts of the timeline.
   * Security levels are are managed at the domain level, so one is created if the
   * spark acls are enabled. Full access is then granted to the current user,
   * all users in the configuration options `"spark.modify.acls"` and `"spark.admin.acls"`;
   * read access to those users and those listed in `"spark.ui.view.acls"`
   *
   * @return an optional domain string. If `None`, then no domain was created.
   */
  private def createTimelineDomain(eventPublisher: EventPublisher): Option[String] = {
    val sparkConf = sparkContext.getConf
    val aclsOn = sparkConf.getBoolean("spark.ui.acls.enable",
        sparkConf.getBoolean("spark.acls.enable", false))
    if (!aclsOn) {
      logDebug("ACLs are disabled; not creating the timeline domain")
      return None
    }
    val predefDomain = sparkConf.getOption(TIMELINE_DOMAIN)
    if (predefDomain.isDefined) {
      logDebug(s"Using predefined domain $predefDomain")
      return predefDomain
    }
    val current = UserGroupInformation.getCurrentUser.getShortUserName
    val adminAcls = stringToSet(sparkConf.get("spark.admin.acls", ""))
    val viewAcls = stringToSet(sparkConf.get("spark.ui.view.acls", ""))
    val modifyAcls = stringToSet(sparkConf.get("spark.modify.acls", ""))

    val readers = (Seq(current) ++ adminAcls ++ modifyAcls ++ viewAcls).mkString(" ")
    val writers = (Seq(current) ++ adminAcls ++ modifyAcls).mkString(" ")
    val domain = DOMAIN_ID_PREFIX + applicationId
    logInfo(s"Creating domain $domain with readers: $readers and writers: $writers")

    // create the timeline domain with the reader and writer permissions
    try {
      eventPublisher.putNewDomain(domain, readers, writers)
      Some(domain)
    } catch {
      case e: Exception =>
        logError(s"cannot create the domain $domain", e)
        // fallback to default
        None
    }
  }

  /**
   * Start the service.
   *
   * @param binding binding to the spark application and YARN
   */
  override def start(binding: SchedulerExtensionServiceBinding): Unit = {
    val oldstate = enterState(StartedState)
    if (oldstate != CreatedState) {
      // state model violation
      _serviceState.set(oldstate)
      throw new IllegalArgumentException(s"Cannot start the service from state $oldstate")
    }
    val context = binding.sparkContext
    val appId = binding.applicationId
    val attemptId = binding.attemptId
    require(context != null, "Null context parameter")
    logDebug(s"Starting YarnHistoryService with appID $appId, attempt $attemptId ")
    this.sparkContext = context
    this.config = new YarnConfiguration(context.hadoopConfiguration)
    timelineVersion1_5 = timelineServiceV1_5Enabled(config)
    val sparkConf = sparkContext.conf
    bindToYarnApplication(appId, attemptId,
      if (timelineVersion1_5) Some(appId.toString) else None)

    // work out the attempt ID from the YARN attempt ID. No attempt, assume "1".
    val attempt1 = attemptId match {
      case Some(attempt) => attempt.getAttemptId.toString
      case None => CLIENT_BACKEND_ATTEMPT_ID
    }
    setContextAppAndAttemptInfo(Some(appId.toString), Some(attempt1), attempt1, "")

    def intOption(key: String, defVal: Int): Int = {
      val v = sparkConf.getInt(key, defVal)
      require(v > 0, s"Option $key out of range: $v")
      v
    }


    batchSize = intOption(BATCH_SIZE, batchSize)
    postQueueLimit = batchSize + intOption(POST_EVENT_LIMIT, postQueueLimit)


    registerMetricSource(metrics)

    // set up the timeline service, unless it's been disabled
    if (timelineServiceEnabled) {
      startTimelineReporter()
      if (registerListener()) {
        logInfo(s"History Service listening for events: $this")
      } else {
        // special test option; listener is inactive
        logWarning(s"History Service is not listening for events: $this")
      }
    } else {
      logInfo("YARN History Service integration is disabled")
    }
  }

  /**
   * Start the timeline reporter: instantiate the client, start the background
   * entity posting thread.
   */
  def startTimelineReporter(): Unit = {
    timelineWebappAddress = getTimelineEndpoint(config)

    logInfo(s"Starting $this")
    val version = config.getFloat(
      YarnConfiguration.TIMELINE_SERVICE_VERSION,
      YarnConfiguration.DEFAULT_TIMELINE_SERVICE_VERSION)
    val timeline = createTimelineClient()
    _timelineClient = Some(timeline)
    def millis(key: String, defVal: String): Long = {
      1000 * sparkContext.conf.getTimeAsSeconds(key, defVal)
    }

    /** ATS v 1.5 group ID. */
    val groupId = if (timelineVersion1_5) {
      Some(TimelineEntityGroupId.newInstance(applicationId,
        applicationInfo.get.groupId.get))
    } else {
      None
    }

    // create the publisher
    val eventPublisher = new EventPublisher(
      applicationInfo,
      attemptId,
      timeline,
      timelineWebappAddress,
      timelineVersion1_5,
      groupId,
      millis(POST_RETRY_INTERVAL, DEFAULT_POST_RETRY_INTERVAL),
      millis(POST_RETRY_MAX_INTERVAL, DEFAULT_POST_RETRY_MAX_INTERVAL),
      millis(SHUTDOWN_WAIT_TIME, DEFAULT_SHUTDOWN_WAIT_TIME))
    registerMetricSource(eventPublisher)

    // create the timeline domain with the reader and writer permissions
    domainId = createTimelineDomain(eventPublisher)
    logInfo(s"Spark events will be published to $timelineWebappAddress"
      + s" API version=$version; domain ID = $domainId; client=${_timelineClient.toString}")
    eventPublisher.start()
    publisher = Some(eventPublisher)
  }

  /**
   * Check the service configuration to see if the timeline service is enabled.
   *
   * @return true if `YarnConfiguration.TIMELINE_SERVICE_ENABLED` is set.
   */
  def timelineServiceEnabled: Boolean = {
    YarnTimelineUtils.timelineServiceEnabled(config)
  }

  /**
   * Return a summary of the service state to help diagnose problems
   * during test runs, possibly even production.
   *
   * @return a summary of the current service state
   */
  override def toString(): String =
    s"""YarnHistoryService for application $applicationId attempt $attemptId;
       | state=$serviceState;
       | endpoint=$timelineWebappAddress;
       | bonded to ATS=$bondedToATS;
       | ATS v1.5=$timelineVersion1_5
       | listening=$listening;
       | batchSize=$batchSize;
       | postQueueLimit=$postQueueLimit;
       | postQueueSize=$postQueueActionSize;
       | postQueueEventSize=$postQueueEventSize;
       | flush count=$getFlushCount;
       | total number queued=$eventsQueued, processed=$eventsProcessed;
       | attempted entity posts=$postAttempts
       | successful entity posts=$postSuccesses
       | failed entity posts=$postFailures;
       | events dropped=${eventsDropped.getCount};
       | app start event received=$appStartEventProcessed;
       | start time=$startTime;
       | app end event received=$appEndEventProcessed;
       | end time=$endTime;
       | publisher=$publisher;
     """.stripMargin

  /**
   * Is the service listening to events from the spark context?
   *
   * @return true if it has registered as a listener
   */
  def listening: Boolean = {
    listener.isDefined
  }

  /**
   * Is the service hooked up to an ATS server?
   *
   * This does not check the validity of the link, only whether or not the service
   * has been set up to talk to ATS.
   *
   * @return true if the service has a timeline client
   */
  def bondedToATS: Boolean = {
    _timelineClient.isDefined
  }

  /**
   * Set the YARN binding information.
   *
   * This is called during startup. It is private to the package so that tests
   * may update this data.
    *
    * @param appId YARN application ID
   * @param maybeAttemptId optional attempt ID
   */
  private[yarn] def bindToYarnApplication(appId: ApplicationId,
      maybeAttemptId: Option[ApplicationAttemptId],
      groupId: Option[String]): Unit = {
    require(appId != null, "Null appId parameter")
    applicationInfo = Some(AppAttemptDetails(appId, maybeAttemptId, groupId))
  }

  /**
   * Set the "spark" application and attempt information -the information
   * provided in the start event. The attempt ID here may be `None`; even
   * if set it may only be unique amongst the attempts of this application.
   * That is: not unique enough to be used as the entity ID
   *
   * @param appId application ID
   * @param attemptId attempt ID
   */
  private def setContextAppAndAttemptInfo(
      appId: Option[String],
      attemptId: Option[String],
      name: String,
      user: String): Unit = {
    logDebug(s"Setting Spark application ID to $appId; attempt ID to $attemptId")
    sparkApplicationId = appId
    sparkApplicationAttemptId = attemptId
    sparkAttemptDetails = SparkAppAttemptDetails(appId, attemptId, name, user)
  }

  /**
   * Add the listener if it is not disabled.
   * This is accessible in the same package purely for testing
   *
   * @return true if the register was enabled
   */
  private[yarn] def registerListener(): Boolean = {
    assert(sparkContext != null, "Null context")
    if (sparkContext.conf.getBoolean(REGISTER_LISTENER, true)) {
      logDebug("Registering listener to spark context")
      val l = new YarnEventListener(sparkContext, this)
      listener = Some(l)
      sparkContext.listenerBus.addListener(l)
      true
    } else {
      false
    }
  }

  /**
   * Process an action, or, if the service's `stopped` flag is set, discard it.
   *
   * This is the method called by the event listener when forwarding events to the service,
   * and at shutdown.
   *
   * @param event event to process
   * @return true if the event was queued
   */
  def process(event: SparkListenerEvent): Boolean = {
    if (!publisher.map(_.isPostingQueueStopped).getOrElse(true)) {
      sparkEventsQueued.inc()
      logDebug(s"Enqueue $event")
      handleEvent(event)
      true
    } else {
      // the service is stopped, so the event will not be processed.
      if (timelineServiceEnabled) {
        // if a timeline service was ever enabled, log the fact the event
        // is being discarded. Don't do this if it was not, as it will
        // only make the (test run) logs noisy.
        logInfo(s"History service stopped; ignoring queued event : $event")
      }
      false
    }
  }

  /**
   * Stop the service; this triggers flushing the queue and, if not already processed,
   * a pushing out of an application end event.
   *
   * This operation will block for up to `maxTimeToWaitOnShutdown` milliseconds
   * to await the asynchronous action queue completing.
   */
  override def stop(): Unit = {
    if (enterState(StoppedState) != StartedState) {
      // stopping from a different state
      logDebug(s"Ignoring stop() request from state ${enterState(StoppedState)}")
      return
    }
    try {
      stopPublisher()
    } finally {
      contextMetricsSystem.foreach( _.removeSource(metrics))
    }
  }

  /**
   * Return the metrics system of the context/environment if there is one,
   * and metrics are enabled for this class.
   *
   * @return an optional metrics system
   */
  private def contextMetricsSystem: Option[MetricsSystem] = {
    if (metricsEnabled) {
      Option(sparkContext.env.metricsSystem)
    } else {
      None
    }
  }

  /**
   * Register the metrics source with any system-wide metrics, and into the
   * local metrics map for string lookup
   * @param m metric source
   */
  def registerMetricSource(m: ExtendedMetricsSource): Unit = {
    contextMetricsSystem.foreach(_.registerSource(m))
    m.metricsMap.foreach( e => metricsMap.put(e._1, e._2))
  }

  def lookupMetric(name: String): Option[Metric] = {
    metricsMap.get(name)
  }

  /**
   * Get a count by name; return -1 if it is none
   * @param name metric name
   * @return value or -1
   */
  def counterMetric(name: String): Long = {
    lookupMetric(name) match {
      case Some(c: Counting) =>
        c.getCount
      case _ => -1L
    }
  }

  /**
   * Stop the publisher
   */
  private def stopPublisher(): Unit = {
    // if the queue is live
    publisher.foreach { pub =>
      if (!pub.isPostingQueueStopped) {

        if (appStartEventProcessed.get && !appEndEventProcessed.get) {
          // push out an application stop event if none has been received
          logDebug("Generating a SparkListenerApplicationEnd during service stop()")
          process(SparkListenerApplicationEnd(now()))
        }

        // flush out the events
        asyncFlush()

        // push out that queue stop event; this immediately sets the `queueStopped` flag
        pub.pushQueueStop()

        // Now await the halt of the posting thread.
        var shutdownPosted = pub.awaitQueueCompletion();
        if (!shutdownPosted) {
          // there was no running post thread, just stop the timeline client ourselves.
          // (if there is a thread running, it must be the one to stop it)
          pub.stopTimelineClient()
          logInfo(s"Stopped: $this")
        }
      }
    }
  }

  /**
   * Can an event be added?
   *
   * The policy is: only if the number of queued entities is below the limit, or the
   * event marks the end of the application.
   *
   * @param isLifecycleEvent is this operation triggered by an application start/end?
   * @return true if the event can be added to the queue
   */
  private def canAddEvent(isLifecycleEvent: Boolean): Boolean = {
    isLifecycleEvent || postQueueHasCapacity
  }

  /**
   * Does the post queue have capacity for this event?
   *
   * @return true if the count of queued events is below the limit
   */
  private def postQueueHasCapacity: Boolean = {
    sparkEventsQueued.getCount < postQueueLimit
  }

  /**
   * Add another event to the pending event list.
   *
   * Returns the size of the event list after the event was added
   * (thread safe).
    *
    * @param event event to add
   * @return the event list size
   */
  private def addPendingEvent(event: TimelineEvent): Int = {
    pendingEvents.synchronized {
      pendingEvents :+= event
      pendingEvents.size
    }
  }

  /**
   * Publish next set of pending events if there are events to publish,
   * and the application has been recorded as started.
   *
   * @return true if another entity was queued
   */
  private def publishPendingEvents(): Boolean = {
    // verify that there are events to publish
    val size = pendingEvents.synchronized {
      pendingEvents.size
    }
    if (size > 0 && applicationStartEvent.isDefined) {
      // push if there are events *and* the app is recorded as having started.
      // -as the app name is needed for the the publishing.
      flushCount.inc()
      val t = now()
      val count = entityVersionCounter.getAndIncrement()
      // create the detail entry. On ATS 1.0, this is essentially the
      // summary entry
      val detail = createTimelineEntity(false, t, count)
      // copy in pending events and then reset the list
      var oldPendingEvents: mutable.MutableList[TimelineEvent] = null
      pendingEvents.synchronized {
        oldPendingEvents = pendingEvents
        pendingEvents = new mutable.MutableList[TimelineEvent]()
      }
      oldPendingEvents.foreach(detail.addEvent)

      publisher.foreach(_.queueForPosting(detail))

      if (timelineVersion1_5) {
        // ATS 1.5: push out an updated summary entry
        publisher.foreach(_.
            queueForPosting(createTimelineEntity(true, t, count)))
      }
      true
    } else {
      false
    }
  }


  def createEntityType(isSummaryEntity: Boolean): String = {
    if (!timelineVersion1_5 || isSummaryEntity) {
      EntityConstants.SPARK_SUMMARY_ENTITY_TYPE
    } else {
      EntityConstants.SPARK_DETAIL_ENTITY_TYPE
    }
  }

  /**
   * Create a timeline entity populated with the state of this history service
   * @param isSummaryEntity entity type: summary or full
   * @param timestamp timestamp
   * @return
   */
  def createTimelineEntity(
      isSummaryEntity: Boolean,
      timestamp: Long,
      entityCount: Long): TimelineEntity = {
    YarnTimelineUtils.createTimelineEntity(
      createEntityType(isSummaryEntity),
      applicationInfo.get,
      sparkAttemptDetails,
      startTime,
      endTime,
      timestamp,
      entityCount)
  }

  /**
    * Queue an asynchronous flush operation.
    *
    * @return if the flush event was queued
    */
  def asyncFlush(): Boolean = {
    publishPendingEvents()
  }



  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event event. If null, no event is queued, but the post-queue flush logic still applies
   */
  private def handleEvent(event: SparkListenerEvent): Unit = {
    // publish events unless stated otherwise
    var publish = true
    // don't trigger a push to the ATS
    var push = false
    // lifecycle events get special treatment: they are never discarded from the queues,
    // even if the queues are full.
    var isLifecycleEvent = false
    val timestamp = now()
    eventsProcessedCounter.inc()
    if (eventsProcessedCounter.getCount() % 1000 == 0) {
      logDebug(s"${eventsProcessedCounter} events are processed")
    }
    event match {
      case start: SparkListenerApplicationStart =>
        // we already have all information,
        // flush it for old one to switch to new one
        logDebug(s"Handling application start event: $event")
        if (!appStartEventProcessed.getAndSet(true)) {
          applicationStartEvent = Some(start)
          var applicationName = start.appName
          if (applicationName == null || applicationName.isEmpty) {
            logWarning("Application does not have a name")
            applicationName = applicationId.toString
          }
          startTime = start.time
          if (startTime == 0) {
            startTime = timestamp
          }
          setContextAppAndAttemptInfo(start.appId, start.appAttemptId, applicationName,
            start.sparkUser)
          logDebug(s"Application started: $event")
          isLifecycleEvent = true
          push = true
        } else {
          logWarning(s"More than one application start event received -ignoring: $start")
          publish = false
        }

      case end: SparkListenerApplicationEnd =>
        if (!appStartEventProcessed.get()) {
          // app-end events being received before app-start events can be triggered in
          // tests, even if not seen in real applications.
          // react by ignoring the event altogether, as an un-started application
          // cannot be reported.
          logError(s"Received application end event without application start $event -ignoring.")
        } else if (!appEndEventProcessed.getAndSet(true)) {
          // the application has ended
          logDebug(s"Application end event: $event")
          applicationEndEvent = Some(end)
          // flush old entity
          endTime = if (end.time > 0) end.time else timestamp
          push = true
          isLifecycleEvent = true
        } else {
          // another test-time only situation: more than one application end event
          // received. Discard the later one.
          logInfo(s"Discarding duplicate application end event $end")
          publish = false
        }

      case update: SparkListenerBlockUpdated =>
        publish = false

      case update: SparkListenerExecutorMetricsUpdate =>
        publish = false

      case _ =>
    }

    if (publish) {
      val tlEvent = toTimelineEvent(event, timestamp)
      val eventCount = if (tlEvent.isDefined && canAddEvent(isLifecycleEvent)) {
        addPendingEvent(tlEvent.get)
      } else {
        // discarding the event
        // if this is due to a full queue, log it
        if (!postQueueHasCapacity) {
          logInfo(s"Queue full at ${sparkEventsQueued.getCount}, limit =$postQueueLimit" +
              s" batch size = $batchSize: discarding event $tlEvent")
          eventsDropped.inc()
        }
        0
      }

      // trigger a push if the batch limit is reached
      // There's no need to check for the application having started, as that is done later.
      if (eventCount >= batchSize) {
        logDebug(s"Event count at $eventCount > $batchSize, pushing queue ")
        // note that push may already have been true; it's unimportant here
        push = true
      }

      if (push) {
        logDebug("Push triggered")
        publishPendingEvents()
      }
    }
  }

  /**
   * Get the number of flush events that have taken place.
   *
   * This includes flushes triggered by the event list being bigger the batch size,
   * but excludes flush operations triggered when the action processor thread
   * is stopped, or if the timeline service binding is disabled.
   *
   * @return count of processed flush events.
   */
  def getFlushCount: Long = {
    flushCount.getCount
  }

  /**
   * Metrics integration: the various counters of activity
   */
  private[yarn] class HistoryMetrics extends ExtendedMetricsSource {

    /** Name for metrics: yarn_history */
    override val sourceName = YarnHistoryService.METRICS_NAME

    /** Metrics registry */
    override val metricRegistry = new MetricRegistry()

    val metricsMap: Map[String, Metric] = Map(
      "eventsDropped" -> eventsDropped,
      "eventsProcessed" -> eventsProcessedCounter,
      "sparkEventsQueued" -> sparkEventsQueued,
      "flushCount" -> flushCount)
  }

}


/**
 * Constants and defaults for the history service.
 */
private[spark] object YarnHistoryService {


  /**
   * Domain ID.
   */
  val DOMAIN_ID_PREFIX = "Spark_ATS_"

  /**
   * Time in millis to wait for shutdown on service stop.
   */
  val DEFAULT_SHUTDOWN_WAIT_TIME = "30s"

  /**
   * The maximum time in to wait for event posting to complete when the service stops.
   */
  val SHUTDOWN_WAIT_TIME = "spark.hadoop.yarn.timeline.shutdown.waittime"

  /**
   * Option to declare that the history service should register as a spark context
   * listener. (default: true; this option is here for testing)
   *
   * This is a spark option, though its use of name will cause it to propagate down to the Hadoop
   * Configuration.
   */
  val REGISTER_LISTENER = "spark.hadoop.yarn.timeline.listen"

  /**
   * Option for the size of the batch for timeline uploads. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val BATCH_SIZE = "spark.hadoop.yarn.timeline.batch.size"

  /**
   * The default size of a batch.
   */
  val DEFAULT_BATCH_SIZE = 100

  /**
   * Name of a domain for the timeline.
   */
  val TIMELINE_DOMAIN = "spark.hadoop.yarn.timeline.domain"

  /**
   * Limit on number of posts in the outbound queue -when exceeded
   * new events will be dropped.
   */
  val POST_EVENT_LIMIT = "spark.hadoop.yarn.timeline.post.limit"

    /**
   * The default limit of events in the post queue.
   */
  val DEFAULT_POST_EVENT_LIMIT = 10000

  /**
   * Interval in milliseconds between POST retries. Every
   * failure causes the interval to increase by this value.
   */
  val POST_RETRY_INTERVAL = "spark.hadoop.yarn.timeline.post.retry.interval"

  /**
   * The default retry interval in millis.
   */
  val DEFAULT_POST_RETRY_INTERVAL = "1000ms"

  /**
   * The maximum interval between retries.
   */

  val POST_RETRY_MAX_INTERVAL = "spark.hadoop.yarn.timeline.post.retry.max.interval"

  /**
   * The default maximum retry interval.
   */
  val DEFAULT_POST_RETRY_MAX_INTERVAL = "60s"

 /**
   * ID used in yarn-client attempts only.
   */
  val CLIENT_BACKEND_ATTEMPT_ID = "1"

  /**
   * The classname of the history service to instantiate in the YARN AM.
   */
  val CLASSNAME = "org.apache.spark.deploy.history.yarn.YarnHistoryService"

  /**
   * Name of metrics.
   */
  val METRICS_NAME = "yarn_history"

  /**
   * Enum value of application created state
   */
  val CreatedState = 0

  /**
   * Enum value of started state.
   */
  val StartedState = 1

  /**
   * Enum value of stopped state.
   */
  val StoppedState = 2

  @volatile var metricsEnabled = true

  /**
   * This is a flag for testing: disables metric registration and so avoids stack traces
   * from the registration code if there is more than one service instance trying to register.
   *
   * @param enabled new value
   */
  private[yarn] def enableMetricRegistration(enabled: Boolean): Unit = {
    metricsEnabled = enabled
  }


}
