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

import java.net.{ConnectException, URI}
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicInteger}
import java.util.concurrent.{TimeUnit, LinkedBlockingDeque}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.timeline.{TimelineDomain, TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{YarnExtensionService, YarnExtensionServiceBinding}
import org.apache.spark.util.{SystemClock, Utils}
import org.apache.spark.{Logging, SparkContext}

/**
 * A Yarn Extension Service to post lifecycle events to a registered
 * YARN Timeline Server.
 */
private[spark] class YarnHistoryService extends YarnExtensionService with Logging {

  import org.apache.spark.deploy.history.yarn.YarnHistoryService._

  /** Simple state model implemented in an atomic integer */
  private val _serviceState = new AtomicInteger(CreatedState)

  def serviceState: Int = {
    _serviceState.get()
  }
  def enterState(state: Int): Int = {
    logDebug(s"Entering state $state from $serviceState")
    _serviceState.getAndSet(state)
  }

  /**
   * Spark context; valid once started
   */
  private var sparkContext: SparkContext = _

  /** YARN configuration from the spark context */
  private var config: YarnConfiguration = _

  /** application ID. */
  private var _applicationId: ApplicationId = _

  /** attempt ID this will be null if the service is started in yarn-client mode */
  private var _attemptId: Option[ApplicationAttemptId] = None

  /** YARN timeline client */
  private var _timelineClient: Option[TimelineClient] = None

  /** registered event listener */
  private var listener: Option[YarnEventListener] = None

  /** Application name  from the spark start event */
  private var applicationName: String = _

  /** Application ID*/
  private var sparkApplicationId: Option[String] = None

  /** Optional Attempt ID from the spark start event */
  private var sparkApplicationAttemptId: Option[String] = None

  /** user name as derived from `SPARK_USER` env var or `UGI` */
  private var userName = Utils.getCurrentUserName()

  /** Clock for recording time */
  private val clock = new SystemClock()

  /**
   * Start time of the application, as received in the start event.
   */
  private var startTime: Long = _

  /**
   * Start time of the application, as received in the end event.
   */
  private var endTime: Long = _

  /** number of events to batch up before posting*/
  private var _batchSize = DEFAULT_BATCH_SIZE

  /** queue of entities to asynchronously post, plus the number of events in each entry */
  private var _entityQueue = new LinkedBlockingDeque[TimelineEntity]()

  /** limit on the total number of events permitted */
  private var _postQueueLimit = DEFAULT_POST_QUEUE_LIMIT

  /**
   * List of events which will be pulled into a timeline
   * entity when created
   */
  private var pendingEvents = new mutable.LinkedList[TimelineEvent]()

  private var applicationStartEvent: Option[SparkListenerApplicationStart] = None
  private var applicationEndEvent: Option[SparkListenerApplicationEnd] = None

  /** Has a start event been processed? */
  private val appStartEventProcessed = new AtomicBoolean(false)

  /* has the application event event been processed */
  private val appEndEventProcessed = new AtomicBoolean(false)

  /** counter of events processed -that is have been through handleEvent()*/
  private val _eventsProcessed = new AtomicLong(0)

  /** counter of events queued. */
  private val _eventsQueued = new AtomicLong(0)

  private val _entityPostAttempts = new AtomicLong(0)
  private val _entityPostSuccesses = new AtomicLong(0)
  /** how many entity postings failed? */
  private val _entityPostFailures = new AtomicLong(0)
  private val _eventsDropped = new AtomicLong(0)

  /** how many flushes have taken place? */
  private val flushCount = new AtomicLong(0)

  /** Event handler */
  private var eventHandlingThread: Option[Thread] = None

  /**
   * Flag to indicate the thread is stopped; events aren't being
   * processed.
   */
  private val stopped = new AtomicBoolean(true)

  /**
   * Boolean to track whether a thread is active or not, for tests to
   * monitor and see if the thread has completed.
   */
  private val postThreadActive = new AtomicBoolean(false)

  /** How long to wait for shutdown before giving up */
  private var shutdownWaitTime = 0L

  /**
   * What is the initial and incrementing interval for POST retries?
   */
  private var retryInterval = 0L

  /** Domain ID for entities: may be null */
  private var domainId: Option[String] = None

  /** URI to timeline web application -valid after `serviceStart()` */
  private var _timelineWebappAddress: URI = _

  /**
   * Create a timeline client and start it. This does not update the
   * `timelineClient` field, though it does verify that the field
   * is unset.
   *
   * The method is private to the package so that tests can access it, which
   * some of the mock tests do to override the timeline client creation.
   * @return the timeline client
   */
  private[yarn] def createTimelineClient(): TimelineClient = {
    require(_timelineClient.isEmpty, "timeline client already set")
    YarnTimelineUtils.createTimelineClient(sparkContext)
  }

  /**
   * Get the timeline client.
   * @return the client
   * @throws Exception if the timeline client is not currently running
   */
  def timelineClient: TimelineClient = {
    require(_timelineClient.isDefined)
    _timelineClient.get
  }

  /**
   * Get the total number of events dropped due to the queue of
   * outstanding posts being too long
   * @return counter of events processed
   */

  def eventsDropped: Long = {
    _eventsDropped.get()
  }

  /**
   * Get the total number of processed events, those handled in the back-end thread without
   * being rejected
   * @return counter of events processed
   */
  def eventsProcessed: Long = {
    _eventsProcessed.get
  }

  /**
   * Get the total number of events queued
   * @return the total event count
   */
  def eventsQueued: Long = {
    _eventsQueued.get
  }

  /**
   * Get the current size of the queue
   * @return the current queue length
   */
  def getQueueSize: Int = {
    _entityQueue.size()
  }

  /**
   * Get the current batch size
   * @return the batch size
   */
  def batchSize: Int = {
    _batchSize
  }

  /**
   * Query the counter of attempts to post events
   * @return
   */
  def postAttempts: Long = _entityPostAttempts.get()

  /**
   * Get the total number of failed post operations
   * @return counter of timeline post operations which failed
   */
  def postFailures: Long = {
    _entityPostFailures.get
  }

  /**
   * Query the counter of post successes
   * @return the number of successful posts
   */
  def postSuccesses: Long = _entityPostSuccesses.get()

  /**
   * is the asynchronous posting thread active?
   * @return true if the post thread has started; false if it has not yet/ever started, or
   *         if it has finished.
   */
  def isPostThreadActive: Boolean = {
    postThreadActive.get
  }

  /**
   * The YARN application ID of this history service
   * @return the application ID provided when the service started
   */
  def applicationId: ApplicationId =  _applicationId

  /**
   * The YARN attempt ID of this history service
   * @return the attempt ID provided when the service started
   */
  def attemptId: Option[ApplicationAttemptId] =  _attemptId

  /**
   * Reset the timeline client
   * <p>
   * 1. Stop the timeline client service if running.
   * 2. set the `timelineClient` field to `None`
   */
  def stopTimelineClient(): Unit = {
    _timelineClient.foreach(_.stop())
    _timelineClient = None
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
  private def createTimelineDomain(): Option[String] = {
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
    val adminAcls  = stringToSet(sparkConf.get("spark.admin.acls", ""))
    val viewAcls = stringToSet(sparkConf.get("spark.ui.view.acls", ""))
    val modifyAcls = stringToSet(sparkConf.get("spark.modify.acls", ""))

    val readers = (Seq(current) ++ adminAcls ++ modifyAcls ++ viewAcls).mkString(" ")
    val writers = (Seq(current) ++ adminAcls ++ modifyAcls).mkString(" ")
    var domain = DOMAIN_ID_PREFIX + _applicationId
    logInfo(s"Creating domain $domain with readers: $readers and writers: $writers")

    // create the timeline domain with the reader and writer permissions
    val timelineDomain = new TimelineDomain()
    timelineDomain.setId(domain)
    timelineDomain.setReaders(readers)
    timelineDomain.setWriters(writers)
    try {
      timelineClient.putDomain(timelineDomain)
      Some(domain)
    } catch {
      case e: Exception => {
        logError(s"cannot create the domain $domain", e)
        // fallback to default
        None
      }
    }
  }

  /**
   * Start the service, calling the service's `init()` and `start()` actions in the
   * correct order
   * @param binding binding to the spark application and YARN
   */
  override def start(binding: YarnExtensionServiceBinding): Unit = {
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
    bindToYarnApplication(appId, attemptId)
    this.sparkContext = context

    this.config = new YarnConfiguration(context.hadoopConfiguration)

    val sparkConf = sparkContext.conf

    // work out the attempt ID from the YARN attempt ID. No attempt, assume "1".
    // this is assumed by the AM, which uses it when creating a path to an attempt
    val attempt1 = attemptId match {
      case Some(attempt) => attempt.getAttemptId.toString
      case None => CLIENT_BACKEND_ATTEMPT_ID
    }
    setContextAppAndAttemptInfo(Some(appId.toString), Some(attempt1))
    _batchSize = sparkConf.getInt(BATCH_SIZE, _batchSize)
    _postQueueLimit = sparkConf.getInt(POST_QUEUE_LIMIT, _postQueueLimit)
    retryInterval = 1000 * sparkConf.getTimeAsSeconds(POST_RETRY_INTERVAL,
      DEFAULT_POST_RETRY_INTERVAL)
    shutdownWaitTime = 1000 * sparkConf.getTimeAsSeconds(SHUTDOWN_WAIT_TIME,
      DEFAULT_SHUTDOWN_WAIT_TIME)


    if (timelineServiceEnabled) {
      _timelineWebappAddress = getTimelineEndpoint(config)

      logInfo(s"Starting $this")
      logInfo(s"Spark events will be published to the Timeline service at ${_timelineWebappAddress}")
      _timelineClient = Some(createTimelineClient())
      domainId = createTimelineDomain()
      // declare that the processing is started
      stopped.set(false)
      eventHandlingThread = Some(new Thread(new Dequeue(), "HistoryEventHandlingThread"))
      eventHandlingThread.get.start()
    } else {
      logInfo("Timeline service is disabled")
    }
    if (registerListener()) {
      logInfo(s"History Service listening for events: $this")
    } else {
      logInfo(s"History Service is not listening for events: $this")
    }
  }

  /**
   * Check the service configuration to see if the timeline service is enabled
   * @return true if `YarnConfiguration.TIMELINE_SERVICE_ENABLED` is set.
   */
  def timelineServiceEnabled: Boolean = {
    YarnTimelineUtils.timelineServiceEnabled(config)
  }

  /**
   * Return a summary of the service state to help diagnose problems
   * during test runs, possibly even production
   * @return a summary of the current service state
   */
  override def toString(): String = {
    s"YarnHistoryService for application $applicationId attempt $attemptId;" +
    s" state=$serviceState;" +
    s" endpoint=${_timelineWebappAddress};" +
    s" bonded to ATS=$bondedToATS;" +
    s" listening=$listening;" +
    s" batchSize=$batchSize;" +
    s" flush count=$getFlushCount;" +
    s" total number queued=$eventsQueued, processed=$eventsProcessed;" +
    s" attempted entity posts=$postAttempts" +
    s" successful entity posts=$postSuccesses" +
    s" failed entity posts=$postFailures;" +
    s" events dropped=$eventsDropped;" +
    s" app start event received=$appStartEventProcessed;" +
    s" app end event received=$appEndEventProcessed;"
  }

  /**
   * Is the service listening to events from the spark context?
   * @return true if it has registered as a listener
   */
  def listening: Boolean = {
    listener.isDefined
  }

  /**
   * Is the service hooked up to an ATS server. This does not
   * check the validity of the link, only whether or not the service
   * has been set up to talk to ATS.
   * @return true if the service has a timeline client
   */
  def bondedToATS: Boolean = {
    _timelineClient.isDefined
  }

  /**
   * Set the YARN binding information. This is called during startup. It is private
   * to the package so that tests may update this data
   * @param appId YARN application ID
   * @param attemptId optional attempt ID
   */
  private[yarn] def bindToYarnApplication(appId: ApplicationId,
      attemptId: Option[ApplicationAttemptId]): Unit = {
    require(appId != null, "Null appId parameter")
    _applicationId = appId
    _attemptId = attemptId
  }

  /**
   * Set the "spark" application and attempt information -the information
   * provided in the start event. The attempt ID here may be `None`; even
   * if set it may only be unique amongst the attempts of this application.
   * That is: not unique enough to be used as the entity ID
   * @param appId application ID
   * @param attemptId attempt ID
   */
  private def setContextAppAndAttemptInfo(appId: Option[String],
      attemptId: Option[String]): Unit = {
    logDebug(s"Setting application ID to $appId; attempt ID to $attemptId")
    sparkApplicationId = appId
    sparkApplicationAttemptId = attemptId
  }

  /**
   * Add the listener if it is not disabled.
   * This is accessible in the same package purely for testing
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
   * Queue an action, or if the service's `stopped` flag
   * is set, discard it
   * @param event event to process
   * @return true if the event was queued
   */
  def enqueue(event: SparkListenerEvent): Boolean = {
    if (!stopped.get) {
      _eventsQueued.incrementAndGet
      logDebug(s"Enqueue $event")
      handleEvent(event)
      true
    } else {
      if (timelineServiceEnabled) {
        // if the timeline service was ever enabled, log the fact the event
        // is being discarded. Don't do this if it was not, as it will
        // only make the logs noisy.
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
    val oldState = enterState(StoppedState)
    if (oldState != StartedState) {
      // stopping from a different state
      logDebug(s"Ignoring stop() request from state $oldState")
      return
    }
    // if the queue is live
    if (!stopped.get) {

      if (appStartEventProcessed.get && !appEndEventProcessed.get) {
        // push out an application stop event if none has been received
        logDebug("Generating a SparkListenerApplicationEnd during service stop()")
        enqueue(SparkListenerApplicationEnd(now()))
      }

      // flush out the events
      asyncFlush()

      // stop operation
      postThreadActive.synchronized {
        // now await that marker flag
        if (postThreadActive.get) {
          logDebug(s"Stopping posting thread and waiting $shutdownWaitTime mS")
          stopped.set(true)
          eventHandlingThread.foreach(_.interrupt())
          postThreadActive.wait(shutdownWaitTime)
        } else {
          stopTimelineClient()
          logInfo(s"Stopped: $this")
        }
      }
    }
  }

  /**
   * Can an event be added?
   * Policy is only if the number of queued entities is below the limit, or the
   * event marks the end of the application.
   * @param isLifecycleEvent is this operation triggered by an application start/end?
   * @return true if the event can be added to the queue
   */
  private def canAddEvent(isLifecycleEvent: Boolean): Boolean = {
    _entityQueue.synchronized {
      _entityQueue.size() < _postQueueLimit  || isLifecycleEvent
    }
  }

  /**
   * Add another event to the pending event list.
   * Returns the size of the event list after the event was added
   * (thread safe).
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
   * Publish next set of pending events.
   *
   * Builds the next event to push on the entity Queue; resets
   * the current [[pendingEvents]] list and then notifies
   * any listener of the [[_entityQueue]] that there is new data.
   */
  private def publishPendingEvents(): Boolean = {
    // verify that there are events to publish
    val size = pendingEvents.synchronized {
      pendingEvents.size
    }
    if (size > 0 && applicationStartEvent.isDefined) {
      flushCount.incrementAndGet()
      val timelineEntity = createTimelineEntity(
        applicationId,
        attemptId,
        sparkApplicationId,
        sparkApplicationAttemptId,
        applicationName,
        userName,
        startTime,
        endTime,
        now())

      // copy in pending events and then clear the list
      pendingEvents.synchronized {
        pendingEvents.foreach(timelineEntity.addEvent)
        pendingEvents = new mutable.LinkedList[TimelineEvent]()
      }
      // queue the entity for posting
      preflightCheck(timelineEntity)
      _entityQueue.synchronized {
        _entityQueue.push(timelineEntity)
      }
      true
    } else {
      false
    }
  }

  /**
   * Perform any preflight checks.
   *
   * @param entity timeline entity to review.
   */
  private def preflightCheck(entity: TimelineEntity): Unit = {
    require(entity.getStartTime != null,
      s"No start time in ${describeEntity(entity)}")
  }

  /**
   * Post a single entity.
   *
   * Any network/connectivity errors will be caught and logged, and returned as the
   * exception field in the returned tuple.
   *
   * Any posting which generates a response will result in the timeline response being
   * returned. This response *may* contain errors; these are almost invariably going
   * to re-occur when resubmitted.
   *
   * @param entity entity to post
   * @return tuple with exactly one of the response or the exception set.
   */
  private def postOneEntity(entity: TimelineEntity):
      (Option[TimelinePutResponse], Option[Exception]) = {
    domainId.foreach {
      entity.setDomainId
    }
    val entityDescription = describeEntity(entity)
    logDebug(s"About to POST $entityDescription")
    _entityPostAttempts.incrementAndGet()
    try {
      val response = timelineClient.putEntities(entity)
      val errors = response.getErrors
      if (errors.isEmpty) {
        logDebug(s"entity successfully posted")
        _entityPostSuccesses.incrementAndGet()
      } else {
        // something went wrong.
        errors.asScala.foreach { err =>
          _entityPostFailures.incrementAndGet()
          logError(s"Failed to post $entityDescription\n:${describeError(err)}")
        }
      }
      // whatever the outcome, this request is not re-issued
      (Some(response), None)
    } catch {
      case e: ConnectException =>
        _entityPostFailures.incrementAndGet
        logWarning(s"Connection exception submitting $entityDescription", e)
        (None, Some(e))

      case e: RuntimeException =>
        // this is probably a retry timeout event; some Hadoop versions don't
        // rethrow the exception causing the problem, instead raising an RTE
        _entityPostFailures.incrementAndGet
        logWarning(s"Runtime exception submitting $entityDescription", e)
        // same policy as before: retry on these
        (None, Some(e))

      case e: Exception =>
        // something else has gone wrong.
        _entityPostFailures.incrementAndGet
        logWarning(s"Could not handle history entity: $entityDescription", e)
        (None, Some(e))
    }
  }

  /**
   * Spin and post until stopped.
   *
   * Algorithm.
   *
   * 1. The thread waits for events in the _entityQueue until stopped or interrupted
   * 1. Failures result in the entity being queued for resending, after a delay which grows
   * linearly on every retry.
   * 1. Successful posts reset the retry delay.
   * 1. If the process is interrupted, the loop continues with the `stopFlag` flag being checked.
   *
   * To stop this process then, first set the `stopFlag` flag, then interrupt the thread.
   *
   * @param stopFlag a flag to set to stop the loop.
   * @param retryInterval delay in milliseconds for the first retry delay; the delay increases
   *        by this value on every future failure. If zero, there is no delay, ever.
   */
  private def postEntities(stopFlag: AtomicBoolean, retryInterval: Long): Unit = {
    var lastAttemptFailed = false
    var currentRetryDelay = retryInterval
    while (!stopped.get) {
      try {
        // spin, rather than block, to check for the stopped bit -sometimes interrupts
        // appear to get swallowed
        val head = _entityQueue.pollFirst(1000, TimeUnit.MILLISECONDS)
        if (head != null) {
          val (_, ex) = postOneEntity(head)
          if (ex.isDefined && !stopped.get()) {
            // something went wrong and it wasn't being told to stop
            if (!lastAttemptFailed) {
              // avoid filling up logs with repeated failures
              logWarning(s"Exception submitting entity to ${_timelineWebappAddress}", ex.get)
            }
            // log failure and repost
            lastAttemptFailed = true
            currentRetryDelay += retryInterval
            _entityQueue.addFirst(head)
            if (currentRetryDelay > 0 ) {
              Thread.sleep(currentRetryDelay)
            }
          } else {
            // success
            lastAttemptFailed = false
            currentRetryDelay = retryInterval
          }
        }
      } catch {
        case ex: InterruptedException =>
          // interrupted; this will break out of IO/Sleep operations and
          // trigger a rescan of the stopped() event.
          // The ATS code implements some of its own retry logic, which
          // can catch interrupts and convert to other exceptions —this catch clause picks up
          // interrupts outside of that code.
          logDebug(s"Interrupted", ex)
        case other: Exception =>
          throw other
      }
    }
  }

  /**
   * Shutdown phase: continually post oustanding entities until the timeout has been exceeded.
   * The interval between failures is the retryInterval: there is no escalation, and if
   * is longer than the remaining time in the shutdown, the remaining time sets the limit.
   * @param timeout timeout in milliseconds.
   * @param retryInterval delay in milliseconds for every delay.
   */
  private def postEntitiesShutdownPhase(timeout: Long, retryInterval: Long): Unit = {
    val timeLimit = now() + timeout;
    logDebug(s"Entering shutdown post phase, time period = $timeout mS")
    while (now() < timeLimit && !_entityQueue.isEmpty) {

      try {
        val head = _entityQueue.poll(timeLimit - now(), TimeUnit.MILLISECONDS)
        if (head != null) {
          val (_, ex) = postOneEntity(head)
          if (ex.isDefined) {
            // failure, push back to try again
            _entityQueue.addFirst(head)
            if (retryInterval > 0) {
              Thread.sleep(Math.min(retryInterval, timeLimit - now()))
            }
          }
        }
      } catch {
        case ex: InterruptedException =>
          // ignoring, as this is shutdown time anyway
          logDebug("Ignoring Interrupt", ex)
        case ex: Exception =>
          throw ex
      }
    }
  }

  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event event. If null no event is queued, but the post-queue flush logic still applies
   */
  private def handleEvent(event: SparkListenerEvent): Unit = {
    var push = false
    var isLifecycleEvent = false;
    val timestamp = now()
    if (_eventsProcessed.incrementAndGet() % 1000 == 0) {
      logDebug(s"${_eventsProcessed} events are processed")
    }
    event match {
      case start: SparkListenerApplicationStart =>
        // we already have all information,
        // flush it for old one to switch to new one
        logDebug(s"Handling application start event: $event")
        if (!appStartEventProcessed.getAndSet(true)) {
          applicationStartEvent = Some(start)
          applicationName = start.appName
          if (applicationName == null || applicationName.isEmpty) {
            logWarning("Application does not have a name")
            applicationName = applicationId.toString
          }
          userName = start.sparkUser
          startTime = start.time
          if (startTime == 0) {
            startTime = timestamp
          }
          setContextAppAndAttemptInfo(start.appId, start.appAttemptId)
          logInfo(s"Application started: $event")
          isLifecycleEvent = true
          push = true
        } else {
          logWarning(s"More than one application start event received -ignoring: $start")
        }

      case end: SparkListenerApplicationEnd =>
        if (!appStartEventProcessed.get()) {
          logError(s"Received application end event without application start $event")
        } else if (!appEndEventProcessed.getAndSet(true)) {
          // we already have all information,
          // flush it for old one to switch to new one
          logInfo(s"Application end event: $event")
          applicationEndEvent = Some(end);
          // flush old entity
          endTime = if (end.time > 0) end.time else timestamp
          push = true
          isLifecycleEvent = true
        } else {
          logInfo(s"Discarding duplicate application end event $end")
        }

      case _ =>
    }

    val tlEvent = toTimelineEvent(event, timestamp)
    val eventCount = if (tlEvent.isDefined && canAddEvent(isLifecycleEvent)) {
       addPendingEvent(tlEvent.get)
    } else {
      // discarding the event
      logWarning("Discarding event")
      _eventsDropped.incrementAndGet()
      0
    }
    // set the push flag if the batch limit is reached
    push |= (eventCount >= _batchSize && appStartEventProcessed.get())

    logDebug(s"current event num: $eventCount")
    if (push) {
      logDebug("Push triggered")
      publishPendingEvents()
    }
  }

  /**
   * Return the current time in milliseconds
   * @return system time
   */
  private def now(): Long = {
    clock.getTimeMillis()
  }

  /**
   * Queue an asynchronous flush operation.
   * @return if the flush event was queued
   */
  def asyncFlush(): Boolean = {
    publishPendingEvents()
  }

  /**
   * Get the number of flush events that have taken place
   *
   * This includes flushes triggered by the event list being bigger the batch size,
   * but excludes flush operations triggered when the action processor thread
   * is stopped, or if the timeline service binding is disabled.
   *
   * @return count of processed flush events.
   */
  def getFlushCount: Long = {
    flushCount.get
  }

  /**
   * Get the URI of the timeline service webapp; null until service is started
   * @return a URI or null
   */
  def timelineWebappAddress: URI = _timelineWebappAddress

  /**
   * Dequeue thread: post events until told to stop.
   */
  private class Dequeue extends Runnable {

    override def run(): Unit = {
      postThreadActive.set(true)
      try {
        postEntities(stopped, retryInterval)
        // getting here means the `stop` flag is true
        postEntitiesShutdownPhase(shutdownWaitTime, retryInterval)
        logInfo(s"Stopping dequeue service, final queue size is ${_entityQueue.size}")
        stopTimelineClient()
      } catch {
        // handle unexpected case of an exception triggering thread exit.
        case ex: Exception =>
          logError("Deque thread exiting after exception raised", ex)
      } finally {
        postThreadActive synchronized {
          // declare that this thread is no longer active
          postThreadActive.set(false)
          // and notify all listeners of this fact
          postThreadActive.notifyAll()
        }
      }
    }
  }

}

/**
 * Constants and defaults for the history service
 */
private[spark] object YarnHistoryService {
  /**
   * Name of the entity type used to declare spark Applications
   */
  val SPARK_EVENT_ENTITY_TYPE = "spark_event_v01"

  /**
   * Domain ID
   */
  val DOMAIN_ID_PREFIX = "Spark_ATS_"


  /**
   * Time in millis to wait for shutdown on service stop
   */
  val DEFAULT_SHUTDOWN_WAIT_TIME = "30s"

  /**
   * Maximum time in to wait for event posting to complete when the service stops
   */
  val SHUTDOWN_WAIT_TIME = "spark.hadoop.yarn.timeline.shutdown.waittime"

  /**
   * Option to declare that the history service should register as a spark context
   * listener. (default: true; this option is here for testing)
   * <p>
   * This is a spark option, though its use of name will cause it to propagate down to the Hadoop
   * Configuration.
   */
  val REGISTER_LISTENER = "spark.hadoop.yarn.timeline.listen"

  /**
   * Option for the size of the batch for timeline uploads. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val BATCH_SIZE = "spark.hadoop.yarn.timeline.batchSize"

  /**
   * The default size of a batch
   */
  val DEFAULT_BATCH_SIZE = 10

  /**
   * Name of a domain for the timeline
   */
  val TIMELINE_DOMAIN = "spark.hadoop.yarn.timeline.domain"

  /**
   * Limit on number of posts in the outbound queue -when exceeded
   * new events will be dropped
   */
  val POST_QUEUE_LIMIT = "spark.hadoop.yarn.timeline.post.queue.limit"

    /**
   * The default limit of the post queue
   */
  val DEFAULT_POST_QUEUE_LIMIT = 100

  /**
   * Interval in milliseconds between POST retries. Every
   * failure causes the interval to increase by this value.
   */
  val POST_RETRY_INTERVAL = "spark.hadoop.yarn.timeline.post.retry.interval"

  /**
   * The default retry interval in millis
   */
  val DEFAULT_POST_RETRY_INTERVAL = "1000ms"

  /**
   * Primary key used for events
   */
  val PRIMARY_KEY = "spark_application_entity"

  /** Entity `OTHER_INFO` field: start time */
  val FIELD_START_TIME = "startTime"

  /** Entity `OTHER_INFO` field: last updated time */
  val FIELD_LAST_UPDATED = "lastUpdated"

  /** Entity `OTHER_INFO` field: end time. Not present if the app is running */
  val FIELD_END_TIME = "endTime"

  /** Entity `OTHER_INFO` field: application name from context */
  val FIELD_APP_NAME = "appName"

  /** Entity `OTHER_INFO` field: user */
  val FIELD_APP_USER = "appUser"

  /** Entity `OTHER_INFO` field: YARN application ID*/
  val FIELD_APPLICATION_ID = "applicationId"

  /** Entity `OTHER_INFO` field: attempt ID from spark start event*/
  val FIELD_ATTEMPT_ID = "attemptId"

  /** Entity filter field: to search for entities that have started*/
  val FILTER_APP_START = "startApp"

  /** value of the `startApp` filter field */
  val FILTER_APP_START_VALUE = "SparkListenerApplicationStart"

  /** Entity filter field: to search for entities that have ended */
  val FILTER_APP_END = "endApp"

  /** value of the `endApp`filter field */
  val FILTER_APP_END_VALUE = "SparkListenerApplicationEnd"

  /**
   * ID used in yarn-client attempts only
   */
  val CLIENT_BACKEND_ATTEMPT_ID = "1"

  /**
   * The classname of the history service to instantiate in the YARN AM
   */
  val CLASSNAME = "org.apache.spark.deploy.history.yarn.YarnHistoryService"

  val CreatedState = 0
  val StartedState = 1
  val StoppedState = 2
}
