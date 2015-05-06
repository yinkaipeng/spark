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
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable.LinkedList

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.service.{AbstractService, Service}
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.timeline.{TimelineDomain, TimelineEntity, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException

import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.YarnExtensionService
import org.apache.spark.{Logging, SparkContext}

import scala.util.control.NonFatal

/**
 * Implements a Hadoop service with the init/start logic replaced by that
 * of the YarnService.
 * <p>
 * As [[AbstractService]] implements `close()`, routing
 * to its `stop` method, calling `close()` is sufficient
 * to stop the service instance.
 * <p>
 * However, when registered to receive spark events, the service will continue to
 * receive them until the spark context is stopped. Events received when this service
 * is in a `STOPPED` state will be discarded.
 */
private[spark] class YarnHistoryService  extends AbstractService("History Service")
  with YarnExtensionService with Logging {

  private var sparkContext: SparkContext = _
  private var appId: ApplicationId = _
  // Hadoop timeline client
  private var timelineClient: Option[TimelineClient] = None
  private var listener: YarnEventListener = _
  private var appName: String = null
  private var userName: String = null
  private var startTime: Long = _


  // number of events to batch up before posting
  private var batchSize: Int = YarnHistoryService.DEFAULT_BATCH_SIZE

  // queue of actions 
  private val actionQueue = new LinkedBlockingQueue[QueuedAction]
  // cache layer to handle timeline client failure.
  private var entityList = new LinkedList[TimelineEntity]
  private var curEntity: Option[TimelineEntity] = None
  // Do we have enough information filled for the entity
  @volatile
  private var appStartEventProcessed = false
  @volatile
  private var appEndEventProcessed = false
  // How many event we saved
  private var curEventNum = 0
  // counter of events processed -that is have been through handleEvent()
  private val eventsProcessed: AtomicInteger = new AtomicInteger(0)
  // counter of events queued. if less than the
  private val eventsQueued: AtomicInteger = new AtomicInteger(0)
  // how many event postings failed?
  private val eventPostFailures: AtomicInteger = new AtomicInteger(0)
  // how many flushes have taken place
  private val flushCount = new AtomicInteger(0)
  private var eventHandlingThread: Thread = null
  private val stopped: AtomicBoolean = new AtomicBoolean(true)
  /*
   boolean to track whether a thread is active or not, for tests to
   monitor and see if the thread has completed.
   */
  private val postThreadActive: AtomicBoolean = new AtomicBoolean(false)
  private final val lock: AnyRef = new AnyRef
  private var maxTimeToWaitOnShutdown: Long = YarnHistoryService.SHUTDOWN_WAIT_TIME
  private var domainId: String = null
  // URI to timeline web application -valid after serviceStart()
  private var timelineWebappAddress: URI = _

  /**
   * Create a timeline client and start it. This does not update the
   * `timelineClient` field, though it does verify that the field
   * is unset.
   *
   * The method is private to the package so that tests can access it, which
   * some of the mock tests do to override the timeline client creation.
   * @return the timeline client
   */
  private [yarn] def createTimelineClient(): TimelineClient = {
    require(timelineClient == None, "timeline client already set")
    YarnHistoryService.createTimelineClient(sparkContext)
  }

  /**
   * Get the timeline client.
   * @return the client
   * @throws Exception if the timeline client is not currently running
   */
  def getTimelineClient: TimelineClient = {
    timelineClient.getOrElse(throw new Exception("Timeline client not running"))
  }

  /**
   * Get the total number of processed events
   * @return counter of events processed
   */
  def getEventsProcessed: Int = {
    eventsProcessed.get()
  }

  /**
   * Get the total number of events queued
   * @return the total event count
   */
  def getEventsQueued: Int = {
    eventsQueued.get()
  }

  /**
   * Get the current size of the queue
   * @return the current queue length
   */
  def getQueueSize: Int = {
    actionQueue.size()
  }

  def getBatchSize: Int = {
    batchSize
  }

  /**
   * Get the total number of failed posts events
   * @return counter of timeline post operations which failed
   */
  def getEventPostFailures: Int = {
    eventPostFailures.get()
  }

  /**
   * is the asynchronous posting thread active?
   * @return true if the post thread has started; false if it has not yet/ever started, or
   *         if it has finished.
   */
  def isPostThreadActive: Boolean = {
    postThreadActive.get()
  }

  /**
   * Reset the timeline client
   * <p>
   * 1. Stop the timeline client service if running.
   * 2. set the `timelineClient` field to `None`
   */
  def stopTimelineClient(): Unit = {
    stopOptionalService(timelineClient)
    timelineClient = None
  }

  /**
   * Create the timeline domain
   * @return a domain string or null
   */
  private def createTimelineDomain(): String = {
    val sparkConf = sparkContext.getConf
    val aclsOn = sparkConf.getOption("spark.acls.enable").getOrElse(
      sparkConf.get("spark.ui.acls.enable", "false")).toBoolean
    if (!aclsOn) {
      return null
    }
    val predefDomain = sparkConf.getOption("spark.ui.domain")
    if (predefDomain.isDefined) {
      return predefDomain.get
    }
    val current = UserGroupInformation.getCurrentUser.getShortUserName
    val adminAcls  = stringToSet(sparkConf.get("spark.admin.acls", ""))
    val viewAcls = stringToSet(sparkConf.get("spark.ui.view.acls", ""))
    val modifyAcls = stringToSet(sparkConf.get("spark.modify.acls", ""))

    val readers = (adminAcls ++ modifyAcls ++ viewAcls).foldLeft(current)(_ + " " + _)
    val writers = (adminAcls ++ modifyAcls).foldLeft(current)(_ + " " + _)
    var tmpId = YarnHistoryService.DOMAIN_ID_PREFIX + appId
    logInfo("Creating domain " + tmpId + " with  readers: "
      + readers + " and writers:" + writers)
    val timelineDomain = new TimelineDomain()
    timelineDomain.setId(tmpId)

    timelineDomain.setReaders(readers)
    timelineDomain.setWriters(writers)
    try {
      getTimelineClient.putDomain(timelineDomain)
    } catch {
      // TODO IOException
      case e: YarnException => {
        logError("cannot create the domain")
        // fallback to default
        tmpId = null
      }
    }
    tmpId
  }

  /**
   * start the service, calling the [[Service]] init and start events in the
   * correct order
   * @param context spark context
   * @param id YARN application ID
   * @return true if the service is hooked up to the timeline service; that is: it is live
   */
  def start(context: SparkContext, id: ApplicationId): Boolean = {
    require(context != null, "Null context parameter")
    require(id != null, "Null id parameter")

    val yarnConf = new YarnConfiguration(context.hadoopConfiguration)
    // the init() operation checks the state machine & prevents invocation out of sequence
    init(yarnConf)
    sparkContext = context
    appId = id
    batchSize = sparkContext.conf.getInt(YarnHistoryService.BATCH_SIZE, batchSize)

    start()
    if (timelineServiceEnabled) {
      true
    } else {
      logInfo("Yarn timeline service not available, disabling client.")
      false
    }
  }

  /**
   * Service start.
   *
   * If the timeline client is enabled:
   * Create the timeline client, the timeline domain, and the vent handling thread.
   *
   * Irrespective of the timeline enabled flag, the service will attempt to register
   * as a listener for events. They will merely be discarded.
   */
  override protected def serviceStart {
    require(sparkContext != null, "No spark context set")
    super.serviceStart()
    val conf: Configuration = getConfig
    if (timelineServiceEnabled) {
      timelineWebappAddress = rootTimelineUri(conf)
      timelineClient = Some(createTimelineClient())
      domainId = createTimelineDomain
      eventHandlingThread = new Thread(new Dequeue(), "HistoryEventHandlingThread")
      eventHandlingThread.start
    }
    // irrespective of state, hook up to the listener
    registerListener
    logInfo(s"$this")
  }

  /**
   * Check the service configuration to see if the timeline service is enabled
   * @return true if `YarnConfiguration.TIMELINE_SERVICE_ENABLED`
   *         is set.
   */
  def timelineServiceEnabled: Boolean = {
    getConfig.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)
  }

  /**
   * Return a summary of the service state to help diagnose problems
   * during test runs, possibly even production
   * @return a summary of the current service state
   */
  override def toString: String = {
    super.toString +
        s" endpoint=$timelineWebappAddress;" +
        s" bonded to ATS=$bondedToATS;" +
        s" listening=$listening;" +
        s" batchSize=$batchSize;" +
        s" flush count=$getFlushCount;" +
        s" current queue size=$getQueueSize;" +
        s" total number queued=$getEventsQueued, processed=$getEventsProcessed;" +
        s" post failures=$getEventPostFailures;"
  }

  /**
   * Is the service listening to events from the spark context?
   * @return true if it has registered as a listener
   */
  def listening: Boolean = {
    listener != null;
  }

  /**
   * Is the service hooked up to an ATS server. This does not
   * check the validity of the link, only whether or not the service
   * has been set up to talk to ATS.
   * @return true if the service has a timeline client
   */
  def bondedToATS: Boolean = {
    timelineClient != None;
  }

  /**
   * Add the listener if it is not disabled.
   * This is accessible in the same package purely for testing
   * @return true if the register was enabled
   */
  private [yarn] def registerListener: Boolean = {
    assert(sparkContext != null, "Null context")
    if (sparkContext.conf.getBoolean(YarnHistoryService.REGISTER_LISTENER, true)) {
      logDebug("Registering listener to spark context")
      listener = new YarnEventListener(sparkContext, this)
      sparkContext.listenerBus.addListener(listener)
      true
    } else {
      false
    }
  }

  /**
   * Queue an action, or if the service's `stopped` flag
   * is set, discard it
   * @param action action to process
   * @return true if the event was queued
   */
  def enqueue(action: QueuedAction): Boolean = {
    if (!stopped.get()) {
      innerEnqueue(action)
      true
    } else {
      if (timelineServiceEnabled) {
        // if the timeline service was ever enabled, log the fact the event
        // is being discarded. Don't do this if it was not, as it will
        // only make the logs noisy.
        logInfo(s"History service stopped; ignoring queued event : ${action}")
      }
      false
    }
  }

  /**
   * Inner operation to queue the event. This does not check for service state
   * @param event
   */
  private def innerEnqueue(event: QueuedAction) = {
    eventsQueued.incrementAndGet()
    logDebug(s"Enqueue ${event}")
    actionQueue.add(event)
  }

  /**
   * Stop the service; this triggers flushing the queue and, if not already processed,
   * a pushing out of an application end event.
   *
   * This operation will block for up to [[maxTimeToWaitOnShutdown]] milliseconds
   * to await the asynchronous action queue completing.
   */
  override protected def serviceStop {
    // if the queue is live
    if (!stopped.get) {

      if (!appEndEventProcessed) {
        // push out an application stop event if none has been received
        logDebug("Generating a SparkListenerApplicationEnd during  service stop()")
        val current = now()
        innerEnqueue(new HandleSparkEvent(SparkListenerApplicationEnd(current), current))
      }

      // flush
      logInfo(s"Shutting down: pushing out ${actionQueue.size } events")
      innerEnqueue(FlushTimelineEvents())

      // stop operation
      postThreadActive synchronized {
        innerEnqueue(StopQueue())
        // now await that marker flag
        postThreadActive.wait(maxTimeToWaitOnShutdown)
      }

      if (!actionQueue.isEmpty) {
        // likely cause is ATS not responding.
        // interrupt the thread, albeit at the risk of app stop events
        // being lost. There's not much else that can be done if
        // ATS is being unresponsive
        logWarning(s"Did not finish flushing actionQueue before " +
          s"stopping ATSService, eventQueueBacklog= ${actionQueue.size}")
        if (eventHandlingThread != null) {
          eventHandlingThread.interrupt
        }
      }
      stopTimelineClient
      logInfo(s"Stopped: $this")
    }
  }

  /**
   * Get the current entity, creating one on demand
   * @return the current entity
   */
  private def getCurrentEntity = {
    curEntity.getOrElse {
      val entity: TimelineEntity = new TimelineEntity
      curEventNum = 0
      entity.setEntityType(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
      entity.setEntityId(appId.toString)
      if (appStartEventProcessed) {
        entity.addPrimaryFilter(YarnHistoryService.FIELD_APP_NAME, appName)
        entity.addPrimaryFilter(YarnHistoryService.FIELD_APP_USER, userName)
        entity.addOtherInfo(YarnHistoryService.FIELD_APP_NAME, appName)
        entity.addOtherInfo(YarnHistoryService.FIELD_APP_USER, userName)
      }
      entity.setStartTime(startTime)
      curEntity = Some(entity)
      logDebug(s"Demand creation of new entity ${describeEntity(entity)}")
      entity
    }
  }

  /**
   * Perform any preflight checks
   * @param entity
   */
  def preflightCheck(entity: TimelineEntity): Unit = {
    require(entity.getStartTime != null,
             s"No start time in ${describeEntity(entity)}")
  }

  /**
   * If there is any available entity to be sent, push to timeline server
   */
  private def flushEntity(): Unit = {
    if (entityList.nonEmpty) {
      val count = flushCount.incrementAndGet()
      logDebug(s"flushEntity #$count: list size ${entityList.size}")
      var client = getTimelineClient
      entityList = entityList.filter {
        en => {
          if (en == null) {
            false
          } else {
            if (domainId != null) {
              en.setDomainId(domainId);
            }
            val entityDescription = describeEntity(en)
            logDebug(s"About to put $entityDescription")
            try { 
              val response: TimelinePutResponse = client.putEntities(en)

              val errors = response.getErrors
              if (!response.getErrors.isEmpty) {
                val err: TimelinePutResponse.TimelinePutError = errors.get(0)
                eventPostFailures.addAndGet(errors.size())
                if (err.getErrorCode != 0) {
                  logError(s"Failed to post ${entityDescription}\n:${describeError(err)}")
                }
              } else {
                // successful submission
                logDebug(s"entity successfully posted: $entityDescription")
              }
              // whatever the outcome, this request is not re-issued
              false
            } catch {
              case e: ConnectException => {
                eventPostFailures.incrementAndGet()
                logWarning(s"Connection exception submitting $entityDescription\n$e", e)
                true
              }
              case e: RuntimeException => {
                // this is probably a retry timeout event; Hadoop <= 2.7 doesn't
                // rethrow the exception causing the problem
                eventPostFailures.incrementAndGet()
                logWarning(s"Runtime exception submitting $entityDescription\n$e", e)
                // same policy as before: retry on these
                true
              }
              case e: Exception => {
                // something else has gone wrong.
                eventPostFailures.incrementAndGet()
                logWarning(s"Could not handle history entity: $entityDescription\n$e", e)
                false
              }
            }
          }
        }
      }
      logDebug(s"after pushEntities: ${entityList.size}")
    }
  }

  /**
   * Process the action placed in the queue
   * @param action action
   * @return true if the queue processor must now exit
   */
  private def processAction(action: QueuedAction): Boolean = {
    action match {
      case StopQueue() =>
        logDebug("Stop queue action received")
        true

      case event: HandleSparkEvent =>
        try {
          handleEvent(event)
          false
        } catch {
          case NonFatal(e) => false
        }

      case FlushTimelineEvents() =>
        logDebug("Flush queue action received")
        flushCurrentEventsToATS
        false
    }
  }
  
  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event event. If null no event is queued, but the post-queue flush logic
   *              still applies
   * @return true if the event should stop the service.
   */
  private def handleEvent(event: HandleSparkEvent): Unit = {
    var push = false
    // if we receive a new appStart event, we always push
    // not much contention here, only happens when service is stopped
    lock synchronized {
      logDebug(s"Processing event $event")
      if (eventsProcessed.get() % 1000 == 0) {
        logDebug(s"${eventsProcessed} events are processed")
      }
      val sparkEvent = event.sparkEvent
      sparkEvent match {
        case start: SparkListenerApplicationStart =>
          // we already have all information,
          // flush it for old one to switch to new one
          logDebug(s"Handling application start event: $event")
          appName = start.appName
          userName = start.sparkUser
          startTime = start.time
          if (startTime == 0) {
            startTime = now()
          }
          // flush old entity
          resetCurrentEntity()
          // force create the new one
          val en = getCurrentEntity
          en.addPrimaryFilter(YarnHistoryService.FILTER_APP_START,
            YarnHistoryService.FILTER_APP_START_VALUE)
          push = true
          appStartEventProcessed = true
          appEndEventProcessed = false

        case end: SparkListenerApplicationEnd =>
          if (!appEndEventProcessed) {
            // we already have all information,
            // flush it for old one to switch to new one
            logDebug(s"Handling application end event: $event")
            if (!appStartEventProcessed) {
              logWarning(s"Handling application end event without application start $event")
            }
            // flush old entity
            val endtime = if (end.time > 0) end.time else now()
            if (startTime == 0) {
              // special case: an app end is received without any active start
              // event sent yet
              logInfo(s"start time is 0; fixing up to match end time")
              startTime = endtime
            }
            // reset the current entity
            resetCurrentEntity()
            // then trigger its recreation
            val en = getCurrentEntity
            en.addPrimaryFilter(YarnHistoryService.FILTER_APP_END,
                                 YarnHistoryService.FILTER_APP_END_VALUE)
            en.addOtherInfo(YarnHistoryService.FIELD_START_TIME,
                             startTime)
            en.addOtherInfo(YarnHistoryService.FIELD_END_TIME, end.time)
            appEndEventProcessed = true
            appStartEventProcessed = false
            push = true
          }
        case _ =>
          logDebug(s"Handling event: $event")
          val currentEntity: TimelineEntity = getCurrentEntity
          if (currentEntity.getStartTime == null) {
            logInfo(s"Adding event $event to a timeline entity with no start time;" +
                s" using current time as start time")
            currentEntity.setStartTime(now())
          }

      }

      val tlEvent = YarnTimelineUtils.toTimelineEvent(event)
      getCurrentEntity.addEvent(tlEvent)
      curEventNum += 1
      // set the push flag if the batch limit is reached
      push |= curEventNum == batchSize
      // after the processing, increment the counter
      // this is done at the tail to count real number of processed events, not count received
      incEventsProcessed
    } // end of synchronized clause

    logDebug(s"current event num: $curEventNum")
    if (push) {
      logDebug("Push triggered")
      flushCurrentEventsToATS
    }
  }

  /**
   * Flush the current event set to ATS
   */
  private def flushCurrentEventsToATS: Unit = {
    lock synchronized {
      resetCurrentEntity()
      flushEntity()
    }
  }

  /**
   * Return a time value
   * @return a time
   */
  private def now(): Long = {
    System.currentTimeMillis()
  }

  /*
   * increment the counter of events processed
   */
  private def incEventsProcessed: Unit = {
    val count = eventsProcessed.incrementAndGet()
    logDebug(s"Event processed count at $count")
  }

  /**
   * Reset the current entity by propagating its values to the entity list
   */
  protected def resetCurrentEntity(): Unit = {
    curEntity.foreach(entityList :+= _)
    curEntity = None
    curEventNum = 0
  }

  /**
   * Queue an asynchronous flush operation.
   * @return if the flush event was queued
   */
  def asyncFlush(): Boolean = {
    enqueue(FlushTimelineEvents())
  }

  /**
   * Get the number of flush events that have taken place
   *
   * This includes flushes triggered by the event list being > the batch size,
   * but excludes flush operations triggered when the action processor thread
   * is stopped, or if the timeline service binding is disabled.
   *
   * @return count of processed flush events.
   */
  def getFlushCount(): Int = {
    flushCount.get()
  }

  /**
   * Get the URI of the timeline service webapp; null until service is started
   * @return a URI or null
   */
  def getTimelineServiceAddress(): URI = {
    timelineWebappAddress
  }

  /**
   * Dequeue thread
   */
  private class Dequeue extends Runnable {
    def run {
      postThreadActive.set(true)
      try {
        var shouldStop = false
        logInfo(s"Starting Dequeue service for AppId $appId")
        // declare that the processing is started
        stopped.set(false)
        while (!stopped.get) {
          try {
            val action = actionQueue.take
            shouldStop = processAction(action)
            if (shouldStop) {
              log.info("Event handler thread stopping the service")
              stopped.set(true)
            }
          } catch {
            case e: InterruptedException => {
              logWarning("EventQueue interrupted")
              stopped.set(true)
            }
            case e: Exception => {
              logWarning(s"Exception in dequeue thread $e", e)
              // if this happened while the service was stopped -stop
              // immediately
              stopped.set(isInState(Service.STATE.STOPPED))
            }
          }
        }
        logInfo(s"Stopping dequeue service, final queue size is ${actionQueue.size}")
        stopTimelineClient
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
 * Any event on the History timeline
 */
private[yarn] sealed abstract class QueuedAction

/**
 * Flush the timeline event list
 */
private[yarn] case class FlushTimelineEvents() extends QueuedAction

/**
 * Stop the queue entirely. This does not force a flush: that must
 * be separate
 */
private[yarn] case class StopQueue() extends QueuedAction

/**
 * A spark event has been received: handle it
 * @param sparkEvent spark event
 * @param time time of event being received
 */
private[yarn] case class HandleSparkEvent(sparkEvent: SparkListenerEvent,
    time: Long)
    extends QueuedAction {
  override def toString: String = {
    s"[${time }]: ${sparkEvent}"
  }
}


private[spark] object YarnHistoryService {
  /**
   * Name of the entity type used to declare spark Applications
   */
  val SPARK_EVENT_ENTITY_TYPE = "spark_event_v01"

  /**
   * Doman ID
   */
  val DOMAIN_ID_PREFIX = "Spark_ATS_"

  /**
   * Time in millis to wait for shutdown on service stop
   */
  val SHUTDOWN_WAIT_TIME = 10000L

  /**
   * Option to declare that the history service should register as a spark context
   * listener. (default: true; this option is here for testing)
   * <p>
   * This is a spark option, though its use of name will cause it to propagate down to the Hadoop
   * Configuration.
   */
  val REGISTER_LISTENER = "spark.hadoop.yarn.atshistory.listen"

  /**
   * Option for the size of the batch for timeline uploads. Bigger: less chatty.
   * Smaller: history more responsive.
   */
  val BATCH_SIZE = "spark.hadoop.yarn.timeline.batchSize"

  /**
   * The default size of a batch
   */
  val DEFAULT_BATCH_SIZE = 3

  /**
   * Primary key used for events
   */
  val PRIMARY_KEY = "spark_application_entity"

  val FIELD_START_TIME = "startTime"
  val FIELD_END_TIME = "endTime"
  val FIELD_APP_NAME = "appName"
  val FIELD_APP_USER = "appUser"

  val FILTER_APP_START = "startApp"
  val FILTER_APP_START_VALUE = "SparkListenerApplicationStart"
  val FILTER_APP_END = "endApp"
  val FILTER_APP_END_VALUE = "SparkListenerApplicationEnd"

  /**
   * The classname of the history service to instantiate in the YARN AM
   */
  val CLASSNAME = "org.apache.spark.deploy.history.yarn.YarnHistoryService"

  /**
   * Create and start a timeline client, using the configuration context to
   * set up the binding
   * @param sparkContext spark context
   * @return the started instance
   */
  def createTimelineClient(sparkContext: SparkContext): TimelineClient = {
    val client = TimelineClient.createTimelineClient()
    client.init(sparkContext.hadoopConfiguration)
    client.start
    client
  }
}
