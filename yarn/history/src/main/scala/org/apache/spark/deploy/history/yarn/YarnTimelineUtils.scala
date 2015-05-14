/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.history.yarn


import java.io.IOException
import java.net.{URI, URL}
import java.util
import java.util.{ArrayList => JArrayList, Collection => JCollection, Date, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.service.Service
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.json4s.JsonAST.JObject
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.Logging
import org.apache.spark.deploy.history.ApplicationHistoryInfo
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.{JsonProtocol, Utils}

private[spark] object YarnTimelineUtils extends Logging {

  /**
   * Converts a Java object to its equivalent json4s representation.
   */
  def toJValue(obj: Object): JValue = obj match {
    case str: String => JString(str)
    case dbl: java.lang.Double => JDouble(dbl)
    case dec: java.math.BigDecimal => JDecimal(dec)
    case int: java.lang.Integer => JInt(BigInt(int))
    case long: java.lang.Long => JInt(BigInt(long))
    case bool: java.lang.Boolean => JBool(bool)
    case map: JMap[_, _] =>
      val jmap = map.asInstanceOf[JMap[String, Object]]
      JObject(jmap.entrySet().map { e => (e.getKey() -> toJValue(e.getValue())) }.toList)
    case array: JCollection[_] =>
      JArray(array.asInstanceOf[JCollection[Object]].map(o => toJValue(o)).toList)
    case null => JNothing
  }

  /**
   * Converts a JValue into its Java equivalent.
   */
  def toJavaObject(v: JValue): Object = v match {
    case JNothing => null
    case JNull => null
    case JString(s) => s
    case JDouble(num) => java.lang.Double.valueOf(num)
    case JDecimal(num) => num.bigDecimal
    case JInt(num) => java.lang.Long.valueOf(num.longValue)
    case JBool(value) => java.lang.Boolean.valueOf(value)
    case obj: JObject => toJavaMap(obj)
    case JArray(vals) => {
      val list = new JArrayList[Object]()
      vals.foreach(x => list.add(toJavaObject(x)))
      list
    }
  }

  /**
   * Converts a json4s list of fields into a Java Map suitable for serialization by Jackson,
   * which is used by the ATS client library.
   */
  def toJavaMap(sourceObj: JObject): JHashMap[String, Object] = {
    val map = new JHashMap[String, Object]()
    sourceObj.obj.foreach(f => map.put(f._1, toJavaObject(f._2)))
    map
  }

  /**
   * Convert a timeline event to a spark one. Includes
   * some basic checks for validity of event payload.
   * @param event timeline event
   * @return an unmarshalled event
   */
  def toSparkEvent(event: TimelineEvent): SparkListenerEvent = {
    val info = event.getEventInfo()
    if (info == null) {
      throw new IOException(E_NO_EVENTINFO)
    }
    if (info.size() == 0) {
      throw new IOException(E_EMPTY_EVENTINFO)
    }
    val payload = toJValue(info)
    val json = compact(render(payload))
    logDebug(s"payload is ${json}")
    val eventField = payload \ "Event"
    if (eventField == JNothing) {
      throw new IOException("No \"Event\" entry in $json")
    }

    // now the real unmarshalling
    try {
      JsonProtocol.sparkEventFromJson(payload)
    } catch {
      // failure in the marshalling; include payload in the message
      case ex: MappingException => {
        logError(s"$ex while rendering $json", ex)
        throw ex
      }
    }
  }

  val E_NO_EVENTINFO= "No \"eventinfo\" entry"
  val E_EMPTY_EVENTINFO = "Empty \"eventinfo\" entry"


  def toTimelineEvent(event: HandleSparkEvent): TimelineEvent = {
    val tlEvent = new TimelineEvent()
    tlEvent.setEventType(Utils.getFormattedClassName(event.sparkEvent).toString)
    tlEvent.setTimestamp(event.time)
    val kvMap = new JHashMap[String, Object]()
    val json = JsonProtocol.sparkEventToJson(event.sparkEvent)
    val jObject = json.asInstanceOf[JObject]
    // the timeline event wants a map of java objects for Jackson to serialize
    val hashMap = toJavaMap(jObject)
    tlEvent.setEventInfo(hashMap)
    tlEvent
  }


  def describeEvent(event: TimelineEvent): String = {
    var sparkEventDetails = ""
    try {
      sparkEventDetails = toSparkEvent(event).toString
    } catch {
      case _: Exception =>
        sparkEventDetails = "(cannot convert event details to spark exception)"
    }
    s"Timeline Event ${event.getEventType()} @ ${new Date(event.getTimestamp())}" +
        s" \n  ${sparkEventDetails}"
  }

  def eventDetails(entity: TimelineEntity): String = {
    val events: util.List[TimelineEvent] = entity.getEvents
    if (events != null) {
      events.foldLeft("") {
        (s, evt) => ("\n" + describeEvent(evt))
      }
    } else {
      ""
    }
  }

  def describeEntity(entity: TimelineEntity): String = {
    val events: util.List[TimelineEvent] = entity.getEvents
    val eventDetails = if (events != null) {
      s"contains ${events.size()} event(s)"
    } else {
      "contains no events"
    }

    s"Timeline Entity ${entity.getEntityType}/${entity.getEntityId}@${entity.getDomainId}} " +
        (if (entity.getStartTime() != null)
           s"${new Date(entity.getStartTime())} " else "no start time ") +
        eventDetails
  }

  /**
   * A verbose description of the entity which contains event details and info about
   * primary/secondary keys
   * @param entity
   * @return
   */
  def describeEntityVerbose(entity: TimelineEntity): String = {
    val header = describeEntity(entity)
    val primaryFilters = entity.getPrimaryFilters.toMap
    var filterElements = ""
    for ((k, v) <- primaryFilters) {
      filterElements = filterElements +
        " filter " + k + ": [ " + v.foldLeft("")((s, o) => (s + o.toString + " ")) +"]\n"
    }

    val events = eventDetails(entity)
    header + "\n" + filterElements + events
  }

  /**
   * Stop any optional service
   * @param svc service
   */
  def stopOptionalService(svc: Option[Service]): Unit = {
    svc match {
      case Some(client) => client.stop()
      case None =>
    }
  }

  /**
   * Split a comma separated String, filter out any empty items, and return a Set of strings
   */
  def stringToSet(list: String): Set[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty).toSet
  }

  /**
   * The path for the V1 ATS REST API
   */
  val TIMELINE_REST_PATH = s"/ws/v1/timeline/"

  /**
   * Build the URI to the base of the timeline web application
   * from the Hadoop context.
   * <p>
   * Raises an exception if the address cannot be determined.
   * <p>
   * Does not perform any checks as to whether or note the timeline
   * service is enabled
   * @param conf configuration
   * @return the URI to the timeline service.
   */
  def getTimelineEndpoint(conf: Configuration): URI = {
    val isHttps = YarnConfiguration.useHttps(conf)
    val address = if (isHttps) {
      conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS)
    } else {
      conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS)
    }
    val protocol = if (isHttps) "https://" else "http://"
    require(address != null, s"No timeline service defined")
    URI.create(s"$protocol${address}$TIMELINE_REST_PATH")
  }


  /**
   * Create a URI to the history service. This uses the entity type of
   * <code>YarnHistoryService.ENTITY_TYPE</code> to
   * @param conf
   * @return
   */
  def timelineWebappUri(conf: Configuration): URI = {
    timelineWebappUri(conf, YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
  }


  /**
   * Get the URI of a path under the timeline web UI
   * @param conf configuration
   * @param subpath path under the root web UI
   * @return a URI
   */
  def timelineWebappUri(conf: Configuration, subpath: String): URI = {
    val base = getTimelineEndpoint(conf)
    new URL(base.toURL, subpath).toURI
  }

  /**
   * Check the service configuration to see if the timeline service is enabled
   * @return true if `YarnConfiguration.TIMELINE_SERVICE_ENABLED`
   *         is set.
   */
  def timelineServiceEnabled(conf: Configuration): Boolean = {
    conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                    YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)
  }

  /**
   * Get the URI to an application under the timeline
   * (this requires the applicationID to have been used to
   * publish entities there)
   * @param timelineUri timeline URI
   * @param appId App ID (really, the entityId used to publish)
   * @return the path
   */
  def applicationURI(timelineUri: URI, appId: String): URI = {
    require(appId != null && !appId.isEmpty, "No application ID")
    require(!appId.contains("/"), s"Illegal character '/' in $appId")
    timelineUri.resolve(s"${timelineUri.getPath()}/$appId")
  }

  /**
   * Map an error code to a string. For known codes, it returns
   * a description; for others it just returns the error code.
   * 
   * @param code error code
   * @return a string description for error messages
   */
  def timelineErrorCodeToString(code: Int): String ={
    code match {
      case 1 => "No start time"
      case 2 => "IO Exception"
      case 3 => "System Filter Conflict"
      case 4 => "Access Denied"
      case 5 => "No Domain"
      case 6 => "Forbidden Relation"
      case other: Int => s"Error code $other"
    }
  }

  /**
   * Convert a timeline error response to a meaningful string
   * @param error error
   * @return text for diagnostics
   */
  def describeError(error: TimelinePutError): String = {
    s"Entity ID=${error.getEntityId()}; Entity type=${error.getEntityType}" +
    s" Error code ${error.getErrorCode}" +
    s": ${timelineErrorCodeToString(error.getErrorCode)}"
  }

  /**
   * Describe a put response by enumerating and describing all errors
   * (if present. A null errors element is handles robustly)
   * @param response response to describe
   * @return text for diagnostics
   */
  def describePutResponse(response: TimelinePutResponse) : String = {
    val responseErrs = response.getErrors
    if (responseErrs!=null) {
      val errors: List[String] = List(s"TimelinePutResponse with ${responseErrs.size()} errors")
      for (err <- responseErrs) {
        errors +: describeError(err)
      }
      errors.foldLeft("")((buff, elt) => buff + "\n" + elt)
    } else {
      s"TimelinePutResponse with null error list"
    }
  }

  /**
   * Lookup a required field in the `otherInfo` section of a [[TimelineEntity]]
   * @param en entity
   * @param name field name
   * @return the value
   * @throws Exception if the field is not found
   */
  private def field(en: TimelineEntity, name: String) : Object = {
    var value = en.getOtherInfo().get(name)
    if (value == null) {
      value = "Undefined"
    }
    value
  }

  /**
   * Lookup a required numeric field in the `otherInfo` section of a [[TimelineEntity]]
   * @param en entity
   * @param name field name
   * @return the value
   * @throws Exception if the field is not found or it is not a number
   */
  private def numberField(en: TimelineEntity, name: String) : Number = {
    val contents = field(en, name)
    contents match {
      case n: Number => n
      case _ => 0L
    }
  }

  /**
   * Build an [[ApplicationHistoryInfo]] instance from
   * a [[TimelineEntity]]
   * @param en the entity
   * @return an history info structure. The completed bit is strue if the entity has an
   *         end time.
   * @throws Exception if the entity lacked an entry of that key
   * @throws ClassCastException if the the key contained value, but it
   *                            could not be converted to the desired type
   */
  def toApplicationHistoryInfo(en: TimelineEntity) : ApplicationHistoryInfo = {
    var endTime = 0L
    try {
      endTime = numberField(en, FIELD_END_TIME).longValue
    } catch {
      case NonFatal(e) => endTime = 0L
    }
    ApplicationHistoryInfo(en.getEntityId(),
      field(en, FIELD_APP_NAME).asInstanceOf[String],
      numberField(en, FIELD_START_TIME).longValue,
      endTime,
      endTime,
      field(en, FIELD_APP_USER).asInstanceOf[String],
      endTime > 0)
  }
}
