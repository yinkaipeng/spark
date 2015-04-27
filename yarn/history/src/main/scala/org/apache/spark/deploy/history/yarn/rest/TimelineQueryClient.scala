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
package org.apache.spark.deploy.history.yarn.rest

import java.net.{URI, URL}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import com.sun.jersey.api.client.{Client, WebResource}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntities, TimelineEntity}

import org.apache.spark.Logging

/**
 * A class to make queries of the Timeline sever through a Jersey client.
 * Exceptions raised by Jersey are extracted and potentially translated.
 * @param jerseyClient client
 * @param timelineURI URI of the timeline service
 */
private[spark] class TimelineQueryClient(timelineURI: URI, jerseyClient: Client) extends Logging {

  val timelineResource = jerseyClient.resource(timelineURI)
  val timelineURL = timelineURI.toURL

  /**
   * Construct a URI under the timeline service URI
   * @param path subpath
   * @return a new URI
   */
  def uri(path: String): URI = {
    new URL(timelineURL, path).toURI
  }

  /**
   * Get a resource under the service
   * @param path path
   * @return a new resource
   */
  def subresource(path: String): WebResource = {
    timelineResource.path(path)
  }

  /**
   * Execute a GET operation against a specific URI, uprating jersey faults
   * into more specific exceptions
   * @param uri URI (used when handling exceptions)
   * @param action action to perform
   * @tparam T type of response
   * @return the result of the action
   */
  def get[T](uri: URI, action: (() => T)) = {
    exec("GET", uri, action)
  }

  /**
   * Execute a POST operation against a specific URI, uprating jersey faults
   * into more specific exceptions
   * @param uri URI (used when handling exceptions)
   * @param action action to perform
   * @tparam T type of response
   * @return the result of the action
   */
  def exec[T](verb: String, uri: URI, action: (() => T)) = {
    logDebug(s"$verb $uri")
    try {
      action()
    } catch {
      case e: Exception =>
        throw JerseyBinding.translateException(verb, uri, e)
    }
  }

  /**
   * Peform an about query
   * @return information about the seervice
   */
  def about(): String = {
    val aboutURI = uri("")
    val resource = jerseyClient.resource(aboutURI)
    val result = get(aboutURI,
       (() => resource.accept(MediaType.APPLICATION_JSON)
             .get(classOf[String])))
    result
  }

  /**
   * Add a new query param if the option contains a value; the stringified value of the optional
   * is used as the query parameter value
   * @param resource resource to extend
   * @param name parameter name
   * @param opt option
   * @return a new resource
   */
  private def applyOptionalParam(resource: WebResource,
      name: String,
      opt: Option[Any]): WebResource = {
    opt match {
      case Some(value) => resource.queryParam(name, value.toString)
      case None => resource
    }
  }


  /**
   * Get entities matching the entity type and any optinal filters.
   * All parameters other than <code>entityType</code> have
   * default values; None for <code>optional</code>, empty
   * collections for the others.
   * @param entityType entity type
   * @param primaryFilter primary filter
   * @param secondaryFilters map of secondary filters
   * @param fields list of fields to retrieve
   * @param limit limit on how many to retrieve
   * @param windowStart time window to start retrieval
   * @param windowEnd time window to stop retrieval
   * @param fromId ID to start from
   * @param fromTs
   * @return a possibly empty list of entities
   */
    def listEntities(entityType: String,
        primaryFilter: Option[(String, String)] = None,
        secondaryFilters: Map[String, String] = Map(),
        fields: Seq[String] = Nil,
        limit: Option[Long] = None,
        windowStart: Option[Long] = None,
        windowEnd: Option[Long] = None,
        fromId: Option[String] = None,
        fromTs: Option[Long] = None): List[TimelineEntity] = {
      require(!entityType.isEmpty, "no entity type")
      var resource = subresource(entityType)
      // build the resource
      // every application returns a new result, which complicates applying map and list arguments
      // to it. hence the use of a variable
      resource = primaryFilter match {
        case Some((key, value)) =>
          resource.queryParam("primaryFilter", s"$key:$value")
        case None =>
          resource
      }
      secondaryFilters foreach
          ((t: (String, String)) =>
            resource = resource.queryParam("secondaryFilter", s"${t._1}:${t._2}"))
      resource = applyOptionalParam(resource, "windowStart", windowStart)
      resource = applyOptionalParam(resource, "windowEnd", windowEnd)
      resource = applyOptionalParam(resource, "limit", limit)
      resource = applyOptionalParam(resource, "fromId", fromId)
      resource = applyOptionalParam(resource, "fromTs", fromTs)
      if (fields.nonEmpty) {
        resource = resource.queryParam("fields", fields.mkString(","))
      }
      // execute the request
      val response = get(resource.getURI,
          (() => resource
                 .accept(MediaType.APPLICATION_JSON)
                 .get(classOf[TimelineEntities])))
      response.getEntities.asScala.toList
  }

  /**
   * Get an entity
   * @param entityType type
   * @param entityId the entity
   * @return the entity if it was found
   */
  def getEntity(entityType: String, entityId: String): TimelineEntity = {
    require(!entityId.isEmpty, "no entity ID")
    var resource = subresource(entityType).path(entityId)
      get(resource.getURI,
        (() => resource
               .accept(MediaType.APPLICATION_JSON)
               .get(classOf[TimelineEntity])))
  }

  /**
   * toString method returns the URI of the timeline service
   * @return
   */
  override def toString: String = {
    s"Timeline Query Client against $timelineURI"
  }
}
