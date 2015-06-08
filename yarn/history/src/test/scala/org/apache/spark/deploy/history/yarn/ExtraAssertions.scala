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

import java.util.{Collection => JCollection}

import org.apache.hadoop.service.Service
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity
import org.scalatest.Assertions

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._

/**
 * Miscellaneous extra assertions
 */
trait ExtraAssertions extends Logging with Assertions {

  /**
   * Assert that an exception's toString value contains the supplied text.
   * If not, an error is logged and the exception is rethrown
   * @param ex exception
   * @param text text
   */
  def assertExceptionMessageContains(ex: Exception, text: String): Unit =  {
    if (!ex.toString.contains(text)) {
      logError(s"Did not find text ${text} in $ex", ex)
      throw ex
    }
  }

  /**
   *  assert that a value is not null
   * @param value value
   * @param text text for assertion
   */
  def assertNotNull(value: Any, text: String): Unit = {
    if (value == null) {
      fail(s"Null value; $text")
    }
  }

  /**
   * Assert that a Spark traversable instance is not empty
   * @param traversable the traversable to check
   * @param text text for assertion
   * @tparam T traversable type
   */
  def assertNotEmpty[T](traversable: Traversable[T], text: String): Unit = {
    assert(!traversable.isEmpty, s"Empty traversable; $text")

  }

  /**
   * Assert that a java collection is not empty
   * @param collection the collection to check
   * @param text text for assertion
   * @tparam T collection type
   */
  def assertNotEmpty[T](collection: JCollection[T], text: String): Unit = {
    assert (!collection.isEmpty, s"Empty collection; $text")
  }


  /**
   * assert that a service is not listening
   * @param historyService history service
   */
  def assertNotListening(historyService: YarnHistoryService): Unit = {
    assert(!historyService.listening,
            s"history service is listening for events: $historyService")
  }


  /**
   * Assert that the number of events processed matches the number expected
   * @param historyService history service
   * @param expected expected number
   * @param details text to include in error messages
   */
  def assertEventsProcessed(historyService: YarnHistoryService,
      expected: Int, details: String): Unit = {
    assertResult(expected, "wrong number of events processed " + details) {
      historyService.getEventsProcessed
    }
  }

  /**
   * Assert that two timeline entities are non-null and equal
   * @param expected expected entry
   * @param actual actual
   */
  def assertEquals(expected: TimelineEntity, actual: TimelineEntity): Unit = {
    require(expected != null)
    require(actual != null)
    assert(expected == actual,
            s"Expected ${describeEntity(expected)} -- got ${describeEntity(actual)}}")
  }

  /**
   * Assert that a service is in a specific state
   * @param service service
   * @param state required state
   */
  def assertInState(service: Service, state: Service.STATE): Unit = {
    assertNotNull(service, "null service")
    assert(service.isInState(state),
            s"not in state $state: $service")
  }

  /**
   * Assert that a source string contains the `contained` substring.
   * (This is not a check for a proper subset; equality is also acceptable)
   * @param source source string
   * @param contained string to look for
   */
  def assertContains(source: String, contained: String): Unit = {
    assertNotNull(source, "null source")
    assert(source.contains(contained),
          s"did not find '${contained}' in '${source}'")
  }

  /**
   * Assert that a source string does contains the `contained` substring.
   * @param source source string
   * @param contained string to look for
   */
  def assertDoesNotContain(source: String, contained: String): Unit = {
    assertNotNull(source, "null source")
    assert(!source.contains(contained),
          s"Found '${contained}' in '${source}'")
  }

  /**
   * Assert that a [String, String] map contains a key:value mapping,
   * and that the value contains the specified text.
   * @param map map to query
   * @param key key to retrieve
   * @param text text to look for in the resolved value
   */
  protected def assertMapValueContains(map: Map[String, String], key: String, text: String): Unit = {
    map.get(key) match {
      case Some(s) =>
        if (!text.isEmpty && !s.contains(text)) {
          fail(s"Did not find '$text' in key[$key] = '$s'")
        }
      case None =>
        fail(s"No entry for key $key")
    }
  }
}
