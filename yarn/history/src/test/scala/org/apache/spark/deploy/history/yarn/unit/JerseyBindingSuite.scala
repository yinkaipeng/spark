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
package org.apache.spark.deploy.history.yarn.unit

import java.io.{ByteArrayInputStream, IOException, FileNotFoundException}
import java.net.URI

import com.sun.jersey.api.client.{ClientResponse, UniformInterfaceException, ClientHandlerException}
import org.apache.hadoop.fs.PathPermissionException

import org.apache.spark.deploy.history.yarn.AbstractYarnHistoryTests
import org.apache.spark.deploy.history.yarn.rest.JerseyBinding

/**
 * Unit test of how well the Jersey Binding works -especially some error handling logic
 * Which can follow different paths
 */
class JerseyBindingSuite extends AbstractYarnHistoryTests {

  val uriPath = "http://spark.apache.org"
  val uri = new URI(uriPath)

  def translate(ex: Throwable): Throwable = {
    JerseyBinding.translateException("GET", uri, ex)
  }

  /**
   * Build a [[UniformInterfaceException]] with the given string body
   * @param status status code
   * @param body body message
   * @param buffer buffer flag
   * @return new instance
   */
  def newUIE(status: Int, body: String, buffer: Boolean ): UniformInterfaceException = {
    val response = new ClientResponse(status,
             null,
             new ByteArrayInputStream(body.getBytes("UTF-8")),
             null)
    new UniformInterfaceException(response, buffer)
  }

  /**
   * If a [[ClientHandlerException]] contains an IOE, it
   * is unwrapped and returned
   */
  test("UnwrapIOEinClientHandler") {
    val fnfe = new FileNotFoundException("/tmp")
    val che = new ClientHandlerException(fnfe)
    assertResult(fnfe) {
      translate(che)
    }
  }

  /**
   * If a [[ClientHandlerException]] does not contains an IOE, it
   * is wrapped, but the inner text is extracted
   */
  test("BuildIOEinClientHandler") {
    val npe = new NullPointerException("oops")
    val che = new ClientHandlerException(npe)
    val ex = translate(che)
    assertResult(che) { ex.getCause}
    assert(ex.getMessage.contains("oops"))
    assert(ex.toString.contains(uriPath))
  }

  /**
   * If a [[ClientHandlerException]] does not contains an IOE, it
   * is unwrapped and returned
   */
  test("EmptyClientHandlerException") {
    val che = new ClientHandlerException("che")
    val ex = translate(che)
    assertResult(che) { ex.getCause}
    assert(ex.getMessage.contains("che"))
    assert(ex.toString.contains(uriPath))
  }

  /**
   * If the URI passed into translating a CHE is null, no
   * URI is printed
   */
  test("Null URI for ClientHandlerException") {
    val che = new ClientHandlerException("che")
    val ex = JerseyBinding.translateException("POST", null, che)
    assertResult(che) { ex.getCause}
    assert(ex.toString.contains("POST"))
    assert(ex.toString.contains("unknown"))
  }


  test("UniformInterfaceException null response") {
    // bufferResponseEntity must be false to avoid triggering NPE in constructor
    val uie = new UniformInterfaceException("uae", null, false)
    val ex = translate(uie)
    assertResult(uie) { ex.getCause }
    assert(ex.getMessage.contains("uae"))
    assert(ex.toString.contains(uriPath))
  }

  test("UniformInterfaceException 404 no body response") {
    val uie = newUIE(404,"", false)
    val ex = translate(uie)
    assertResult(uie) { ex.getCause }
    assert(ex.isInstanceOf[FileNotFoundException], s"not FileNotFoundException: $ex")
    assert (ex.getMessage.contains("404"))
    assert (ex.toString.contains(uriPath))
  }


  test("UniformInterfaceException 403 forbidden") {
    val uie = newUIE(403, "forbidden", false)
    val ex = translate(uie)
    assertResult(uie) { ex.getCause }
    assert(ex.isInstanceOf[PathPermissionException], s"not PathPermissionException: $ex")
    assert(ex.getMessage.contains("403"))
    assert(ex.toString.contains(uriPath))
  }


  test("UniformInterfaceException 500 response") {
    val uie = newUIE(500, "internal error", false)
    val ex = translate(uie)
    assertResult(uie) { ex.getCause }
    assert(ex.isInstanceOf[IOException], s"not IOException: $ex")
    assert(ex.getMessage.contains("500"))
    assert(ex.getMessage.contains("GET"))
    assert(ex.toString.contains(uriPath))
  }


}
