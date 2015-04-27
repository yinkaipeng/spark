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

import java.io.{FileNotFoundException, IOException}
import java.net.{HttpURLConnection, URI, URL}
import java.nio.file.{AccessDeniedException, FileSystemException}
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType

import com.sun.jersey.api.client.config.{DefaultClientConfig, ClientConfig}
import com.sun.jersey.api.client.{Client, ClientHandlerException, UniformInterfaceException}
import com.sun.jersey.api.json.JSONConfiguration
import com.sun.jersey.client.urlconnection.{HttpURLConnectionFactory, URLConnectionClientHandler}
import org.apache.hadoop.conf.Configuration
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector

import org.apache.spark.Logging

/**
 * Jersey specific integration with the SPNEG Auth
 * @param conf configuration to build off
 */
private[spark] class JerseyBinding(conf: Configuration) extends Logging with HttpURLConnectionFactory {
  private val connector = SpnegoUrlConnector.newInstance(conf)
  private val handler = new URLConnectionClientHandler(this);

  override def getHttpURLConnection(url: URL): HttpURLConnection = {
    return connector.getHttpURLConnection(url)
  }
}

private[spark] object JerseyBinding extends Logging {


  /**
   * Translate exceptions, where possible. If not, it is passed through unchanged
   * @param verb HTTP verb
   * @param targetURL URL of operation
   * @param exception exception caught
   * @return an exception to log, ingore, throw...
   */
  def translateException(verb: String,
    targetURL: URI,
    exception: Exception): Exception = {
    exception match {
      case ex: ClientHandlerException =>
        translateException(verb, targetURL, ex)
      case ex: UniformInterfaceException =>
        translateException(verb, targetURL, ex)
      case _ =>
        exception
    }
  }
  
  /**
   * Handle a client-side Jersey exception by extracting the inner cause
   * <p>
   * If there's an inner IOException, return that.
   * <p>
   * Otherwise: create a new wrapper IOE including verb and target details
   * @param verb HTTP Verb used
   * @param targetURL URL being targeted
   * @param exception original exception
   * @return an exception to throw
   */
  def translateException(verb: String,
    targetURL: URI,
    exception: ClientHandlerException): IOException = {
    if (exception.getCause.isInstanceOf[IOException]) {
      exception.getCause.asInstanceOf[IOException]
    } else {
      val ioe = new IOException(verb + " " + targetURL + " failed: " + exception)
      ioe.initCause(exception)
      ioe
    }
  }

  /**
   * Convert Jersey exceptions into useful IOExceptions. This includes
   * building an error message which include the URL, verb and status code,
   * logging any text body, and wrapping in an IOException or subclass
   * with that message and the response's exception as a nested exception.
   * @param verb HTTP Verb used
   * @param targetURL URL being targeted
   * @param exception original exception
   * @return a new exception, the original one nested as a cause
   */
  def translateException(verb: String,
    targetURL: URI,
    exception: UniformInterfaceException): IOException = {
    var ioe: IOException = null
    val response = exception.getResponse
    val uri = if (targetURL != null) targetURL.toString else ("unknown URL")
    if (response != null) {
      val status: Int = response.getStatus
      var body: String = ""
      try {
        if (response.hasEntity) {
          try {
            body = response.getEntity(classOf[String])
          }
          catch {
            case e: Exception => // ignored
          }
          logError(s"$verb $uri returned status $status and body\n$body")
        }
        else {
          logError(s"$verb $uri returned status $status and empty body")
        }
      } catch {
        case e: Exception => {
          log.warn("Failed to extract body from client response", e)
        }
      }
      if (status == HttpServletResponse.SC_UNAUTHORIZED ||
          status == HttpServletResponse.SC_FORBIDDEN) {
        ioe = new AccessDeniedException(uri, null, s"Status code $status; $body")
      } else if (status == HttpServletResponse.SC_BAD_REQUEST ||
          status == HttpServletResponse.SC_NOT_ACCEPTABLE ||
          status == HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE) {
        // ideally a specific exception could be raised here, but there is no ideal match in the JDK
        ioe = new FileSystemException(uri, null,
                    s"Bad $verb request: status code $status against $uri; $body")

      } else if (status > 400 && status < 500) {
        ioe = new FileNotFoundException(
            s"Bad $verb request: status code $status against $uri; $body")
      } else {
        ioe = new FileSystemException(uri, null, s"$verb $uri failed: " +
            s"with status code $status: $exception; $body")
      }
    } else {
      ioe = new FileSystemException(uri, null, s"$verb $uri failed: $exception")
    }
    ioe.initCause(exception)
    ioe
  }

  /**
   * Create a Jersey client with the UGI binding set up
   * @param conf Hadoop configuration
   * @param clientConfig jersey client config
   * @return a new client instance
   */
  def createJerseyClient(conf: Configuration, clientConfig: ClientConfig): Client = {
    val jerseyBinding = new JerseyBinding(conf)
    new Client(jerseyBinding.handler, clientConfig);
  }

  /**
   * Create the client config for Jersey. Made static
   * @return
   */
  def createClientConfig(): ClientConfig = {
    val cc = new DefaultClientConfig()
    cc.getClasses().add(classOf[JsonJaxbBinding])
    cc.getFeatures.put(JSONConfiguration.FEATURE_POJO_MAPPING, true)
    cc
  }
}


/**
 * Define the jaxb binding for the Jersey client
 */
private[spark] class JsonJaxbBinding extends JacksonJaxbJsonProvider {

  override def locateMapper(classtype: Class[_], mediaType: MediaType): ObjectMapper = {
    val mapper = super.locateMapper(classtype, mediaType)
    val introspector = new JaxbAnnotationIntrospector
    mapper.setAnnotationIntrospector(introspector)
    mapper.setSerializationInclusion(Inclusion.NON_NULL)
    mapper
  }

}
