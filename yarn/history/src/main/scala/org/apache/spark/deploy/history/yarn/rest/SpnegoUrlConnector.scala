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
import java.net.{HttpURLConnection, URL, URLConnection}
import javax.net.ssl.HttpsURLConnection

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.PathPermissionException
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.client.{AuthenticatedURL, AuthenticationException, Authenticator, ConnectionConfigurator, KerberosAuthenticator, PseudoAuthenticator}
import org.apache.hadoop.security.ssl.SSLFactory
import org.apache.hadoop.security.token.delegation.web.{DelegationTokenAuthenticatedURL, DelegationTokenAuthenticator, KerberosDelegationTokenAuthenticator, PseudoDelegationTokenAuthenticator}

import org.apache.spark.Logging

/**
 * SPNEGO-backed URL connection factory for Java net, and hence Jersey.
 * Based on WebHFDS client code in
 * <code>org.apache.hadoop.hdfs.web.URLConnectionFactory</code>
 */
private[spark] class SpnegoUrlConnector(connConfigurator: ConnectionConfigurator,
  token: DelegationTokenAuthenticatedURL.Token) extends Logging {

  private val secure = UserGroupInformation.isSecurityEnabled
  private val authenticator: DelegationTokenAuthenticator = if (secure)  {
      new KerberosDelegationTokenAuthenticator()
    } else {
      new PseudoDelegationTokenAuthenticator
    }

  authenticator.setConnectionConfigurator(connConfigurator)

  /**
   * Opens a url 
   *
   * @param url
   * URL to open
   * @param isSpnego
   * whether the url should be authenticated via SPNEGO
   * @return URLConnection
   * @throws IOException
   * @throws AuthenticationException
   */

  def openConnection(url: URL, isSpnego: Boolean): URLConnection = {
    require(connConfigurator != null)
    require(url.getPort != 0, "no port")
    if (isSpnego) {
      logDebug("open AuthenticatedURL connection" + url)
      UserGroupInformation.getCurrentUser.checkTGTAndReloginFromKeytab
      val authToken: AuthenticatedURL.Token = new AuthenticatedURL.Token
      new AuthenticatedURL(KerberosUgiAuthenticator, connConfigurator).openConnection(url, authToken)
    }
    else {
      logDebug("open URL connection")
      val connection: URLConnection = url.openConnection
      connection match {
        case connection1: HttpURLConnection =>
          connConfigurator.configure(connection1)
        case _ =>
      }
      connection
    }
  }

  /**
   * Opens a url with cache disabled, redirect handled in
   * (JDK) implementation.
   *
   * @param url to open
   * @return URLConnection
   * @throws IOException
   */

  def getHttpURLConnection(url: URL): HttpURLConnection = {
    val isProxyAccess =
      UserGroupInformation.getCurrentUser.getAuthenticationMethod ==
          UserGroupInformation.AuthenticationMethod.PROXY
    val callerUGI = if (isProxyAccess) {
        UserGroupInformation.getCurrentUser.getRealUser
      } else {
        UserGroupInformation.getCurrentUser
      }
    val doAsUser = if (isProxyAccess) {
        UserGroupInformation.getCurrentUser.getShortUserName
      } else {
        null
      }
    val conn = callerUGI.doAs(new PrivilegedFunction(
        (() => {
          new DelegationTokenAuthenticatedURL(authenticator, connConfigurator)
              .openConnection(url, token, doAsUser)
        })))
    conn.setUseCaches(false)
    conn.setInstanceFollowRedirects(true)
    conn
  }

  /**
   * Uprate error codes 400 and up into exceptions, which are thrn thrown;
   * 404 is converted to a {@link NotFoundException},
   * 401 to {@link ForbiddenException}
   * all others above 400: <code>IOException</code>
   * Any error code under 400 is not considered a failure; this function will return normally.
   * @param verb HTTP Verb used
   * @param url URL as string
   * @param resultCode response from the request
   * @param bodyAsString optional body as a string. If set, used in preference to <code>body</code>
   * @param body optional body of the request. If set (and bodyAsString) unset, the body is
   *             converted to a string and used in the response
   * @throws IOException if the result code was 400 or higher
   */
  @throws(classOf[IOException])
  def uprateFaults(
    verb: String,
    url: String,
    resultCode: Int,
    bodyAsString: String,
    body: Array[Byte]): Unit = {
    if (resultCode >= 400) {
      val msg = s"$verb $url"
      if (resultCode == 404) {
        throw new FileNotFoundException(msg)
      } else if (resultCode == 401) {
        throw new PathPermissionException(msg)
      }
      val bodyText = if (bodyAsString != null) {
          bodyAsString
        } else if (body != null && body.length > 0) {
          new String(body)
        } else {
          ""
        }
      val message = s"$msg failed with exit code $resultCode, body length" +
          s" ${bodyText.length}\n${bodyText}"
      logError(message)
      throw new IOException(message)
    }
  }

  def execHttpOperation(verb: String,
    url: URL,
    payload: Array[Byte],
    payloadContentType: String): HttpOperationResponse = {
    var conn: HttpURLConnection = null
    val outcome = new HttpOperationResponse
    var resultCode = 0
    var body: Array[Byte] = null
    logDebug(s"$verb $url spnego=$secure")
    val hasData = payload != null
    try {
      conn = getHttpURLConnection(url)
      conn.setRequestMethod(verb)
      conn.setDoOutput(hasData)
      if (hasData) {
        require(payloadContentType != null, "no content type")
        conn.setRequestProperty("Content-Type", payloadContentType)
      }
      // connection
      conn.connect
      if (hasData) {
        // submit any data
        val output = conn.getOutputStream
        IOUtils.write(payload, output)
        output.close()
      }
      resultCode = conn.getResponseCode
      outcome.lastModified = conn.getLastModified
      outcome.contentType = conn.getContentType

      var stream = conn.getErrorStream
      if (stream == null) {
        stream = conn.getInputStream
      }
      if (stream != null) {
        body = IOUtils.toByteArray(stream)
      }
      else {
        log.debug("No body in response")
      }
    } finally {
      if (conn != null) {
        conn.disconnect
      }
    }
    uprateFaults(verb, url.toString, resultCode, "", body)
    outcome.responseCode = resultCode
    outcome.data = body
    outcome
  }
}

/**
 * A response for use as a return value from operations
 */
private[spark] class HttpOperationResponse {
  var responseCode: Int = 0
  var lastModified: Long = 0L
  var contentType: String = ""
  var data: Array[Byte] = new Array(0)


  override def toString: String = {
    s"status $responseCode; last modified $lastModified," +
        s" contentType $contentType" +
        s" data size=${if (data == null) -1 else data.length}}"
  }

  /**
   * Convert the response data into a string and return it.
   * <p>
   * There must be some response data for this to work
   * @return the body as a string.
   */
  def responseBody: String = {
    require( data != null, "no response body in $this")
    new String(data)
  }
}

/**
 * Use UserGroupInformation as a fallback authenticator
 * if the server does not use Kerberos SPNEGO HTTP authentication.
 */
private object KerberosUgiAuthenticator extends KerberosAuthenticator {

  private object UgiAuthenticator extends PseudoAuthenticator {
    protected override def getUserName: String = {
      try {
        return UserGroupInformation.getLoginUser.getUserName
      } catch {
        case e: IOException => {
          throw new SecurityException("Failed to obtain current username", e)
        }
      }
    }
  }
  
  protected override def getFallBackAuthenticator: Authenticator = {
    return UgiAuthenticator;
  }
}

/**
 * Default Connection Configurator: simply sets the socket timeout
 */
private object DefaultConnectionConfigurator extends ConnectionConfigurator {
  override def configure(conn: HttpURLConnection): HttpURLConnection = {
    SpnegoUrlConnector.setTimeouts(conn, SpnegoUrlConnector.DEFAULT_SOCKET_TIMEOUT)
    conn
  }
}

private[spark] object SpnegoUrlConnector extends Logging {

  /**
   * Timeout for socket connects and reads
   */
  val DEFAULT_SOCKET_TIMEOUT: Int = 1 * 60 * 1000
  /**
   * Sets timeout parameters on the given URLConnection.
   *
   * @param connection
   * URLConnection to set
   * @param socketTimeout
   * the connection and read timeout of the connection.
   */
  def setTimeouts(connection: URLConnection, socketTimeout: Int) {
    connection.setConnectTimeout(socketTimeout)
    connection.setReadTimeout(socketTimeout)
  }

  /**
   * Construct a new URLConnectionFactory based on the configuration.
   *
   * @param conf configuration
   * @param token delegation token
   * @return a new instance
   */
  def newInstance(conf: Configuration,
      token: DelegationTokenAuthenticatedURL.Token = new DelegationTokenAuthenticatedURL.Token): SpnegoUrlConnector = {
    var conn: ConnectionConfigurator = null
    try {
      conn = newSslConnConfigurator(DEFAULT_SOCKET_TIMEOUT, conf)
    } catch {
      case e: Exception => {
        logDebug("Cannot load customized ssl related configuration." +
            " Fallback to system-generic settings.",
                  e)
        conn = DefaultConnectionConfigurator
      }
    }
    new SpnegoUrlConnector(conn, token)
  }

  /**
   * Create a new ConnectionConfigurator for SSL connections
   */

  def newSslConnConfigurator(timeout: Int, conf: Configuration): ConnectionConfigurator = {
    val factory: SSLFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf)
    factory.init
    val sf = factory.createSSLSocketFactory
    val hv = factory.getHostnameVerifier
    new ConnectionConfigurator {
      @throws(classOf[IOException])
      def configure(conn: HttpURLConnection): HttpURLConnection = {
        conn match {
          case c: HttpsURLConnection =>
            c.setSSLSocketFactory(sf)
            c.setHostnameVerifier(hv)
          case _ =>
        }
        setTimeouts(conn, timeout)
        conn
      }
    }
  }
}
