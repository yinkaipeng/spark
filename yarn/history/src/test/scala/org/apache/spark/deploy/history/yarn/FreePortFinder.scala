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

import java.net.{InetAddress, ServerSocket}

/**
 * Trait to find free ports on localhost
 */
trait FreePortFinder {

  /**
   * Find a free port by listening on port 0
   * @return
   */
  def findPort(): Int = {
    val listen = tryToListen(0)
    return listen._2
  }

  /**
   * Simple function to see if a port is free; if it is
   * return the address and the port allocated.
   * <p>
   * This function can be passed to <code>Util.startServiceOnPort</code>
   * @param port port to try. If 0, the OS chooses the port
   * @return an (address, port) tuple
   */
  def tryToListen(port: Int): (InetAddress, Int) = {
    val socket: ServerSocket = new ServerSocket(port)
    val address = socket.getInetAddress
    val localPort = socket.getLocalPort
    socket.close
    (address, localPort)
  }

  /**
   * Return the value of the local host address. Defaults
   * to 127.0.0.1
   * @return the address to use for local/loopback addresses.
   */
  def localIPv4Address(): String = {
    "127.0.0.1"
  }

  def findIPv4AddressAsPortPair(): String = {
    localIPv4Address() + ":" + findPort()
  }

}
