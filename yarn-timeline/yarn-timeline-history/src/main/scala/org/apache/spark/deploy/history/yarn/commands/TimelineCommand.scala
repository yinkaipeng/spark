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

package org.apache.spark.deploy.history.yarn.commands

import java.io.FileNotFoundException

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.Tool

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.rest.UnauthorizedRequestException

abstract class TimelineCommand extends Configured with Tool with Logging {

  /**
   * Run the command.
   *
   * xit codes:
   * <pre>
   *  0: success
   * 44: "not found": the endpoint or a named
   * 41: "unauthed": caller was not authenticated
   * -1: any other failure
   * </pre>
   * @param args command line
   * @return exit code
   */
  override def run(args: Array[String]): Int = {
    if (UserGroupInformation.isSecurityEnabled) {
      logInfo(s"Logging in to secure cluster as ${UserGroupInformation.getCurrentUser}")
    }
    try {
      exec(args)
    } catch {

      case notFound: FileNotFoundException =>
        44

      case ure: UnauthorizedRequestException =>
        logError(s"Authentication Failure $ure", ure)
        41

      case e: Exception =>
        logError(s"Failed to fetch history", e)
        -1
    }
  }

  /**
   * Execute the operation.
   * @param args list of arguments
   * @return the exit code.
   */
  def exec(args: Seq[String]): Int = {
    0

  }
}
