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

package org.apache.spark.sql.hive.thriftserver

import org.apache.hive.service.cli.CLIService
import org.apache.hive.service.cli.thrift.{TOpenSessionReq, TOpenSessionResp, TProtocolVersion}
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}

/**
  * Subclasses for http/thrift cli service, where we also explicitly set the version.
  */
private[hive] object SparkCLIServices {

  private def setProtocolVersion(session: TOpenSessionResp): TOpenSessionResp = {
    session.setServerProtocolVersion(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5)
    session
  }

  class SparkThriftHttpCLIService(cliService: CLIService)
    extends ThriftHttpCLIService(cliService) {

    override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
      setProtocolVersion(super.OpenSession(req))
    }
  }

  class SparkThriftBinaryCLIService(cliService: CLIService)
    extends ThriftBinaryCLIService(cliService) {

    override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
      setProtocolVersion(super.OpenSession(req))
    }
  }
}
