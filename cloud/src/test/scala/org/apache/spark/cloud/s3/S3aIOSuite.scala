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

package org.apache.spark.cloud.s3

import org.apache.hadoop.fs.Path

import org.apache.spark.cloud.common.BasicIOTests
import org.apache.spark.cloud.s3.examples.S3FileGenerator

private[cloud] class S3aIOSuite extends BasicIOTests with S3aTestSetup {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  ctest("FileGeneratorUsage",
    "S3A File Generator",
    "Execute the S3FileGenerator example with a bad argument; expect a failure") {
    val conf = newSparkConf()
    conf.setAppName("FileGenerator")
    assert(-2 === S3FileGenerator.action(conf, Array()))
  }



  ctest("FileGenerator",
    "S3A File Generator",
    "Execute the S3FileGenerator example") {
    val conf = newSparkConf()
    conf.setAppName("FileGenerator")
    val destFile = new Path(TestDir, "filegenerator.txt")
    assert(0 === S3FileGenerator.action(conf, Array(destFile.toString)))
    filesystem.getFileStatus(destFile)
  }
}
