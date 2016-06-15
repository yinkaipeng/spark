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

package org.apache.spark.cloud.s3.examples

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * A line count example which has a default reference of a public Amazon S3
 * .csv.gz file dataset in the absence of anything on the command line.
 */
private[cloud] object S3LineCount extends S3ExampleBase {

  /**
   * Read a file and print out some details.
   * Any exception raised is logged at error and then the exit code set to -1.
   * @param args argument array; if empty then the default CSV path is used.
   */
  def main(args: Array[String]) {
    execute(action, args)
  }

  /**
   * Count all lines in a file in a remote object store.
   * This is scoped to be accessible for testing.
   * If the default S3A file is used, the configuration is patched to allow for anonymous access
   * on Hadoop 2.8+. The option to set the credential provider is not supported on Hadoop 2.6/2.7,
   * so the context must contain any credentials needed to authenticate with AWS.
   * @param sparkConf configuration to use
   * @param args argument array; if empty then the default CSV path is used.
   * @return an exit code
   */
  def action(sparkConf: SparkConf, args: Array[String]): Int = {
    val source = if (args.length >= 1) args(0) else S3A_CSV_PATH_DEFAULT
    logInfo(s"Source file = $source")
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)
    // smaller block size to divide up work
    cset(sparkConf, "fs.s3a.block.size", (2 * 1024 * 1024).toString)

    // when using the default value,
    // if the version of Hadoop supports credential provider plugin, switch
    // to the anonymous provider, so credentials are not needed
    if (args.length == 0) {
      cset(sparkConf, "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    }
    logInfo(s"Data Source $srcURI")

    val sc = new SparkContext(sparkConf)
    try {
      val fs = FileSystem.get(srcURI, sc.hadoopConfiguration)
      // this will throw an exception if the file is missing
      val status = fs.getFileStatus(srcPath)
      val started = System.currentTimeMillis()
      val input = sc.textFile(source)
      val count = input.count()
      val finished = System.currentTimeMillis()
      logInfo(s"Read $srcURI: size = ${status.getLen}; line count = $count")
      logInfo(s"Time to read: ${finished - started} milliseconds")
      logInfo(s"File System = $fs")
    } finally {
      sc.stop()
    }
    0
  }

}
