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
 * CSV .gz file in the absence of anything on the command line.
 */
private[cloud] object S3LineCount extends S3ExampleBase {

  /**
   * Read a file and print out some details.
   * Any exception raised is logged at error and then the exit code set to -1.
   * @param args argument array; if empty then the default CSV path is used.
   */
  def main(args: Array[String]): Unit = {
    execute(action, args)
  }

  def usage(): Int = {
    logInfo("Usage: org.apache.spark.cloud.s3.examples.S3LineCount [<source>] [<dest>]")
    EXIT_USAGE
  }

  /**
   * Count all lines in a file in a remote object store.
   * This is scoped to be accessible for testing.
   * If there is no destination file, the configuration is patched to allow for S3A
   * anonymous access on Hadoop 2.8+.
   * The option to set the credential provider is not supported on Hadoop 2.6/2.7,
   * so the spark/cluster configuration must contain any credentials needed to
   * authenticate with AWS.
   * @param sparkConf configuration to use
   * @param args argument array; if empty then the default CSV path is used.
   * @return an exit code
   */
  def action(sparkConf: SparkConf, args: Array[String]): Int = {
    if (args.length > 2) {
      return usage()
    }
    val source = arg(args, 0, S3A_CSV_PATH_DEFAULT)
    val dest = arg(args, 1)
    logInfo(s"Source file = $source")
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)
    // smaller block size to divide up work
    hconf(sparkConf, "fs.s3a.block.size", (2 * 1024 * 1024).toString)

    // If there is no destination, switch to the anonymous provider.
    if (dest.isEmpty) {
      hconf(sparkConf, "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    }
    logInfo(s"Data Source $srcURI")

    val sc = new SparkContext(sparkConf)
    try {
      val fs = FileSystem.get(srcURI, sc.hadoopConfiguration)
      // this will throw an exception if the source file is missing
      val status = fs.getFileStatus(srcPath)
      val input = sc.textFile(source)
      val count = duration(s" count $status") {
        input.count()
      }
      logInfo(s"line count = $count")
      dest.foreach { d =>
        duration("save") {
          val destPath = new Path(new URI(d))
          fs.mkdirs(destPath.getParent())
          input.saveAsTextFile(d)
          val status = fs.getFileStatus(destPath)
          logInfo(s"Generated file $status")
        }
      }
      logInfo(s"File System = $fs")
    } finally {
      logInfo("Stopping Spark Context")
      sc.stop()
    }
    0
  }

}
