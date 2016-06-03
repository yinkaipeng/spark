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

import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * A line count example which has a default reference of a public Amazon S3
 * .csv.gz file dataset in the absence of anything on the command line.
 */
object S3ALineCount extends Logging {
  /**
   * Default source of a public multi-MB CSV file.
   */
  val S3A_CSV_PATH_DEFAULT = "s3a://landsat-pds/scene_list.gz"

  /**
   * Read a file and print out some details.
   * Any exception raised is logged at error and then the exit code
   * set to -1.
   * @param args argument array; if 0 the default CSV path is used.
   */
  def main(args: Array[String]) {
    var exitCode = 0
    try {
      val conf = new SparkConf()
      // set a low block size for patitions
      conf.set("spark.hadoop.fs.s3a.block.size", (2* 1024 * 1024).toString)

      exitCode = innerMain(args, conf)
    } catch {
      case e: Exception =>
        logError(s"Failed to execute line count", e)
        exitCode = -1
    }
    System.exit(exitCode)
  }

  /**
   * Read a file and print out some details.
   * This is scoped to be accessible for testing.
   * @param args argument array; if 0 the default CSV path is used.
   * @param sparkConf configuration to use
   * @return an exit code
   */
  private[cloud] def innerMain(args: Array[String], sparkConf: SparkConf) : Int = {
    val source = if (args.length >= 1) args(0) else S3A_CSV_PATH_DEFAULT
    logInfo(s"Source file = $source")
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)
    sparkConf.setAppName("S3AlineCount")
    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(srcURI, sc.hadoopConfiguration)
    // this will throw an exception if the file is missing
    val status = fs.getFileStatus(srcPath)
    val input = sc.textFile(source)
    val count = input.count()
    logInfo(s"Read $srcURI: size = ${status.getLen} line count = ${count}")
    0
  }

}
