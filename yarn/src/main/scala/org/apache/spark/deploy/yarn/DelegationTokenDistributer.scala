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
package org.apache.spark.deploy.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.Credentials
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{Logging, SparkConf}

/*
 * Handles distribution of tokens in a yarn spark application.
 * AMDelegationTokenRenewer takes care of renewing the tokens, DelegationTokenDistributer handles
 * distribution and ExecutorDelegationTokenUpdater takes care of updating driver and
 * executors with the distributed tokens.
 *
 * Currently token distribution is done via hdfs, the delegation tokens/credentials are written to
 * HDFS in a pre-specified location - the prefix of which is specified in the sparkConf by
 * spark.yarn.credentials.file (so the file(s) would be named c-1, c-2 etc.
 * Each update goes to a new file, with a monotonically increasing suffix). After this,
 * the credentials are updated once 75% of the new tokens renewal interval has elapsed.
 *
 * On the executor side, the updateCredentialsIfRequired method is called once 80% of the
 * validity of the original tokens has elapsed. At that time the executor finds the
 * credentials file with the latest timestamp and checks if it has read those credentials
 * before (by keeping track of the suffix of the last file it read). If a new file has
 * appeared, it will read the credentials and update the currently running UGI with it. This
 * process happens again once 80% of the validity of this has expired.
 */
private[spark] class DelegationTokenDistributer(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends Logging {

  private var lastCredentialsFileSuffix = 0
  private val hadoopUtil = YarnSparkHadoopUtil.get

  private val credentialsFile = sparkConf.get("spark.yarn.credentials.file")
  private val daysToKeepFiles =
    sparkConf.getInt("spark.yarn.credentials.file.retention.days", 5)
  private val numFilesToKeep =
    sparkConf.getInt("spark.yarn.credentials.file.retention.count", 5)

  private[yarn] val freshHadoopConf =
    hadoopUtil.getConfBypassingFSCache(hadoopConf, new Path(credentialsFile).toUri.getScheme)

  // Keeps only files that are newer than daysToKeepFiles days, and deletes everything else. At
  // least numFilesToKeep files are kept for safety
  private def cleanupOldFiles(): Unit = {
    import scala.concurrent.duration._
    try {
      val remoteFs = FileSystem.get(freshHadoopConf)
      val credentialsPath = new Path(credentialsFile)
      val thresholdTime = System.currentTimeMillis() - (daysToKeepFiles days).toMillis
      hadoopUtil.listFilesSorted(
        remoteFs, credentialsPath.getParent,
        credentialsPath.getName, SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)
        .dropRight(numFilesToKeep)
        .takeWhile(_.getModificationTime < thresholdTime)
        .foreach(x => remoteFs.delete(x.getPath, true))
    } catch {
      // Such errors are not fatal, so don't throw. Make sure they are logged though
      case e: Exception =>
        logWarning("Error while attempting to cleanup old tokens. If you are seeing many such " +
          "warnings there may be an issue with your HDFS cluster.", e)
    }
  }

  def distributeTokens(credentials: Credentials): Unit = {

    val remoteFs = FileSystem.get(freshHadoopConf)
    val credentialsPath = new Path(credentialsFile)

    // If lastCredentialsFileSuffix is 0, then the AM is either started or restarted. If the AM
    // was restarted, then the lastCredentialsFileSuffix might be > 0, so find the newest file
    // and update the lastCredentialsFileSuffix.
    if (lastCredentialsFileSuffix == 0) {
      hadoopUtil.listFilesSorted(
        remoteFs, credentialsPath.getParent,
        credentialsPath.getName, SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)
        .lastOption.foreach { status =>
        lastCredentialsFileSuffix = hadoopUtil.getSuffixForCredentialsPath(status.getPath)
      }
    }
    val nextSuffix = lastCredentialsFileSuffix + 1
    val tokenPathStr =
      credentialsFile + SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM + nextSuffix
    val tokenPath = new Path(tokenPathStr)
    val tempTokenPath = new Path(tokenPathStr + SparkHadoopUtil.SPARK_YARN_CREDS_TEMP_EXTENSION)
    logInfo("Writing out delegation tokens to " + tempTokenPath.toString)
    credentials.writeTokenStorageFile(tempTokenPath, freshHadoopConf)
    logInfo(s"Delegation Tokens written out successfully. Renaming file to $tokenPathStr")
    remoteFs.rename(tempTokenPath, tokenPath)
    logInfo("Delegation token file rename complete.")
    lastCredentialsFileSuffix = nextSuffix

    cleanupOldFiles()
  }

}
