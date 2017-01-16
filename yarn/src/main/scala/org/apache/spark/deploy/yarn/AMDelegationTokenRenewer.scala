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

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{Executors, TimeUnit}

import scala.language.postfixOps
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.ThreadUtils

/*
 * The following methods are primarily meant to make sure long-running apps like Spark
 * Streaming apps can run without interruption while writing to secure HDFS. The
 * scheduleLoginFromKeytab method is called on the driver when the
 * CoarseGrainedScheduledBackend starts up. This method wakes up a thread that logs into the KDC
 * once 75% of the renewal interval of the original delegation tokens used for the container
 * has elapsed. DelegationTokenDistributer takes care of distributing the tokens.
 *
 */
private[yarn] class AMDelegationTokenRenewer(
    sparkConf: SparkConf,
    tokenDistributer: DelegationTokenDistributer) extends Logging {

  private val delegationTokenRenewer =
    Executors.newSingleThreadScheduledExecutor(
      ThreadUtils.namedThreadFactory("Delegation Token Refresh Thread"))

  private val hadoopUtil = YarnSparkHadoopUtil.get

  private val credentialsFilePath = new Path(sparkConf.get("spark.yarn.credentials.file"))
  private val freshHadoopConf = tokenDistributer.freshHadoopConf

  /**
   * Schedule a login from the keytab and principal set using the --principal and --keytab
   * arguments to spark-submit. This login happens only when the credentials of the current user
   * are about to expire. This method reads spark.yarn.principal and spark.yarn.keytab from
   * SparkConf to do the login. This method is a no-op in non-YARN mode.
   *
   */
  private[spark] def scheduleLoginFromKeytab(): Unit = {
    val principal = sparkConf.get("spark.yarn.principal")
    val keytab = sparkConf.get("spark.yarn.keytab")

    /**
     * Schedule re-login and creation of new tokens. If tokens have already expired, this method
     * will synchronously create new ones.
     */
    def scheduleRenewal(runnable: Runnable): Unit = {
      val credentials = UserGroupInformation.getCurrentUser.getCredentials
      val renewalInterval = hadoopUtil.getTimeFromNowToRenewal(sparkConf, 0.75, credentials)
      // Run now!
      if (renewalInterval <= 0) {
        logInfo("HDFS tokens have expired, creating new tokens now.")
        runnable.run()
      } else {
        logInfo(s"Scheduling login from keytab in $renewalInterval millis.")
        delegationTokenRenewer.schedule(runnable, renewalInterval, TimeUnit.MILLISECONDS)
      }
    }

    // This thread periodically runs on the driver to update the delegation tokens on HDFS.
    val driverTokenRenewerRunnable =
      new Runnable {
        override def run(): Unit = {
          try {
            tokenDistributer.distributeTokens(acquireNewTokens(principal, keytab))
          } catch {
            case e: Exception =>
              // Log the error and try to write new tokens back in an hour
              logWarning("Failed to write out new credentials to HDFS, will try again in an " +
                "hour! If this happens too often tasks will fail.", e)
              delegationTokenRenewer.schedule(this, 1, TimeUnit.HOURS)
              return
          }
          scheduleRenewal(this)
        }
      }
    // Schedule update of credentials. This handles the case of updating the tokens right now
    // as well, since the renenwal interval will be 0, and the thread will get scheduled
    // immediately.
    scheduleRenewal(driverTokenRenewerRunnable)
  }

  private def acquireNewTokens(principal: String, keytab: String): Credentials = {

    // HACK:
    // HDFS will not issue new delegation tokens, if the Credentials object
    // passed in already has tokens for that FS even if the tokens are expired (it really only
    // checks if there are tokens for the service, and not if they are valid). So the only real
    // way to get new tokens is to make sure a different Credentials object is used each time to
    // get new tokens and then the new tokens are copied over the the current user's Credentials.
    // So:
    // - we login as a different user and get the UGI
    // - use that UGI to get the tokens (see doAs block below)
    // - copy the tokens over to the current user's credentials (this will overwrite the tokens
    // in the current user's Credentials object for this FS).
    // The login to KDC happens each time new tokens are required, but this is rare enough to not
    // have to worry about (like once every day or so). This makes this code clearer than having
    // to login and then relogin every time (the HDFS API may not relogin since we don't use this
    // UGI directly for HDFS communication.
    logInfo(s"Attempting to login to KDC using principal: $principal")
    val keytabLoggedInUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
    logInfo("Successfully logged into KDC.")
    val tempCreds = keytabLoggedInUGI.getCredentials
    keytabLoggedInUGI.doAs(new PrivilegedExceptionAction[Void] {
      // Get a copy of the credentials
      override def run(): Void = {
        val nns = YarnSparkHadoopUtil.get.getNameNodesToAccess(sparkConf) +
          credentialsFilePath.getParent
        hadoopUtil.obtainTokensForNamenodes(nns, freshHadoopConf, tempCreds)
        hadoopUtil.obtainTokenForHiveMetastore(sparkConf, freshHadoopConf, tempCreds)
        hadoopUtil.obtainTokenForHBase(sparkConf, freshHadoopConf, tempCreds)
        hadoopUtil.obtainTokenForHiveServer2(sparkConf, freshHadoopConf, tempCreds)
        null
      }
    })
    // Add the temp credentials back to the original ones.
    UserGroupInformation.getCurrentUser.addCredentials(tempCreds)
    tempCreds
  }

  def stop(): Unit = {
    delegationTokenRenewer.shutdown()
  }
}
