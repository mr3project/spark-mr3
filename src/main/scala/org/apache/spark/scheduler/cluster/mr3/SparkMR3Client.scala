/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.mr3

import com.datamonad.mr3.DAGAPI
import com.datamonad.mr3.api.client.{DAGClient, MR3SessionClient, SessionStatus}
import com.datamonad.mr3.api.common.{MR3Conf, MR3Constants, MR3Exception, Utils => MR3Utils}
import com.datamonad.mr3.common.{Utils => MR3CommonUtils}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.TaskDescription

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

private[mr3] class SparkMR3Client(
    mr3Conf: MR3Conf,
    sparkConf: SparkConf,
    sparkApplicationId: String,
    driverUrl: String,
    ioEncryptionKey: Option[Array[Byte]]) extends Logging {

  private[this] val mr3Client = MR3SessionClient(sparkApplicationId, mr3Conf,
      initAmCredentials = None, initAmLocalResources = Map.empty,
      additionalSessionCredentials = None, additionalSessionLocalResources = Map.empty)

  // true if mr3Client.close() should be called
  // assume that start() and stop() are called from the same thread, so no @volatile
  private[this] var mr3ClientReady = false

  private[this] val numDagsSubmitted = new AtomicInteger(0)

  @throws[InterruptedException]
  @throws[SparkException]
  def start(): Unit = {
    try {
      if (sparkConf.getOption(SparkMR3Config.SPARK_MR3_APP_ID).isDefined) {
        val appIdStr = sparkConf.get(SparkMR3Config.SPARK_MR3_APP_ID)
        connectExistingMR3Client(appIdStr)
      } else {
        startNewMR3Client()
      }
    } catch {
      case ex: MR3Exception =>
        throw new SparkException("Failed to start MR3Client", ex)
    }

    try {
      waitForDAGAppMaster()
    } catch {
      case ex @ (_: InterruptedException | _: SparkException) =>
        logError(s"Failed to connect to MR3 DAGAppMaster", ex)
        mr3Client.close()
        throw ex
    }

    mr3ClientReady = true

    val appId = mr3Client.getApplicationId.toString
    logInfo("********************************************************")
    logInfo(s"MR3 Application ID: $appId")
    logInfo("********************************************************")
  }

  @throws[MR3Exception]
  private def connectExistingMR3Client(appIdStr: String): Unit = {
    // MR3Utils.getAppId() calls require{}
    val maybeAppId = Try{ MR3Utils.getAppId(appIdStr) }.toOption
    if (maybeAppId.isDefined) {
      val appId = maybeAppId.get
      logInfo(s"Trying to connect to an existing MR3 DAGAppMaster with $appId")
      mr3Client.connect(appId)
    } else {
      logWarning(s"Application ID $appIdStr is invalid")
      startNewMR3Client()
    }
  }

  @throws[MR3Exception]
  private def startNewMR3Client(): Unit = {
    logInfo(s"Trying to create a new MR3 DAGAppMaster")
    mr3Client.start()
  }

  @throws[InterruptedException]
  @throws[SparkException]
  private def waitForDAGAppMaster(): Unit = {
    val waitPeriodMs = 1000L
    val timeoutMs = sparkConf.getLong(
        SparkMR3Config.SPARK_MR3_CLIENT_CONNECT_TIMEOUT_MS,
        SparkMR3Config.SPARK_MR3_CLIENT_CONNECT_TIMEOUT_MS_DEFAULT)
    val endTimeoutTimeMs = System.currentTimeMillis() + timeoutMs
    var isReady = isMR3Ready()
    while (!isReady && System.currentTimeMillis() < endTimeoutTimeMs) {
      logInfo(s"MR3 DAGAppMaster is not ready. Waiting for $waitPeriodMs milliseconds...")
      this.synchronized {
        this.wait(waitPeriodMs)
      }
      isReady = isMR3Ready()
    }
    if (!isReady) {
      throw new SparkException("Time out while waiting to connect to MR3 DAGAppMaster")
    }
  }

  @throws[SparkException]
  private def isMR3Ready(): Boolean = {
    val maybeSessionStatus = Try{ mr3Client.getSessionStatus() }.toOption

    val isReady = maybeSessionStatus.map {
      case SessionStatus.Initializing => false
      case SessionStatus.Ready | SessionStatus.Running => true
      case SessionStatus.Shutdown =>
        throw new SparkException("MR3 DAGAppMaster stopped while Spark is initializing")
    }

    isReady.getOrElse(false)
  }

  @throws[SparkException]
  def stop(): Unit = {
    if (mr3ClientReady) {
      try {
        if (numDagsSubmitted.get() > 0) {
          submitSentinelDagToStopCrossDagReuse()
        }
        if (mr3Conf.getMasterMode != MR3Constants.MR3_MASTER_MODE_KUBERNETES &&
            !sparkConf.getBoolean(SparkMR3Config.SPARK_MR3_KEEP_AM, SparkMR3Config.SPARK_MR3_KEEP_AM_DEFAULT)) {
          mr3Client.shutdownAppMasterToTerminateApplication()
        }
        mr3Client.close()
        mr3ClientReady = false
      } catch {
        case ex: MR3Exception =>
          val msg = "Failed to stop MR3Client gracefully"
          logError(msg, ex)
          throw new SparkException(msg, ex)
      }
    }
  }

  @throws[MR3Exception]
  private def submitSentinelDagToStopCrossDagReuse(): Unit = {
    val amLocalResources = Map.empty[String, LocalResource]
    val amCredentials = None
    val dagProto = SentinelDAG.createDag(applicationId = sparkApplicationId, driverUrl = driverUrl, sparkConf)
    val dagClient = mr3Client.submitDag(amLocalResources, amCredentials, dagProto)
    val result = dagClient.waitForCompletion()
    result match {
      case Some(dagStatus) =>
        logInfo(s"The last sentinel DAG is completed: $dagStatus")
      case None =>
        logWarning("Failed to wait until the last sentinel DAG is completed")
    }
  }

  // Ideally MR3Exception should be wrapped in SparkException, but it is handled in MR3Backend.submitTasks().
  @throws[MR3Exception]
  def submitTasks(
      priority: Int,
      stageId: Int,
      taskDescriptions: Array[TaskDescription],
      taskLocationHints: Array[Seq[String]],
      vertexCommonTaskDescription: TaskDescription,
      sparkConf: SparkConf,
      dagConf: DAGAPI.ConfigurationProto.Builder,
      credentials: Option[Credentials]): DAGClient = {
    val modifiedTaskLocationHints = mr3Conf.getAmWorkerMode match {
      case MR3Constants.MR3_WORKER_MODE_YARN =>
        taskLocationHints.map { hints =>
          hints.map { address => MR3CommonUtils.hostAddressToHostName(address) }
        }
      case MR3Constants.MR3_WORKER_MODE_KUBERNETES =>
        taskLocationHints.map { hints =>
          hints.flatMap { host => MR3CommonUtils.hostNameToHostAddress(host) }
        }
      case _ =>
        taskLocationHints
    }

    val amCredentials = None
    val amLocalResources = Map.empty[String, LocalResource]
    val dagProto = DAG.createDag(
        applicationId = sparkApplicationId, priority = priority, stageId = stageId,
        taskDescriptions, modifiedTaskLocationHints, driverUrl = driverUrl, vertexCommonTaskDescription,
        sparkConf, dagConf, credentials, ioEncryptionKey)

    val dagClient = mr3Client.submitDag(amLocalResources, amCredentials, dagProto)
    numDagsSubmitted.incrementAndGet()
    dagClient
  }
}
