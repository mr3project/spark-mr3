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

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean

import com.datamonad.mr3.api.client.{DAGClient, DAGStatus}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance

import org.apache.spark.util.{Utils => SparkUtils}

class DAGStatusChecker(
    name: String,
    dagClient: DAGClient,
    shouldKillDag: AtomicBoolean,
    mr3TaskSetManager: DAGStatusCheckerInterface,
    backend: MR3Backend)
  extends Logging {

  private def checkPeriodMs: Int = backend.dagStatusCheckerPeriodMs
  private def dagStatusCheckerExecutor = backend.dagStatusCheckerExecutor
  private def env: SparkEnv = backend.sc.env

  // use 'def' because serializer is used only when DAGStatus.state becomes Failed or Killed
  private def serializer: SerializerInstance = env.closureSerializer.newInstance()

  // do not call dagClient.close() because we use MR3SessionClient
  def run(): Unit = {
    try {
      dagStatusCheckerExecutor.execute(() => SparkUtils.logUncaughtExceptions {
        var isFinished: Boolean = false
        var dagStatus: Option[DAGStatus] = None
        while (!isFinished && !shouldKillDag.get()) {
          dagStatus = dagClient.getDagStatusWait(false, checkPeriodMs)
          isFinished = mr3TaskSetManager.checkStatus(dagStatus, serializer)
        }
        logInfo(s"DAGStatusChecker stopped calling checkStatus(): $name, ${dagStatus.map(_.state)}, $shouldKillDag")

        if (!isFinished) {
          assert { shouldKillDag.get() }  // because once set to true, shouldKillDag is never unset
          logInfo(s"MR3TaskSetManager in Killed or Stopped, so trying to kill DAG: $name")
          dagClient.tryKillDag()
        }

        // keep calling DAGClient.getDagStatusWait() to call DAGClientHandler.acknowledgeDagFinished()
        while (dagStatus.nonEmpty && !dagStatus.get.isFinished) {
          dagStatus = dagClient.getDagStatusWait(false, checkPeriodMs)
        }
        logInfo(s"DAGStatusChecker thread finished: $name")
      })
    } catch {
      case _: RejectedExecutionException if env.isStopped =>
        // ignore it
    }
  }
}
