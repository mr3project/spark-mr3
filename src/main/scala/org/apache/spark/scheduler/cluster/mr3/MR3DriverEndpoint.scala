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

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcAddress, RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.{ExecutorLossReason, ExecutorResourceInfo}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RemoveExecutor, StatusUpdate, StopDriver, UpdateDelegationTokens}
import org.apache.spark.scheduler.cluster.ExecutorData

import scala.collection.mutable

private[mr3] class MR3DriverEndpoint(override val rpcEnv: RpcEnv, backend: MR3BackendDriverEndpointInterface)
    extends IsolatedRpcEndpoint with Logging {

  // By default, IsolatedRpcEndpoint runs in a single thread.
  private[this] val rpcAddressToExecutorId = new mutable.HashMap[RpcAddress, String]
  private[this] val executorDataMap = new mutable.HashMap[String, ExecutorData]

  override def receive: PartialFunction[Any, Unit] = {
    case RemoveExecutor(executorId, reason) =>
      removeExecutor(executorId, reason)
    case su @ StatusUpdate(executorId, taskId, state, data, resources) =>
      logDebug(s"Receive StatusUpdate: $su")
      if (executorDataMap contains executorId) {
        val hostname = executorDataMap(executorId).executorHost
        backend.statusUpdate(executorId = executorId, hostname = hostname, taskId, state, data.value)
      } else {
        logWarning(s"MR3Backend receives a StatusUpdate message from unseen executor: $executorId. " +
          s"Ignore the message(TID: $taskId, state: $state")
      }
    case UpdateDelegationTokens(tokens) =>
      backend.updateDelegationTokenManager(tokens)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterExecutor(executorId, executorRef, hostname, cores, logUrl, attributes, resources, resourceProfileId) =>
      // hostName = InetAddress.getLocalHost.getHostAddress in SparkRuntimeEnv.createExecutorBackend()
      if (executorDataMap contains executorId) {
        logWarning(s"Ignore duplicate RegisterExecutor event: $executorId")
        context.sendFailure(new IllegalStateException(s"Duplicate executor: $executorId"))
      } else {
        val senderRpcAddress = context.senderAddress
        val data = createExecutorData(hostname, senderRpcAddress)
        rpcAddressToExecutorId.put(senderRpcAddress, executorId)
        executorDataMap.put(executorId, data)
        val currentNumExecutors = backend.numExecutors.incrementAndGet()
        logInfo(s"Register executor: $executorId, $currentNumExecutors")

        backend.registerExecutor(executorId = executorId, hostname = hostname, data)
        context.reply(true)
      }
    case StopDriver =>
      logInfo(s"Receive StopDriver")
      context.reply(true)
      stop()
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    rpcAddressToExecutorId.get(remoteAddress).foreach { executorId =>
      logWarning(s"RPC disconnected: $executorId")
      removeExecutor(executorId, new ExecutorLossReason("RPC disconnected"))
    }
  }

  private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    if (!(executorDataMap contains executorId)) {
      // SPARK-15262
      backend.removeExecutorFromBlockManagerMaster(executorId)
      logInfo(s"Asked to remove non-existent executor $executorId")
    } else {
      val data = executorDataMap(executorId)
      rpcAddressToExecutorId -= data.executorAddress
      executorDataMap -= executorId
      backend.removeExecutor(executorId, reason)
      val currentNumExecutors = backend.numExecutors.decrementAndGet()
      logInfo(s"Remove executor: $executorId, $currentNumExecutors")
    }
  }

  private val emptyStringMap = Map.empty[String, String]
  private val emptyResourceMap = Map.empty[String, ExecutorResourceInfo]

  private def createExecutorData(hostname: String, address: RpcAddress): ExecutorData = {
    new ExecutorData(
      executorEndpoint = null,
      address,
      executorHost = hostname,
      freeCores = 0,
      totalCores = 0,
      logUrlMap = emptyStringMap,
      attributes = emptyStringMap,
      resourcesInfo = emptyResourceMap,
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
      registrationTs = System.currentTimeMillis())
  }
}
