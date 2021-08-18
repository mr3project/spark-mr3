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

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import org.apache.spark.{InternalAccumulator, SparkEnv, TaskFailedReason, TaskKilled, TaskState, UnknownReason}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.mr3.SparkMR3TaskID.MR3TaskID
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult, TaskResult}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils => SparkUtils}

import scala.util.control.NonFatal

private[mr3] class MR3TaskResultGetter(env: SparkEnv, numThreads: Int) extends Logging {

  private val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(numThreads, "MR3-task-result-getter")

  private val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      env.closureSerializer.newInstance()
    }
  }

  private val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      env.serializer.newInstance()
    }
  }

  def fetchSuccessfulTask(
      taskSetMgr: MR3TaskSetManager,
      mr3TaskId: MR3TaskID,
      serializedData: ByteBuffer,
      executorId: String,
      host: String): Unit = {
    getTaskResultExecutor.execute(() => SparkUtils.logUncaughtExceptions {
      try {
        val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
          case directResult: DirectTaskResult[_] =>
            if (taskSetMgr.canFetchMoreResults(serializedData.limit())) {
              directResult.value(taskResultSerializer.get())
              (directResult, serializedData.limit())
            } else
              (null, 0)
          case IndirectTaskResult(blockId, size) =>
            taskSetMgr.fetchTaskResult(mr3TaskId, executorId, host)
            if (taskSetMgr.canFetchMoreResults(size)) {
              val serializedTaskResult = env.blockManager.getRemoteBytes(blockId)
              if (serializedTaskResult.isDefined) {
                val deserializedResult =
                    serializer.get().deserialize[DirectTaskResult[_]](serializedTaskResult.get.toByteBuffer)
                deserializedResult.value(taskResultSerializer.get())
                env.blockManager.master.removeBlock(blockId)
                (deserializedResult, size)
              } else
                (null, 0)
            } else
              (null, 0)
        }
        // do not exploit size (based on the following assert{}) because we can instead check result
        //   assert { !(size == 0) || result == null }
        if (result == null) {
          taskSetMgr.taskFailed(
              mr3TaskId, TaskState.KILLED, TaskKilled("Tasks result size has exceeded maxResultSize"),
              executorId = executorId, host = host)
        }
        if (result != null) {
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert({ acc.sum == 0L }, "task result size should not have been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }
          taskSetMgr.taskSucceeded(mr3TaskId, result, executorId = executorId, host = host)
        }
      } catch {
        case cnf: ClassNotFoundException =>
          val loader = Thread.currentThread.getContextClassLoader
          val msg = "ClassNotFound with classloader: " + loader
          taskSetMgr.kill(msg, None)
        case NonFatal(ex) =>
          logError("Exception while getting task result", ex)
          val msg = "Exception while getting task result: %s".format(ex)
          taskSetMgr.kill(msg, None)
      }
    })
  }

  def failTaskWithFailedReason(
      taskSetMgr: MR3TaskSetManager,
      mr3TaskId: MR3TaskID,
      taskState: TaskState,
      serializedData: ByteBuffer,
      executorId: String,
      host: String): Unit = {
    var reason: TaskFailedReason = UnknownReason
    try {
      getTaskResultExecutor.execute(() => SparkUtils.logUncaughtExceptions {
        val loader = SparkUtils.getContextOrSparkClassLoader
        try {
          if (serializedData != null && serializedData.limit() > 0) {
            reason = serializer.get().deserialize[TaskFailedReason](serializedData, loader)
          }
        } catch {
          case _: ClassNotFoundException =>
            logError("Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
          case _: Exception => // No-op
        } finally {
          taskSetMgr.taskFailed(mr3TaskId, taskState, reason, executorId = executorId, host = host)
        }
      })
    } catch {
      case _: RejectedExecutionException if env.isStopped =>
        // ignore it
    }
  }

  def stop(): Unit = {
    getTaskResultExecutor.shutdownNow()
  }
}
