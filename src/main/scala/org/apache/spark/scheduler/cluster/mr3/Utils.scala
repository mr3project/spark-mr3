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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{CPUS_PER_TASK, EXECUTOR_CORES, EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD, MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}

object Utils {
  def getTaskMemoryMb(sparkConf: SparkConf): Int = {
    val numTasksPerContainerWorker = getContainerGroupCores(sparkConf) / getTaskCores(sparkConf)
    require({ numTasksPerContainerWorker > 0 },
      s"Number of Spark tasks per ContainerWorker (spark.executor.cores / spark.tasks.cpus) should be > 0: $numTasksPerContainerWorker")
    val containerGroupNonNativeMemoryMB = getContainerGroupNonNativeMemoryMb(sparkConf)
    containerGroupNonNativeMemoryMB / numTasksPerContainerWorker
  }

  def getContainerGroupCores(sparkConf: SparkConf): Int = {
    val cores = sparkConf.get(EXECUTOR_CORES)
    require({ cores > 0 }, s"Number of cores per ContainerWorker (spark.executor.cores) should be > 0: $cores")
    cores
  }

  def getTaskCores(sparkConf: SparkConf): Int = {
    val cores = sparkConf.get(CPUS_PER_TASK)
    require({ cores > 0 }, s"Number of cores per Spark task (spark.tasks.cpus) should be > 0: $cores")
    cores
  }

  def getContainerGroupNonNativeMemoryMb(sparkConf: SparkConf): Int = {
    val memory = getExecutorMemoryMb(sparkConf) + getExecutorMemoryOverheadMb(sparkConf)
    require({ memory > 0 }, s"Memory per ContainerWorker (spark.executor.memory + spark.executor.memoryOverhead) should be > 0: $memory")
    memory
  }

  def getContainerHeapFraction(sparkConf: SparkConf): Double = {
    val executorMemory = getExecutorMemoryMb(sparkConf)
    val executorMemoryOverhead = getExecutorMemoryOverheadMb(sparkConf)
    executorMemory.toDouble / (executorMemory + executorMemoryOverhead)
  }

  def getOffHeapMemoryMb(sparkConf: SparkConf): Int = {
    val offHeapMemoryEnabled = sparkConf.get(MEMORY_OFFHEAP_ENABLED)
    if (offHeapMemoryEnabled) {
      val nativeMemory = sparkConf.get(MEMORY_OFFHEAP_SIZE)
      toMegaByte(nativeMemory)
    } else
      0
  }

  private def getExecutorMemoryOverheadMb(sparkConf: SparkConf): Int = {
    // To remove dependency on Spark-Yarn, we define two constants: MEMORY_OVERHEAD_FACTOR, MEMORY_OVERHEAD_MIN
    // from org.apache.spark.deploy.yarn.YarnSparkHadoopUtil.MEMORY_OVERHEAD_FACTOR
    // from org.apache.spark.deploy.k8s.Config.MEMORY_OVERHEAD_FACTOR = "spark.kubernetes.memoryOverheadFactor"
    val MEMORY_OVERHEAD_FACTOR = 0.10
    // from org.apache.spark.deploy.yarn.YarnSparkHadoopUtil.MEMORY_OVERHEAD_MIN
    // from org.apache.spark.deploy.k8s.Constants.MEMORY_OVERHEAD_MIN_MIB
    val MEMORY_OVERHEAD_MIN = 384L

    // The formula is based on:
    //   org.apache.spark.deploy.yarn.YarnAllocator.memoryOverhead
    //   org.apache.spark.deploy.k8s.feature.BasicExecutorFeatureStep.memoryOverheadMiB
    sparkConf
      .get(EXECUTOR_MEMORY_OVERHEAD)
      .getOrElse(math.max(
          (MEMORY_OVERHEAD_FACTOR * getExecutorMemoryMb(sparkConf)).toLong,
          MEMORY_OVERHEAD_MIN)).toInt
  }

  private def getExecutorMemoryMb(sparkConf: SparkConf): Int = {
    sparkConf.get(EXECUTOR_MEMORY).toInt
  }

  private def toMegaByte(bytes: Long): Int = {
    val megaByteUnit = 1024L * 1024L
    if (bytes % megaByteUnit == 0)
      (bytes / megaByteUnit).toInt
    else
      (bytes / megaByteUnit).toInt + 1
  }
}
