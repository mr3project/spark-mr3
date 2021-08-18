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

import com.datamonad.mr3.DAGAPI
import com.datamonad.mr3.DAGAPI.{ContainerGroupProto, DAGProto, DaemonVertexProto, VertexProto}
import com.google.protobuf.ByteString
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.Credentials
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.TaskDescription

private[mr3] object DAG {
  private val CONTAINER_GROUP_PRIORITY = 0

  private val WORKER_VERTEX_NAME = "SparkWorkerVertex"
  private val WORKER_VERTEX_PROCESSOR_PREFIX = "spark.worker."

  private val CONTAINER_GROUP_NAME_PREFIX = "SparkContainerGroup-"

  private val DAEMON_VERTEX_NAME = "SparkDaemonVertex"
  private val DAEMON_VERTEX_PROCESSOR_NAME = "spark.daemon"

  def createDag(
      applicationId: String,
      priority: Int,
      stageId: Int,
      taskDescriptions: Array[TaskDescription],
      taskLocationHints: Array[Seq[String]],
      driverUrl: String,
      sparkConf: SparkConf,
      dagConf: DAGAPI.ConfigurationProto.Builder,
      credentials: Option[Credentials],
      ioEncryptionKey: Option[Array[Byte]]): DAGProto = {
    val containerGroup = getContainerGroupBuilder(applicationId = applicationId, driverUrl = driverUrl, sparkConf, ioEncryptionKey)
    val workerVertex = getWorkerVertexBuilder(applicationId, stageId, taskDescriptions, taskLocationHints, sparkConf)

    val dagProtoBuilder = DAGProto.newBuilder
      .setName(s"${applicationId}_$stageId")
      .addVertices(workerVertex)
      .addContainerGroups(containerGroup)
      .setDagConf(dagConf)
      .setJobPriority(priority)

    credentials.foreach { creds =>
      val dob = new DataOutputBuffer
      creds.writeTokenStorageToStream(dob)
      val buffer = ByteBuffer.wrap(dob.getData, 0, dob.getLength)
      dagProtoBuilder.setCredentials(ByteString.copyFrom(buffer))
    }

    dagProtoBuilder.build
  }

  def getContainerGroupBuilder(
      applicationId: String,
      driverUrl: String,
      sparkConf: SparkConf,
      ioEncryptionKey: Option[Array[Byte]]): ContainerGroupProto.Builder = {
    val daemonVertex = getDaemonVertexBuilder(driverUrl, sparkConf, ioEncryptionKey)

    // use sparkConf because containerGroupResource is fixed for all DAGs
    val nonNativeMemoryMb = Utils.getContainerGroupNonNativeMemoryMb(sparkConf)
    val nativeMemoryMb = Utils.getOffHeapMemoryMb(sparkConf)
    val containerGroupResource = getResource(
        cores = Utils.getContainerGroupCores(sparkConf),
        memoryMb = nonNativeMemoryMb + nativeMemoryMb)
    val containerConfig = DAGAPI.ContainerConfigurationProto.newBuilder
      .setResource(containerGroupResource)
      .setNativeMemoryMb(nativeMemoryMb)

    ContainerGroupProto.newBuilder
      .setName(getContainerGroupName(applicationId))
      .setPriority(CONTAINER_GROUP_PRIORITY)
      .setContainerConfig(containerConfig)
      .addDaemonVertices(daemonVertex)
  }

  private def getDaemonVertexBuilder(
      driverUrl: String,
      sparkConf: SparkConf,
      ioEncryptionKey: Option[Array[Byte]]): DaemonVertexProto.Builder = {
    val sparkConfProto = DAGAPI.ConfigurationProto.newBuilder
    sparkConf.getAll.foreach { case (key, value) =>
      val keyValueProto = DAGAPI.KeyValueProto.newBuilder
        .setKey(key)
        .setValue(value)
      sparkConfProto.addConfKeyValues(keyValueProto)
    }
    ioEncryptionKey.foreach { ioEncryptionKey =>
      val keyValueProto = DAGAPI.KeyValueProto.newBuilder
        .setKey("spark.mr3.io.encryption.key")
        .setValueBytes(ByteString.copyFrom(ioEncryptionKey))
      sparkConfProto.addConfKeyValues(keyValueProto)
    }

    val daemonConfigProto = DAGAPI.DaemonConfigProto.newBuilder
      .setAddress(driverUrl)
      .setConfig(sparkConfProto)

    val daemonConfigByteString = daemonConfigProto.build.toByteString
    val daemonVertexManagerPayload = DAGAPI.UserPayloadProto.newBuilder
      .setPayload(daemonConfigByteString)

    val daemonVertexManagerPlugin = DAGAPI.EntityDescriptorProto.newBuilder
      .setClassName(DAEMON_VERTEX_NAME)
      .setUserPayload(daemonVertexManagerPayload)

    val daemonVertexProcessor = DAGAPI.EntityDescriptorProto.newBuilder
      .setClassName(DAEMON_VERTEX_PROCESSOR_NAME)
    val daemonVertexResource = getResource(cores = 0, memoryMb = 0)

    DaemonVertexProto.newBuilder
      .setName(DAEMON_VERTEX_NAME)
      .setProcessor(daemonVertexProcessor)
      .setResource(daemonVertexResource)
      .setVertexManagerPlugin(daemonVertexManagerPlugin)
  }

  private def getWorkerVertexBuilder(
      applicationId: String,
      stageId: Int,
      taskDescriptions: Array[TaskDescription],
      taskLocationHints: Array[Seq[String]],
      sparkConf: SparkConf): VertexProto.Builder = {
    val numTasks = taskDescriptions.length

    def taskDescriptionToEntityDescriptorProto(
        taskDescription: TaskDescription): DAGAPI.EntityDescriptorProto.Builder = {
      val taskId = taskDescription.taskId
      val serializedTaskDescription = TaskDescription.encode(taskDescription)

      val className = s"$stageId-$taskId"
      val userPayload = DAGAPI.UserPayloadProto.newBuilder
        .setPayload(ByteString.copyFrom(serializedTaskDescription))

      DAGAPI.EntityDescriptorProto.newBuilder
        .setClassName(className)
        .setUserPayload(userPayload)
    }

    val processorSetProto = DAGAPI.ProcessorSetProto.newBuilder.setNumProcessors(numTasks)
    var i = 0
    while (i < numTasks) {
      val taskDesc = taskDescriptions(i)
      val processor = taskDescriptionToEntityDescriptorProto(taskDesc)
      processorSetProto.addProcessors(processor)
      i += 1
    }

    val processorSetProtoByteString = processorSetProto.build.toByteString
    val workerVertexManagerPayload = DAGAPI.UserPayloadProto.newBuilder
      .setPayload(processorSetProtoByteString)

    val workerVertexManager = DAGAPI.EntityDescriptorProto.newBuilder
      .setClassName(WORKER_VERTEX_NAME)
      .setUserPayload(workerVertexManagerPayload)

    val workerVertexProcessor = DAGAPI.EntityDescriptorProto.newBuilder
      .setClassName(WORKER_VERTEX_PROCESSOR_PREFIX + stageId)

    // use sparkConf because workerVertexResource is fixed for all DAGs
    val workerVertexResource = getResource(
        cores = Utils.getTaskCores(sparkConf),
        memoryMb = Utils.getTaskMemoryMb(sparkConf))

    val vertexProtoBuilder = VertexProto.newBuilder
      .setName(WORKER_VERTEX_NAME)
      .setPriority(stageId)
      .setContainerGroupName(getContainerGroupName(applicationId))
      .setNumTasks(numTasks)
      .setProcessor(workerVertexProcessor)
      .setVertexManagerPlugin(workerVertexManager)
      .setResource(workerVertexResource)

    i = 0
    while (i < numTasks) {
      val taskLocationHint = taskLocationHints(i)
      val taskLocationHintProto = DAGAPI.TaskLocationHintProto.newBuilder
      taskLocationHint.foreach { taskLocationHintProto.addHosts }
      taskLocationHintProto.setAnyHost(true)
      vertexProtoBuilder.addTaskLocationHints(taskLocationHintProto)
      i += 1
    }

    vertexProtoBuilder
  }

  private def getContainerGroupName(applicationId: String): String =
    CONTAINER_GROUP_NAME_PREFIX + applicationId

  private def getResource(cores: Int, memoryMb: Int, coreDivisor: Int = 1): DAGAPI.ResourceProto.Builder = {
    DAGAPI.ResourceProto.newBuilder
      .setVirtualCores(cores)
      .setMemoryMb(memoryMb)
      .setCoreDivisor(coreDivisor)
  }
}
