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
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import javax.annotation.concurrent.GuardedBy
import com.datamonad.mr3.api.common.{MR3Conf, MR3ConfBuilder, MR3Constants}
import com.datamonad.mr3.DAGAPI
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.{BarrierCoordinator, MapOutputTrackerMaster, SparkContext, SparkException, TaskState}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{BARRIER_SYNC_TIMEOUT, CPUS_PER_TASK, EXECUTOR_CORES}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.{DAGScheduler, ExecutorCacheTaskLocation, ExecutorLossReason, HDFSCacheTaskLocation, HostTaskLocation, Pool, SchedulerBackend, SchedulingMode, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, TaskDescription, TaskLocation, TaskScheduler, TaskSet, TaskSetManager}
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.StopDriver
import org.apache.spark.scheduler.cluster.ExecutorData
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ShutdownHookManager, ThreadUtils}

import java.util.Properties
import scala.collection.mutable
import scala.util.control.NonFatal

private[mr3] object SparkMR3TaskID {

  // SparkTaskID: unique for every Spark Task
  // MR3TaskID == 'SparkTaskAttemptID': unique for every Spark Task + attemptNumber (similarly to TaskAttemptID in MR3)
  // *** note that MR3TaskID, not SparkTaskID, corresponds to 'taskId: Long' in Spark ***

  type SparkTaskID = Long
  type MR3TaskID = Long     // | SparkTaskID (56bit) | TaskAttemptID.id (8bit) |

  def toSparkTaskId(mr3TaskId: MR3TaskID): SparkTaskID = {
    mr3TaskId >> 8
  }

  def toMr3TaskId(sparkTaskId: SparkTaskID, taskAttemptIdId: Int): MR3TaskID = {
    assert { taskAttemptIdId < (1 << 8) }
    (sparkTaskId << 8) + taskAttemptIdId
  }

  def toTaskAttemptIdId(mr3TaskId: MR3TaskID): Int = {
    (mr3TaskId & 0xFF).toInt
  }

  def isTaskAttempt(sparkTaskId: SparkTaskID, mr3TaskId: MR3TaskID): Boolean = {
    sparkTaskId == toSparkTaskId(mr3TaskId)
  }
}

private[mr3] trait MR3BackendDriverEndpointInterface {
  import org.apache.spark.scheduler.cluster.mr3.SparkMR3TaskID.MR3TaskID

  def numExecutors: AtomicInteger
  def statusUpdate(
      executorId: String,
      hostname: String,
      taskId: MR3TaskID,
      state: TaskState,
      data: ByteBuffer): Unit
  def registerExecutor(executorId: String, hostname: String, data: ExecutorData): Unit
  def removeExecutor(executorId: String, executorLossReason: ExecutorLossReason): Unit
  def removeExecutorFromBlockManagerMaster(executorId: String): Unit
  def updateDelegationTokenManager(tokens: Array[Byte]): Unit
}

class MR3Backend(val sc: SparkContext) extends TaskScheduler
    with SchedulerBackend
    with MR3BackendDriverEndpointInterface
    with Logging {

  import SparkMR3TaskID.{MR3TaskID, SparkTaskID}

  def this(sc: SparkContext, masterURL: String) = this(sc)

  def sparkConf = sc.conf   // immutable, so common to all DAGs
  private val ENDPOINT_NAME = "MR3Backend"

  // do not use System.currentTimeMillis because two Spark drivers sharing DAGAppMaster may be launched almost simultaneously
  private[this] val appId = "spark-mr3-app-" + java.util.UUID.randomUUID.toString.substring(0, 8)

  // from TaskScheduler
  override val schedulingMode: SchedulingMode = SchedulingMode.FIFO
  override val rootPool: Pool = new Pool("MR3 Pool", schedulingMode, 0, 0)

  // effectively immutable because setDAGScheduler() is called before submitTasks() is called
  private[mr3] var dagScheduler: DAGScheduler = _

  // accessed by DAGScheduler thread only, so no need to guard with a lock
  private[this] val taskSerializer = sc.env.closureSerializer.newInstance()
  private[this] var nextTaskId: SparkTaskID = 0L

  // lock for guarding taskSetManagersByTaskId[] and taskSetManagersByStageIdAttempt[]
  private[this] val taskSetManagersLock = new AnyRef

  // updated by DAGScheduler, MR3TaskResultGetter, and DAGStatusChecker threads
  // read by MR3DriverEndpoint and HeartbeatReceiver threads
  @GuardedBy("taskSetManagersLock")
  private[this] val taskSetManagersByTaskId = new mutable.HashMap[SparkTaskID, MR3TaskSetManager]
  assert { taskSetManagersByTaskId forall { case (sparkTaskId, tsm) => tsm.containSparkTaskId(sparkTaskId) } }

  // updated by DAGScheduler, MR3TaskResultGetter, and DAGStatusChecker threads
  // read by DAGScheduler thread
  @GuardedBy("taskSetManagersLock")
  private[this] val taskSetManagersByStageIdAttempt = new mutable.HashMap[(Int, Int), MR3TaskSetManager]

  // for taskSetManagersByTaskId and taskSetManagersByStageIdAttempt
  assert { taskSetManagersByStageIdAttempt.values forall taskSetManagersByTaskId.values.toSeq.contains }
  assert { taskSetManagersByTaskId.values forall taskSetManagersByStageIdAttempt.values.toSeq.contains }

  // updated by MR3DriverEndPoint
  val numExecutors = new AtomicInteger(0)
  private[this] val coresPerExecutor: Int = sparkConf.get(EXECUTOR_CORES)
  private[this] val cpusPerTask: Int = sparkConf.get(CPUS_PER_TASK)
  require({ coresPerExecutor >= cpusPerTask && cpusPerTask > 0 },
    s"Spark configuration invalid: $EXECUTOR_CORES = $coresPerExecutor, $CPUS_PER_TASK = $cpusPerTask")

  // TODO: blocking operation???
  private[this] val driverEndpoint = sc.env.rpcEnv.setupEndpoint(
      ENDPOINT_NAME, new MR3DriverEndpoint(sc.env.rpcEnv, backend = this))

  private[this] val taskResultGetter = new MR3TaskResultGetter(
      sc.env, sparkConf.getInt("spark.resultGetter.threads", 4))

  private[this] val credentials = new AtomicReference[Credentials]

  private[this] var delegationTokenManager: HadoopDelegationTokenManager = _

  private[this] var barrierCoordinator: BarrierCoordinator = _

  private[this] val mr3Conf = {
    val builder = new MR3ConfBuilder(true)
      .addResource(sc.hadoopConfiguration)
      .set(MR3Conf.MR3_RUNTIME, "spark")
      .setBoolean(MR3Conf.MR3_AM_SESSION_MODE, true)
      .set(MR3Conf.MR3_CONTAINER_MAX_JAVA_HEAP_FRACTION, Utils.getContainerHeapFraction(sparkConf).toString)
    sparkConf.getAll.foreach { case (key, value) =>
      if (key.startsWith(SparkMR3Config.SPARK_MR3_CONFIG_PREFIX) &&
          !SparkMR3Config.RESERVED_CONFIG_KEYS.contains(key)) {
        builder.set(key.replace("spark.", ""), value)
      }
    }
    val containerLaunchEnv = sparkConf.getExecutorEnv.map{ case (key, value) => key + "=" + value }.mkString(",")
    if (containerLaunchEnv.nonEmpty) {
      builder.set(MR3Conf.MR3_CONTAINER_LAUNCH_ENV, containerLaunchEnv)
    }
    builder.build
  }
  // In local worker mode, only a single ContainerWorker/executor should be created.
  require({ !(mr3Conf.getAmWorkerMode == MR3Constants.MR3_WORKER_MODE_LOCAL) ||
    mr3Conf.getAmLocalResourceSchedulerCpuCores == Utils.getContainerGroupCores(sparkConf) },
    s"In local worker mode, ${MR3Conf.MR3_AM_LOCAL_RESOURCESCHEDULER_CPU_CORES} must match ${SparkLauncher.EXECUTOR_CORES}.")
  require({ !(mr3Conf.getAmWorkerMode == MR3Constants.MR3_WORKER_MODE_LOCAL) ||
    mr3Conf.getAmLocalResourceSchedulerMaxMemoryMb == Utils.getContainerGroupNonNativeMemoryMb(sparkConf) },
    s"In local worker mode, ${MR3Conf.MR3_AM_LOCAL_RESOURCESCHEDULER_MAX_MEMORY_MB} must match " +
    s"${SparkLauncher.EXECUTOR_MEMORY} + spark.executor.memoryOverhead")

  private[this] val sparkMr3Client = {
    val rpcAddress = driverEndpoint.address
    val driverName = ENDPOINT_NAME
    val driverAddress = RpcEndpointAddress(rpcAddress, driverName)
    val driverUrl = driverAddress.toString
    val ioEncryptionKey = sc.env.securityManager.getIOEncryptionKey()
    new SparkMR3Client(mr3Conf, sparkConf, sparkApplicationId = applicationId, driverUrl = driverUrl, ioEncryptionKey)
  }

  val dagStatusCheckerPeriodMs: Int = sparkConf.getInt(
      SparkMR3Config.SPARK_MR3_DAG_STATUS_CHECKER_PERIOD_MS,
      SparkMR3Config.SPARK_MR3_DAG_STATUS_CHECKER_PERIOD_MS_DEFAULT)
  val dagStatusCheckerExecutor: ThreadPoolExecutor = ThreadUtils.newDaemonCachedThreadPool("DAGStatusChecker")

  //
  // SchedulerBackend, TskScheduler
  //

  @throws[InterruptedException]
  @throws[SparkException]
  def start(): Unit = {
    ShutdownHookManager.addShutdownHook { () =>
      logInfo(s"MR3Backend shutting down: $appId")
      taskSetManagersLock.synchronized {
        // tsm.kill() just sets shouldKillDag, and the DAGStatusChecker thread may not have to chance to read it.
        // Hence we directly call DAGClient.tryKillDag() right here.
        // Note that DAGClient.getDagStatusWait() is currently in progress in DAGStatusChecker.
        taskSetManagersByStageIdAttempt.values foreach { _.dagClient.tryKillDag() }
      }
    }
    startHadoopDelegationTokenManager()
    sparkMr3Client.start()
  }

  private def startHadoopDelegationTokenManager(): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      delegationTokenManager = new HadoopDelegationTokenManager(sc.conf, sc.hadoopConfiguration, driverEndpoint)
      val tokens =
        if (delegationTokenManager.renewalEnabled) {
          delegationTokenManager.start()
        } else {
          val creds = UserGroupInformation.getCurrentUser.getCredentials
          delegationTokenManager.obtainDelegationTokens(creds)
          if (creds.numberOfTokens() > 0 || creds.numberOfSecretKeys() > 0) {
            SparkHadoopUtil.get.serialize(creds)
          } else
            null
        }
      if (tokens != null) {
        updateDelegationTokenManager(tokens)
      }
    }
  }

  @throws[SparkException]
  def stop(): Unit = {
    logInfo(s"MR3Backend stopping: $appId")

    taskSetManagersLock.synchronized {
      taskSetManagersByStageIdAttempt.values foreach { _.kill("MR3Backend.stop() called", None) }
      // The following assert{} is invalid because unregisterTaskSetManager() may be called from
      // MR3TaskSetManager.taskSucceeded() with transitionToSucceeded == true.
      //   assert { taskSetManagersByStageIdAttempt.isEmpty }
      while (taskSetManagersByStageIdAttempt.nonEmpty) {
        taskSetManagersLock.wait()
      }
    }

    taskResultGetter.stop()
    sparkMr3Client.stop()
    dagStatusCheckerExecutor.shutdownNow()
    if (barrierCoordinator != null) {
      barrierCoordinator.stop()
    }
    if (delegationTokenManager != null) {
      delegationTokenManager.stop()
    }
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askSync[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping MR3DriverEndpoint", e)
    }
  }

  // return the default level of parallelism to use in the cluster, as a hint for sizing jobs
  // Cf. org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
  def defaultParallelism(): Int = {
    val maxNumTasks = maxNumConcurrentTasks() max 2
    sparkConf.getInt("spark.default.parallelism", maxNumTasks)
  }

  override def applicationId: String = appId

  //
  // SchedulerBackend
  //

  def reviveOffers(): Unit = {
    // In Spark on MR3, Spark delegates scheduling decision to MR3.
    // So, this method does nothing and we should not call it.
    assert { false }
  }

  // return the max number of tasks that can be concurrently launched
  // return 0 if no executors are running (Cf. CoarseGrainedSchedulerBackend.scala in Spark)
  def maxNumConcurrentTasks(): Int = {
    val currentNumExecutors = numExecutors.get()
    val numTasksPerExecutor = coresPerExecutor / cpusPerTask
    numTasksPerExecutor * currentNumExecutors
  }

  //
  // TaskScheduler
  //

  // Called by DAGScheduler thread
  def submitTasks(taskSet: TaskSet): Unit = {
    val runningTaskSetManagers = getTaskSetManagersWithStageId(taskSet.stageId)
    runningTaskSetManagers foreach { _.stop() }

    val epoch = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].getEpoch
    val addedFiles = mutable.HashMap[String, Long](sc.addedFiles.toSeq: _*)
    val addedJars = mutable.HashMap[String, Long](sc.addedJars.toSeq: _*)
    val numTasks = taskSet.tasks.length
    val sparkTaskIdStart = nextTaskId

    nextTaskId += numTasks

    val taskDescriptions = new Array[TaskDescription](numTasks)
    val taskLocationHints = new Array[Seq[String]](numTasks)
    var emittedTaskSizeWarning = false
    var index = 0

    try {
      while (index < numTasks) {
        val task = taskSet.tasks(index)
        task.epoch = epoch
        val taskId = sparkTaskIdStart + index

        val serializedTask: ByteBuffer = taskSerializer.serialize(task)   // may throw exception
        if (serializedTask.limit() > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024 && !emittedTaskSizeWarning) {
          emittedTaskSizeWarning = true
          logWarning(s"Stage ${task.stageId} contains a task of very large size " +
            s"(${serializedTask.limit() / 1024} KiB). The maximum recommended task size is " +
            s"${TaskSetManager.TASK_SIZE_TO_WARN_KIB} KiB.")
        }

        taskDescriptions(index) = new TaskDescription(
            taskId = taskId,
            attemptNumber = 0,
            executorId = "Undetermined",
            name = s"task $taskId in stage ${taskSet.id}",
            index = index,
            partitionId = task.partitionId,
            addedFiles = addedFiles,
            addedJars = addedJars,
            task.localProperties,
            resources = Map.empty,
            serializedTask)
        taskLocationHints(index) = task.preferredLocations.flatMap {
          case e: ExecutorCacheTaskLocation =>
            assert { e.executorId.contains('#') }   // Cf. SparkRuntimeEnv.createExecutorBackend()
            Seq(e.executorId.split('#').head, e.host)
          case h: HostTaskLocation =>
            Some(h.host)
          case h: HDFSCacheTaskLocation =>
            // TODO: for HDFSCacheTaskLocation, first add all executorId's on it and then add its host
            Some(h.host)
        }

        index = index + 1
      }

      val dagConf = getMr3ConfProto(taskSet.properties)
      if (taskSet.tasks(0).isBarrier) {
        initializeBarrierCoordinatorIfNecessary()
        // BarrierTaskContext requires the list of addresses of all tasks in the same stage, ordered by partition ID.
        // However, Spark uses this information only to check if all tasks have been launched.
        // Thus using fake addresses is okay for Spark-MR3 (which does not fully support barrier jobs).
        // Note that the fake addresses can be obtained via BarrierTaskContext.getTaskInfos().
        val taskIds = (0 until numTasks).mkString(",")
        taskDescriptions foreach { _.properties.setProperty("addresses", taskIds) }
      }

      taskSetManagersLock.synchronized {
        // MR3DriverEndpoint can call statusUpdate() right after we submit the DAG.
        // So stay inside taskSetManagersLock.synchronized{} until registerTaskSetManager() returns.
        val dagClient = sparkMr3Client.submitTasks(
            priority = taskSet.priority, stageId = taskSet.stageId,
            taskDescriptions, taskLocationHints, sparkConf, dagConf, Option(credentials.get))
        // sparkMr3Client.mr3Client is MR3SessionClient, and we have dagClient.ownDagClientHandler == false.
        // hence dagClient.close() should NOT be called.
        val taskSetMgr = new MR3TaskSetManager(taskSet, sparkTaskIdStart, dagClient, backend = this)
        registerTaskSetManager(taskSetMgr)
      }
      logInfo(s"Registered TaskSetManager: $taskSet, $sparkTaskIdStart")
    } catch {
      case NonFatal(e) =>
        val msg = s"Failed to create a DAG for TaskSet: ${taskSet.id}"
        logError(msg, e)
        dagScheduler.taskSetFailed(taskSet, msg, None)
    }
  }

  private def getMr3ConfProto(properties: Properties): DAGAPI.ConfigurationProto.Builder = {
    val mr3ConfProto = DAGAPI.ConfigurationProto.newBuilder
    val enum = properties.propertyNames
    while (enum.hasMoreElements) {
      val elem = enum.nextElement()
      elem match {
        case key: String =>
          if (key.startsWith(SparkMR3Config.SPARK_MR3_CONFIG_PREFIX) &&
              !SparkMR3Config.RESERVED_CONFIG_KEYS.contains(key)) {
            Option(properties.getProperty(key)) foreach { value =>
              val mr3ConfKey = key.replaceFirst(SparkMR3Config.SPARK_MR3_CONFIG_PREFIX, "mr3.")
              logInfo(s"Converting to add to DAGConf: $mr3ConfKey, $value")
              val keyValueProto = DAGAPI.KeyValueProto.newBuilder
                .setKey(mr3ConfKey)
                .setValue(value)
              mr3ConfProto.addConfKeyValues(keyValueProto)
            }
          }
        case _ =>
      }
    }
    mr3ConfProto
  }

  // This method is called by DAGScheduler thread only.
  // So it is safe to check the nullity of barrierCoordinator without synchronization.
  private def initializeBarrierCoordinatorIfNecessary(): Unit = {
    if (barrierCoordinator == null) {
      val barrierSyncTimeout = sparkConf.get(BARRIER_SYNC_TIMEOUT)
      barrierCoordinator = new BarrierCoordinator(barrierSyncTimeout, sc.listenerBus, sc.env.rpcEnv)
      sc.env.rpcEnv.setupEndpoint("barrierSync", barrierCoordinator)
      logInfo("Registered BarrierCoordinator endpoint")
    }
  }

  // TODO: document why interruptThread is ignored in cancelTasks/killAllTaskAttempts()

  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {
    val runningTaskSetManagers = getTaskSetManagersWithStageId(stageId)
    runningTaskSetManagers foreach { _.kill(s"Cancel stage $stageId", None) }
  }

  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    throw new UnsupportedOperationException
  }

  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit = {
    val runningTaskSetManagers = getTaskSetManagersWithStageId(stageId)
    runningTaskSetManagers foreach { _.stop() }
  }

  private def getTaskSetManagersWithStageId(stageId: Int): Iterable[MR3TaskSetManager] = {
    taskSetManagersLock.synchronized {
      taskSetManagersByStageIdAttempt.filterKeys{ _._1 == stageId }.values
    }
  }

  def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
    // In vanilla Spark, notifyPartitionCompletion() causes TaskSetManager to skip executing the Task with the same partitionId.
    // In Spark on MR3, TaskSetManager cannot stop individual tasks that are already submitted to MR3.
    // Hence notifyPartitionCompletion() is no-op.
  }

  // guaranteed to be called before submitTasks() is called
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }

  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    // cf. TaskSchedulerImpl
    // accumUpdatesWithTaskIds: Array[(taskId, stageId, stageAttemptId, accumUpdates)]
    val accumUpdatesWithTaskIds = taskSetManagersLock.synchronized {
      accumUpdates.flatMap { case (mr3TaskId, updates) =>
        val accumInfos = updates map { acc => acc.toInfo(Some(acc.value), None) }
        val sparkTaskId = SparkMR3TaskID.toSparkTaskId(mr3TaskId)
        taskSetManagersByTaskId.get(sparkTaskId)
          .map{ taskSetMgr => (mr3TaskId, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accumInfos) }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId, executorUpdates)
  }

  def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    throw new UnsupportedOperationException
  }

  def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logError("Running MR3Backend with Spark Standalone Cluster is denied.")
    assert { false }
  }

  //
  // MR3BackendDriverEndpointInterface
  //

  def statusUpdate(
      executorId: String,
      hostname: String,
      mr3TaskId: MR3TaskID,
      state: TaskState,
      data: ByteBuffer): Unit = {
    try {
      val sparkTaskId = SparkMR3TaskID.toSparkTaskId(mr3TaskId)
      val maybeTaskSetManager = taskSetManagersLock.synchronized { taskSetManagersByTaskId.get(sparkTaskId) }
      maybeTaskSetManager match {
        case Some(taskSetMgr) =>
          state match {
            case TaskState.RUNNING =>
              taskSetMgr.taskStarted(mr3TaskId, executorId, hostname)
            case TaskState.FINISHED =>
              taskResultGetter.fetchSuccessfulTask(
                  taskSetMgr, mr3TaskId, data, executorId = executorId, host = hostname)
            case TaskState.FAILED | TaskState.KILLED =>
              taskResultGetter.failTaskWithFailedReason(
                  taskSetMgr, mr3TaskId, state, data, executorId = executorId, host = hostname)
            case _ =>
              logWarning(s"Ignore TaskState in stateUpdate: $state, $mr3TaskId")
          }
        case None =>
          logError(
            ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
              "likely the result of receiving duplicate task finished status updates) or its " +
              "executor has been marked as failed.")
              .format(state, mr3TaskId))
      }
    } catch {
      case e: Exception => logError("Exception in statusUpdate", e)
    }
  }

  // returns immediately because dagScheduler.executorAdded() returns immediately
  def registerExecutor(executorId: String, hostname: String, data: ExecutorData): Unit = {
    dagScheduler.executorAdded(executorId, hostname)
    sc.listenerBus.post(SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
  }

  // returns immediately because dagScheduler.executorLost() returns immediately
  def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    dagScheduler.executorLost(executorId, reason)
    sc.listenerBus.post(SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
  }

  def removeExecutorFromBlockManagerMaster(executorId: String): Unit = {
    sc.env.blockManager.master.removeExecutorAsync(executorId)
  }

  def updateDelegationTokenManager(tokens: Array[Byte]): Unit = {
    val newCredentials = SparkHadoopUtil.get.deserialize(tokens)
    credentials.set(newCredentials)
  }

  //
  // registerTaskSetManager/unregisterTaskSetManager()
  //

  // Invariant: inside taskSetManagersLock.synchronized{}
  private def registerTaskSetManager(taskSetManager: MR3TaskSetManager): Unit = {
    val stageIdAttempt = (taskSetManager.taskSet.stageId, taskSetManager.taskSet.stageAttemptId)

    assert { !taskSetManagersByStageIdAttempt.isDefinedAt(stageIdAttempt) }
    taskSetManagersByStageIdAttempt.put(stageIdAttempt, taskSetManager)

    taskSetManager.sparkTaskIdsForeach { taskId => taskSetManagersByTaskId.put(taskId, taskSetManager) }
    rootPool addSchedulable taskSetManager
  }

  // called from MR3TaskSetManager that makes the final state transition
  def unregisterTaskSetManager(taskSetManager: MR3TaskSetManager): Unit = {
    val numTaskSetManagers = taskSetManagersLock.synchronized {
      val stageIdAttempt = (taskSetManager.taskSet.stageId, taskSetManager.taskSet.stageAttemptId)

      assert { taskSetManagersByStageIdAttempt contains stageIdAttempt }
      taskSetManagersByStageIdAttempt remove stageIdAttempt

      taskSetManager.sparkTaskIdsForeach { taskId => taskSetManagersByTaskId.remove(taskId) }
      rootPool removeSchedulable taskSetManager

      taskSetManagersLock.notifyAll()
      taskSetManagersByStageIdAttempt.size
    }
    logInfo(s"Unregistered TaskSetManager: ${taskSetManager.taskSet}, ($numTaskSetManagers remaining)")
  }
}
