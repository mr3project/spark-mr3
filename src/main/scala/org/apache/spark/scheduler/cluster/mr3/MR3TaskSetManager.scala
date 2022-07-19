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
import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import javax.annotation.concurrent.GuardedBy

import com.datamonad.mr3.api.client.{DAGClient, DAGState, DAGStatus}
import org.apache.spark.{ExceptionFailure, FetchFailed, Success, TaskFailedReason, TaskKilled, TaskState, UnknownReason}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.cluster.mr3.SparkMR3TaskID.{MR3TaskID, SparkTaskID}
import org.apache.spark.scheduler.{DirectTaskResult, ExecutorLossReason, Pool, Schedulable, SchedulingMode, ShuffleMapTask, Task, TaskInfo, TaskLocality, TaskSet, TaskSetManager}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{SystemClock, Utils => SparkUtils}

import scala.collection.mutable

private[mr3] object MR3TaskSetManagerState extends Enumeration {
  type MR3TaskSetManagerState = Value
  val Running, Succeeded, Failed, Stopped, Killed = Value
  // Available state transition: Running => {Succeeded, Failed, Stopped, Killed}
  // Final state: Succeeded, Failed, Killed, Stopped
}

private[mr3] trait DAGStatusCheckerInterface {
  def checkStatus(maybeDagStatus: Option[DAGStatus], serializer: => SerializerInstance): Boolean
}

private[mr3] class MR3TaskSetManager(
    val taskSet: TaskSet,
    val sparkTaskIdStart: SparkTaskID,
    val dagClient: DAGClient,   // passed to DAGStatusChecker and never called directly in MR3TaskSetManager, except in shutdown hook
    backend: MR3Backend) extends Schedulable
  with DAGStatusCheckerInterface
  with Logging {

  import MR3TaskSetManagerState._

  private[this] val tasks: Array[Task[_]]= taskSet.tasks
  private[this] val numTasks: Int = tasks.length
  private[this] val numRunningTaskAttempts: AtomicInteger = new AtomicInteger(0)
  private[this] val isBarrier: Boolean = tasks.nonEmpty && tasks(0).isBarrier
  private[this] val isShuffleMapTasks = tasks(0).isInstanceOf[ShuffleMapTask]

  private def toTaskIndex(mr3TaskId: MR3TaskID): Int = {
    val taskIndex = (SparkMR3TaskID.toSparkTaskId(mr3TaskId) - sparkTaskIdStart).toInt
    assert { 0 <= taskIndex && taskIndex < numTasks }
    taskIndex
  }

  def sparkTaskIdsForeach(f: SparkTaskID => Unit): Unit = {
    var index = sparkTaskIdStart
    while (index < sparkTaskIdStart + numTasks) {
      f(index)
      index += 1
    }
  }

  // only for assert{}
  def containSparkTaskId(sparkTaskId: SparkTaskID): Boolean = {
    sparkTaskIdStart <= sparkTaskId && sparkTaskId < sparkTaskIdStart + numTasks
  }

  //
  // Schedulable
  //

  // Set by DAGScheduler thread in submitTasks().
  var parent: Pool = _
  val schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null
  val schedulingMode: SchedulingMode = SchedulingMode.FIFO
  val weight = 1
  val minShare = 0
  def runningTasks: Int = numRunningTaskAttempts.get
  val priority: Int = taskSet.priority
  val stageId: Int = taskSet.stageId
  val name: String = taskSet.id

  val isSchedulable = true
  def addSchedulable(schedulable: Schedulable): Unit = {}
  def removeSchedulable(schedulable: Schedulable): Unit = {}
  def getSchedulableByName(name: String): Schedulable = null
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = {
    // In Spark on MR3, task resubmission is done by MR3.
    // So we omit calling DAGScheduler.taskEnded(task, Resubmitted, ...) here.
  }
  def executorDecommission(executorId: String): Unit = {
    // Spark v3.2.1 re-computes TaskLocality in this method.
    // Because MR3TaskSetManager does not manage TaskLocality, it does nothing.
  }
  def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean = false
  def getSortedTaskSetQueue: mutable.ArrayBuffer[TaskSetManager] = new mutable.ArrayBuffer

  private[this] val clock = new SystemClock()
  private[this] val EXCEPTION_PRINT_INTERVAL_MS: Long = backend.sparkConf.getLong("spark.logging.exceptionPrintInterval", 10000L)
  private[this] val MAX_RESULT_SIZE: Long = backend.sparkConf.get(config.MAX_RESULT_SIZE)

  // Updated by MR3TaskResultGetter threads.
  // recentExceptionsLock guards contention by multiple MR3TaskResultGetter threads.
  private[this] val recentExceptionsLock = new AnyRef
  // ErrorMessage -> (The number of duplicate message, Last printed time)
  @GuardedBy("recentExceptionsLock")
  private[this] val recentExceptions = new mutable.HashMap[String, (Int, Long)]

  // Updated by MR3TaskResultGetter threads.
  // updateFetchedSizeLock guards contention by multiple MR3TaskResultGetter threads.
  private[this] val updateFetchedSizeLock = new AnyRef
  @GuardedBy("updateFetchedSizeLock")
  private[this] var totalResultSize: Long = 0L
  @GuardedBy("updateFetchedSizeLock")
  private[this] var calculatedTasks: Int = 0

  @GuardedBy("taskAttemptIdMap")
  private val taskAttemptIdMap = new mutable.HashMap[SparkTaskID, Seq[(MR3TaskID, String)]]

  //
  // stateLock guards state, taskSucceededFlags, numSucceededTasks, toBeStoppedKilled, firstAttemptTaskInfos, and taskInfoMap
  //

  private[this] val stateLock = new AnyRef

  // Updated by DAGScheduler, DAGStatusChecker, MR3TaskResultGetter threads.
  // Read by MR3DriverEndpoint.
  // DAGScheduler: Running -> {Stopped, Killed}
  // DAGStatusChecker: Running -> Failed
  // MR3TaskResultGetter: Running -> Succeeded
  // A thread that escapes Running state should call killAllRunningTasks().
  @GuardedBy("stateLock")
  private[this] var state: MR3TaskSetManagerState = Running
  private def getState: MR3TaskSetManagerState = stateLock.synchronized { state }

  // Updated by MR3TaskResultGetter.
  // Read by MR3DriverEndpoint and MR3TaskResultGetter.
  @GuardedBy("stateLock")
  private[this] val taskSucceededFlags = Array.fill[Boolean](numTasks)(false)
  @GuardedBy("stateLock")
  private[this] var numSucceededTasks: Int = 0
  // the following assert{} is invalid because a Task may have multiple attempts.
  // assert { numSucceededTasks == taskSucceededFlags.count(_ == true) }

  // Updated by DAGScheduler, DAGStatusChecker, MR3DriverEndpoint, MR3TaskResultGetter threads.
  // Accessed under two conditions:
  //   1) when state == Running
  //   2) by the thread that makes a state transition from Running to another state
  // We can dispense with stateLock.synchronized{} in killAllRunningTasks():
  //   1) killAllRunningTasks() is called by the thread that escapes Running state.
  //   2) once killAllRunningTasks() is called, no other threads access firstAttemptTaskInfos[] and taskInfoMap[].
  @GuardedBy("stateLock")
  private val firstAttemptTaskInfos = new Array[TaskInfo](numTasks)   // with attempt == 0
  assert { firstAttemptTaskInfos
    .filter{ _ != null }
    .forall{ info => SparkMR3TaskID.toTaskAttemptIdId(info.taskId) == 0 } }
  assert { firstAttemptTaskInfos.zipWithIndex
    .filter{ case (info, _) => info != null }
    .forall{ case (info, index) => toTaskIndex(info.taskId) == index } }
  @GuardedBy("stateLock")
  private val taskInfoMap = new mutable.HashMap[MR3TaskID, TaskInfo]  // with attempt > 0
  assert { taskInfoMap.keys forall { mr3TaskId => SparkMR3TaskID.toTaskAttemptIdId(mr3TaskId) > 0 } }
  assert { taskInfoMap forall { case (mr3TaskId, info) => mr3TaskId == info.taskId } }
  assert { taskInfoMap.keys forall { mr3TaskId => containSparkTaskId(SparkMR3TaskID.toSparkTaskId(mr3TaskId)) } }

  // DAGStatusChecker
  private[this] val shouldKillDag = new AtomicBoolean(false)
  private[this] val dagStatusChecker = new DAGStatusChecker(name, dagClient, shouldKillDag, this, backend)
  dagStatusChecker.run()

  @GuardedBy("stateLock") private[this] var toBeStoppedKilled = false

  private def numRunningTaskAttemptsOkay: Boolean = {
    numRunningTaskAttempts.get() == stateLock.synchronized {
      firstAttemptTaskInfos.count{ taskInfo => taskInfo != null && taskInfo.running } +
      taskInfoMap.count{ _._2.running }
    }
  }

  private def normalizeMr3TaskId(rawMr3TaskId: MR3TaskID, executorId: String): MR3TaskID = taskAttemptIdMap.synchronized {
    val sparkTaskId = SparkMR3TaskID.toSparkTaskId(rawMr3TaskId)
    taskAttemptIdMap.get(sparkTaskId) match {
      case Some(currentSeq) =>
        val taId = currentSeq indexOf (rawMr3TaskId -> executorId)
        if (taId >= 0) {
          val mr3TaskId = SparkMR3TaskID.toMr3TaskId(sparkTaskId, taId)
          mr3TaskId
        } else {
          val taId = currentSeq.length
          val mr3TaskId = SparkMR3TaskID.toMr3TaskId(sparkTaskId, taId)
          taskAttemptIdMap += (sparkTaskId -> (currentSeq :+ (rawMr3TaskId -> executorId)))
          mr3TaskId
        }
      case None =>
        val taId = 0
        val mr3TaskId = SparkMR3TaskID.toMr3TaskId(sparkTaskId, taId)
        taskAttemptIdMap += (sparkTaskId -> Seq(rawMr3TaskId -> executorId))
        mr3TaskId
    }
  }

  //
  // operations on TaskInfo
  //

  @GuardedBy("stateLock")
  private def getTaskInfo(mr3TaskId: MR3TaskID): TaskInfo = {
    assert { state == Running }
    if (SparkMR3TaskID.toTaskAttemptIdId(mr3TaskId) == 0) {
      val index = toTaskIndex(mr3TaskId)
      assert { firstAttemptTaskInfos(index) != null }
      firstAttemptTaskInfos(index)
    } else {
      assert { taskInfoMap isDefinedAt mr3TaskId }
      taskInfoMap(mr3TaskId)
    }
  }

  @GuardedBy("stateLock")
  private def putTaskInfo(mr3TaskId: MR3TaskID, taskInfo: TaskInfo): Unit = {
    assert { state == Running }
    if (SparkMR3TaskID.toTaskAttemptIdId(mr3TaskId) == 0) {
      val index = toTaskIndex(mr3TaskId)
      assert { firstAttemptTaskInfos(index) == null }
      firstAttemptTaskInfos(index) = taskInfo
    } else {
      assert { !taskInfoMap.isDefinedAt(mr3TaskId) }
      taskInfoMap.put(mr3TaskId, taskInfo)
    }
  }

  // must be called by the thread that escapes Running state
  private def killAllRunningTasks(): Unit = {
    assert { getState != Running }
    val killedTime = clock.getTimeMillis()
    // TODO: remove stateLock.synchronized{} (see the invariant on firstAttemptTaskInfos[] and taskInfoMap[])
    stateLock.synchronized {
      firstAttemptTaskInfos.filter{ info => info != null && info.running }.foreach { taskInfo =>
        val task = tasks(toTaskIndex(taskInfo.taskId))
        taskInfo.markFinished(TaskState.KILLED, killedTime)
        backend.dagScheduler.taskEnded(task, UnknownReason, null, Seq.empty, Array.empty, taskInfo)
      }
      taskInfoMap.values.filter{ _.running }.foreach { taskInfo =>
        val task = tasks(toTaskIndex(taskInfo.taskId))
        taskInfo.markFinished(TaskState.KILLED, killedTime)
        backend.dagScheduler.taskEnded(task, UnknownReason, null, Seq.empty, Array.empty, taskInfo)
      }
    }
    numRunningTaskAttempts.set(0)
    assert { numRunningTaskAttemptsOkay }
  }

  // Called by MR3DriverEndpoint thread
  def taskStarted(rawMr3TaskId: MR3TaskID, executorId: String, host: String): Unit = {
    val mr3TaskId = normalizeMr3TaskId(rawMr3TaskId, executorId)
    val index = toTaskIndex(mr3TaskId)
    val attemptNumber = SparkMR3TaskID.toTaskAttemptIdId(mr3TaskId)
    val launchedTime = clock.getTimeMillis()
    val taskLocality = getTaskLocality(index, host)
    val taskInfo = new TaskInfo(
        taskId = mr3TaskId, index = index, attemptNumber = attemptNumber, launchTime = launchedTime,
        executorId = executorId, host = host, taskLocality,
        speculative = false)

    val isTaskStarted = stateLock.synchronized {
      if (state == Running) {
        putTaskInfo(mr3TaskId, taskInfo)
        numRunningTaskAttempts.getAndIncrement()
        backend.dagScheduler.taskStarted(tasks(index), taskInfo)
        true
      } else {
        logWarning(s"Ignore TaskStarted(Task: $mr3TaskId) from $executorId in state: $state")
        false
      }
    }
    if (isTaskStarted) {
      val taskName = s"${SparkMR3TaskID.toSparkTaskId(mr3TaskId)}_${SparkMR3TaskID.toTaskAttemptIdId(mr3TaskId)}"
      val taskIndex = toTaskIndex(mr3TaskId)
      logInfo(s"Task $taskName with index $taskIndex started on $executorId")
    }
  }

  // TODO: On K8s, host is a Pod-local IP address, and taskLocality is set to TaskLocality.ANY if
  //   taskPreferredLocations contains only node-local host names or IP addresses.
  private def getTaskLocality(index: Int, host: String): TaskLocality.Value = {
    val taskPreferredLocations = tasks(index).preferredLocations.map(_.host)
    if (taskPreferredLocations.isEmpty)
      TaskLocality.NO_PREF
    else if (taskPreferredLocations contains host)
      TaskLocality.NODE_LOCAL
    else
      TaskLocality.ANY
  }

  // Called by MR3TaskResultGetter threads
  def taskSucceeded(
      rawMr3TaskId: MR3TaskID,
      result: DirectTaskResult[_],
      executorId: String,
      host: String): Unit = {
    val mr3TaskId = normalizeMr3TaskId(rawMr3TaskId, executorId)
    val (isTaskSucceeded, transitionToSucceeded) = stateLock.synchronized {
      val curState = state
      val isTaskSucceeded =
        if (curState == Running) {
          setTaskSucceeded(mr3TaskId, result)
          if (!toBeStoppedKilled && numSucceededTasks >= numTasks && (taskSucceededFlags forall { _ == true })) {
            state = Succeeded
          }
          true
        } else {
          logWarning(s"Ignore TaskSucceeded(Task: $mr3TaskId) from $executorId in state $curState")
          false
        }
      val newState = state
      (isTaskSucceeded, (curState != newState) && (newState == Succeeded))
    }
    if (isTaskSucceeded) {
      val taskName = s"${SparkMR3TaskID.toSparkTaskId(mr3TaskId)}_${SparkMR3TaskID.toTaskAttemptIdId(mr3TaskId)}"
      val taskIndex = toTaskIndex(mr3TaskId)
      logInfo(s"Task $taskName with index $taskIndex succeeded on $executorId")
    }
    if (transitionToSucceeded) {
      logInfo(s"MR3TaskSetManager $name succeeded")
      killAllRunningTasks()
      backend.unregisterTaskSetManager(this)
    }
  }

  // inside stateLock.synchronized{}
  private def setTaskSucceeded(mr3TaskId: MR3TaskID, result: DirectTaskResult[_]): Unit = {
    val index = toTaskIndex(mr3TaskId)
    val taskInfo = getTaskInfo(mr3TaskId)
    taskInfo.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    // notify DAGScheduler regardless of taskSucceededFlags(index)
    backend.dagScheduler.taskEnded(
      tasks(index), Success, result.value(), result.accumUpdates, result.metricPeaks, taskInfo)
    taskSucceededFlags(index) = true
    numSucceededTasks += 1
    numRunningTaskAttempts.getAndDecrement()
    // assert { numRunningTaskAttemptsOkay }  // disable assert{} for performance
  }

  // Called by MR3TaskResultGetter threads
  def taskFailed(
      rawMr3TaskId: MR3TaskID,
      taskState: TaskState.Value,
      taskFailedReason: TaskFailedReason,
      executorId: String,
      host: String): Unit = {
    val mr3TaskId = normalizeMr3TaskId(rawMr3TaskId, executorId)
    val isTaskFailed = stateLock.synchronized {
      val curState = state
      if (curState == Running) {
        if (isBarrier || taskFailedReason.isInstanceOf[FetchFailed]) {
          // TODO: Should we set shouldKillDag to true?
          toBeStoppedKilled = true
        }

        val task = tasks(toTaskIndex(mr3TaskId))
        val taskInfo = getTaskInfo(mr3TaskId)
        taskInfo.markFinished(taskState, clock.getTimeMillis())
        taskFailedReason match {
          case ff: FetchFailed =>
            backend.dagScheduler.taskEnded(task, ff, null, Seq.empty, Array.empty, taskInfo)
          case ef: ExceptionFailure =>
            backend.dagScheduler.taskEnded(task, ef, null, ef.accums, ef.metricPeaks.toArray, taskInfo)
          case tk: TaskKilled =>
            backend.dagScheduler.taskEnded(task, tk, null, tk.accums, tk.metricPeaks.toArray, taskInfo)
          case e: TaskFailedReason =>
            backend.dagScheduler.taskEnded(task, e, null, Seq.empty, Array.empty, taskInfo)
        }
        numRunningTaskAttempts.getAndDecrement()
        assert { numRunningTaskAttemptsOkay }
        true
      } else {
        logInfo(s"Ignore TaskFailed(Task: $mr3TaskId) from $executorId in state $curState")
        false
      }
    }

    if (isTaskFailed) {
      val taskName = s"${SparkMR3TaskID.toSparkTaskId(mr3TaskId)}_${SparkMR3TaskID.toTaskAttemptIdId(mr3TaskId)}"
      val messageHeader = s"Lost task $taskName in stage $name from ($host, $executorId): "
      val failureMessage = getFailureMessage(taskFailedReason, messageHeader)
      logInfo(failureMessage)
    }
  }

  private def getFailureMessage(taskFailedReason: TaskFailedReason, messageHeader: String): String = {
    taskFailedReason match {
      case ef: ExceptionFailure =>
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = recentExceptionsLock.synchronized {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL_MS) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull)
          messageHeader + s"${taskFailedReason.toErrorString}"
        else
          messageHeader + s"${ef.className} (${ef.description}) [duplicate $dupCount]"
      case _ =>
        messageHeader + s"${taskFailedReason.toErrorString}"
    }
  }

  // Called by MR3TaskResultGetter threads
  def fetchTaskResult(rawMr3TaskId: MR3TaskID, executorId: String, host: String): Unit = {
    val mr3TaskId = normalizeMr3TaskId(rawMr3TaskId, executorId)
    val maybeTaskInfo = stateLock.synchronized {
      if (state == Running)
        Some(getTaskInfo(mr3TaskId))
      else
        None
    }
    maybeTaskInfo.foreach { taskInfo =>
      taskInfo.markGettingResult(clock.getTimeMillis())
      backend.dagScheduler.taskGettingResult(taskInfo)
    }
  }

  // Called by DAGScheduler and MR3TaskResultGetter threads
  def kill(message: String, exception: Option[Throwable]): Unit = {
    val transitionToKilled = stateLock.synchronized {
      if (state == Running) {
        state = Killed
        true
      } else
        false
    }

    if (transitionToKilled) {
      cleanupInKilledStoppedAfterLeavingRunning()
      backend.dagScheduler.taskSetFailed(taskSet, message, exception)
      logInfo(s"MR3TaskSetManager $name is killed")
    } else {
      logInfo(s"Cannot kill MR3TaskSetManager $name")
    }
  }

  // Called by DAGScheduler and MR3TaskResultGetter threads.
  def stop(): Unit = {
    val (transitionToStopped, toBeStoppedKilled) = stateLock.synchronized {
      val transitionToStopped =
        if (state == Running) {
          state = Stopped
          true
        } else
          false
      (transitionToStopped, this.toBeStoppedKilled)
    }

    if (transitionToStopped) {
      cleanupInKilledStoppedAfterLeavingRunning()
      if (toBeStoppedKilled) {
        logInfo(s"MR3TaskSetManager $name stopped")
      } else {
        logWarning(s"MR3TaskSetManager $name stopped while toBeStoppedKilled == false")
      }
    } else {
      logInfo(s"Cannot stop MR3TaskSetManager $name")
    }
  }

  private def cleanupInKilledStoppedAfterLeavingRunning(): Unit = {
    // do not call DAGClient.tryKillDag() directly here because DAG may have already been completed
    shouldKillDag.set(true)
    killAllRunningTasks()
    backend.unregisterTaskSetManager(this)
  }

  // Called by MR3TaskResultGetter threads
  def canFetchMoreResults(size: Int): Boolean = {
    val (currentResultSize, numFinishedTask) = updateFetchedSizeLock.synchronized {
      totalResultSize += size
      calculatedTasks += 1
      (totalResultSize, calculatedTasks)
    }
    if (!isShuffleMapTasks && MAX_RESULT_SIZE > 0 && currentResultSize > MAX_RESULT_SIZE) {
      val msg = s"Total size of serialized results of $numFinishedTask tasks " +
        s"(${SparkUtils.bytesToString(currentResultSize)}) is bigger than ${config.MAX_RESULT_SIZE.key} " +
        s"(${SparkUtils.bytesToString(MAX_RESULT_SIZE)})"
      logError(msg)
      kill(msg, None)
      false
    } else
      true
  }

  //
  // DAGStatusCheckerInterface
  //

  def checkStatus(maybeDagStatus: Option[DAGStatus], serializer: => SerializerInstance): Boolean = {
    maybeDagStatus match {
      case Some(dagStatus) =>
        dagStatus.state match {
          case DAGState.Failed | DAGState.Killed =>
            val transitionToFailed = checkTransitionToFailed()
            if (transitionToFailed) {
              val taskFailedReason = getTaskFailedReason(serializer, dagStatus)
              val exception = taskFailedReason match {
                case ef: ExceptionFailure => ef.exception
                case _ => None
              }
              val message = s"DAG failed/killed, most recent failure: ${taskFailedReason.toErrorString}"
              handleStateFailed(exception, message)
              logWarning(s"DAG failed/killed in $name, failing MR3TaskSetManager")
            } else {
              logWarning(s"DAG failed/killed in $name")
            }
          case DAGState.Succeeded =>
            logInfo(s"DAG succeeded in $name")
          case _ =>
        }
        dagStatus.isFinished
      case None =>
        // DAG has been lost, so we should fail MR3TaskSetManager which otherwise might wait indefinitely
        val transitionToFailed = checkTransitionToFailed()
        if (transitionToFailed) {
          val taskFailedReason = None
          val message = s"DAG lost in $name"
          handleStateFailed(taskFailedReason, message)
          logWarning(s"DAG lost in $name, failing MR3TaskSetManager")
        }
        true
    }
  }

  private def checkTransitionToFailed(): Boolean = stateLock.synchronized {
    if (state == Running && !toBeStoppedKilled) {
      state = Failed
      true
    } else
      false
  }

  private def handleStateFailed(exception: Option[Throwable], message: String): Unit = {
    killAllRunningTasks()
    backend.dagScheduler.taskSetFailed(taskSet, message, exception)
    backend.unregisterTaskSetManager(this)
  }

  private def getTaskFailedReason(serializer: SerializerInstance, dagStatus: DAGStatus): TaskFailedReason = {
    val loader = SparkUtils.getContextOrSparkClassLoader
    try {
      val diagnostics = dagStatus.diagnostics   // either empty or a singleton list
      if (diagnostics.isEmpty) {
        logWarning(s"MR3TaskSetManager $name failed with no diagnostic message")
        UnknownReason
      } else {
        if (diagnostics.length > 1) {
          logWarning(s"DAGStatus contains more than one diagnostic message: ${diagnostics.length}")
        }
        val serializedTaskFailedReason = ByteBuffer.wrap(Base64.getDecoder.decode(diagnostics.head))
        serializer.deserialize[TaskFailedReason](serializedTaskFailedReason, loader)
      }
    } catch {
      case _: ClassNotFoundException =>
        logError("Could not deserialize TaskFailedReason: ClassNotFound with classloader " + loader)
        UnknownReason
      case _: Throwable =>
        logError(s"Could not parse TaskFailedReason: ${dagStatus.diagnostics}")
        UnknownReason
    }
  }
}
