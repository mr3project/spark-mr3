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

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler}

class MR3ClusterManager extends ExternalClusterManager {
  def canCreate(masterURL: String): Boolean = {
    // from Spark 3.0.3, masterURL = "mr3" is rejected, so we use "spark" as well
    masterURL.startsWith("mr3") || masterURL.startsWith("spark")
  }

  def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new MR3Backend(sc, masterURL)
  }

  def createSchedulerBackend(sc: SparkContext, masterURL: String, scheduler: TaskScheduler): SchedulerBackend = {
    scheduler.asInstanceOf[MR3Backend]
  }

  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
  }
}
