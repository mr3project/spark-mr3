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

private[mr3] object SparkMR3Config {
  val SPARK_MR3_CONFIG_PREFIX = "spark.mr3."

  // SPARK_MR3_APP_ID specifies the MR3 application ID on Yarn and Kubernetes.
  // If set, SparkMR3Client directly connects to an existing DAGAppMaster.
  // If not set,
  //   on Yarn, create a new DAGAppMaster.
  //   on Kubernetes,
  //     1) if MR3_APPLICATION_ID_TIMESTAMP is set,
  //        1-1) create a new DAGAppMaster if MR3_APPLICATION_ID_TIMESTAMP has not be used
  //        1-2) connect to an existing DAGAppMaster if MR3_APPLICATION_ID_TIMESTAMP has already been used
  //     2) if MR3_APPLICATION_ID_TIMESTAMP is not set, create a new DAGAppMaster
  //     --> we cannot be sure if a new DAGAppMaster is created.
  // Observation:
  //   1. The ownership of a DAGAppMaster should not decide whether or not to kill DAGAppMaster
  //      because the DAGAppMaster can be intended for sharing among all Spark drivers.
  //   2. On Kubernetes, shutting down DAGAppMaster is bad because we should delete various other resources as well.
  // Decision:
  //   if mr3.master.mode == kubernetes, we keep DAGAppMaster when the Spark driver terminates.
  //   if not, SPARK_MR3_KEEP_AM decides where or not to keep DAGAppMaster when the Spark driver terminates.
  //     - use '--conf spark.mr3.keep.am=true' to keep DAGAppMaster
  //     - use '--conf spark.mr3.keep.am=false' to shut down DAGAppMaster
  val SPARK_MR3_APP_ID = "spark.mr3.appid"

  val SPARK_MR3_KEEP_AM = "spark.mr3.keep.am"
  val SPARK_MR3_KEEP_AM_DEFAULT = true

  val SPARK_MR3_CLIENT_CONNECT_TIMEOUT_MS = "spark.mr3.client.connect.timeout.ms"
  val SPARK_MR3_CLIENT_CONNECT_TIMEOUT_MS_DEFAULT = 30L * 1000

  val SPARK_MR3_DAG_STATUS_CHECKER_PERIOD_MS = "spark.mr3.dag.status.checker.period.ms"
  val SPARK_MR3_DAG_STATUS_CHECKER_PERIOD_MS_DEFAULT = 1000

  val RESERVED_CONFIG_KEYS = Seq(
      SPARK_MR3_APP_ID, SPARK_MR3_KEEP_AM, SPARK_MR3_CLIENT_CONNECT_TIMEOUT_MS, SPARK_MR3_DAG_STATUS_CHECKER_PERIOD_MS)
}
