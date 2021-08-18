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
import com.datamonad.mr3.DAGAPI.DAGProto
import com.datamonad.mr3.api.common.MR3Conf
import org.apache.spark.SparkConf

private[mr3] object SentinelDAG {
  def createDag(
      applicationId: String,
      driverUrl: String,
      sparkConf: SparkConf): DAGProto = {
    val stopCrossDAGReuse = DAGAPI.KeyValueProto.newBuilder
      .setKey(MR3Conf.MR3_CONTAINER_STOP_CROSS_DAG_REUSE)
      .setValue("true")
    val containerGroupConf = DAGAPI.ConfigurationProto.newBuilder.addConfKeyValues(stopCrossDAGReuse)

    val containerGroup = DAG.getContainerGroupBuilder(applicationId, driverUrl, sparkConf, None)
      .setContainerGroupConf(containerGroupConf)

    DAGProto.newBuilder
      .setName(s"${applicationId}_SENTINEL_DAG")
      .addContainerGroups(containerGroup)
      .setJobPriority(0)
      .build
  }
}
