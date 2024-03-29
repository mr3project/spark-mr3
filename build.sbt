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

name := "spark-mr3"
version := "3.2.2"  // should be equal to SPARK_MR3_REV in env.sh

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "com.datamonad.mr3" % "mr3-core" % "1.0" % "provided",
  "org.apache.spark" % "spark-core_2.12" % "3.2.2" % "provided"
)

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := s"${name.value}-${version.value}-assembly.jar"
