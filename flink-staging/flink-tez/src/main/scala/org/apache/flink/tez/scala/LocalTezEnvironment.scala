/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tez.scala

import _root_.org.apache.flink.api.common.JobExecutionResult
import _root_.org.apache.flink.api.scala._
import _root_.org.apache.flink.tez.client.{LocalTezEnvironment => JavaEnv}

class LocalTezEnvironment (javaEnv: JavaEnv) extends ExecutionEnvironment (javaEnv) {
  
  override def execute (jobName: String) : JobExecutionResult = {
    javaEnv.execute(jobName)
  }

  override def getExecutionPlan (): String = {
    javaEnv.getExecutionPlan
  }
  
  def setAsContext (): Unit = {
    javaEnv.setAsContext()
  }

  // Copied from ExecutionEnvironment
  /*
  def fromCollection[T: ClassTag : TypeInformation](
    data: Seq[T]): DataSet[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    CollectionInputFormat.checkCollection(data.asJavaCollection, typeInfo.getTypeClass)
    val dataSource = new DataSource[T](
      javaEnv,
      new CollectionInputFormat[T](data.asJavaCollection, typeInfo.createSerializer(getConfig)),
      typeInfo,
      getCallLocationName())
    wrap(dataSource)
  }

  def getConfig: ExecutionConfig = {
    javaEnv.getConfig
  }

  def readTextFile(filePath: String, charsetName: String = "UTF-8"): DataSet[String] = {
    Validate.notNull(filePath, "The file path may not be null.")
    val format = new TextInputFormat(new Path(filePath))
    format.setCharsetName(charsetName)
    val source = new DataSource[String](javaEnv, format, BasicTypeInfo.STRING_TYPE_INFO,
      getCallLocationName())
    wrap(source)
  }
  */

}

object LocalTezEnvironment {
  
  def create(): LocalTezEnvironment = {
    val javaEnv = JavaEnv.create()
    new LocalTezEnvironment(javaEnv)
  }
  
}


