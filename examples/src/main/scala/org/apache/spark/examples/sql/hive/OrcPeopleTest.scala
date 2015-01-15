/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.orc._

//hadoop dfs -put examples/src/main/resources/people.txt people.txt

object OrcPeopleTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("OrcPeopleTest")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val people = sc.textFile("people.txt")
    val schemaString = "name age"
    val schema = StructType(schemaString.split(" ").map(fieldName => {
      if(fieldName == "name") StructField(fieldName, StringType, true)
      else StructField(fieldName, IntegerType, true)}))
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), new Integer(p(1).trim)))

    val peopleSchemaRDD = hiveContext.applySchema(rowRDD, schema)

    peopleSchemaRDD.registerTempTable("people")
    val results = hiveContext.sql("SELECT * FROM people")
    results.map(t => "Name: " + t.toString).collect().foreach(println)
    peopleSchemaRDD.saveAsOrcFile("people.orc")
    val morePeople = hiveContext.orcFile("people.orc")
    morePeople.registerTempTable("morePeople")
    println("people record")
    hiveContext.sql("SELECT * from morePeople").collect.foreach(println)
    sc.stop()
  }
}
