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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.orc._

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Record(key: Int, value: String)

object OrcFileTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ORCFileTest")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // Importing the hive context gives access to all the SQL functions and implicit conversions.
    import hiveContext._
    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    rdd.registerTempTable("orcRecords")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    sql("SELECT * FROM orcRecords").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = hiveContext.sql("SELECT COUNT(*) FROM orcRecords").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = hiveContext.sql("SELECT key, value FROM orcRecords WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    rdd.where('key === 1).orderBy('value.asc).select('key).collect().foreach(println)

    // Write out an RDD as a parquet file.
    rdd.toSchemaRDD.saveAsOrcFile("pair.orc")

    // Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
    val orcFile = hiveContext.orcFile("pair.orc")

    // Queries can be run using the DSL on parequet files just like the original RDD.
    orcFile.where('key === 1).select('value as 'a).collect().foreach(println)

    // These files can also be registered as tables.
    orcFile.registerTempTable("orcFile")
    sql("SELECT * FROM orcFile").collect().foreach(println)

    sc.stop()
  }
}
