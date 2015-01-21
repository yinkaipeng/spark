package org.apache.spark.examples.streaming

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.DefaultDecoder
import org.apache.spark.storage.StorageLevel

/**
 * Created by kalmesh on 16/01/15.
 */
object PhiKafka extends Logging{

  def isNumeric(input: String): Boolean = input.forall(_.isDigit)

  def main(args: Array[String]) {
    if (args.length < 4) {
      logInfo("Please provide valid parameters: <consumer-topic> <consumer-group> <num-of-partitions> <no of cores> <app name>")
      return;
    }

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "", //Empty in our case - hard-coded
      "zookeeper.hosts"->"",//hard-coded - comma seperated list of zookeeper host-names
      "zookeeper.port" -> "2181",// /hard-coded
      "kafka.topic" -> args(0),
      "group.id" -> args(1),
      "auto.offset.reset" -> "smallest", // must be compatible with when/how we are writing the input data to Kafka
      "zookeeper.connection.timeout.ms" -> "1000")

    val numOfPartitionsOfTopic:Int = args(2).toInt

    val noOfCores = args(3)
    if(!isNumeric(noOfCores)) {
      logInfo("please provide valid noOfCores")
      return;
    }

    val conf = new SparkConf().setAppName(args(4))
    conf.set("spark.cores.max", noOfCores)

    val ssc = new StreamingContext(conf, Seconds(2));

    val kafkaStreams = (1 to numOfPartitionsOfTopic).map { _ =>
      KafkaUtils.createStream[Array[String], Array[String], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, Map(args(0) -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
    }

    val unionStreams = ssc.union(kafkaStreams)

    unionStreams.foreachRDD(rddMessageAndMetaData => {
      logInfo("entered forEachRDD")//This gets printed
      rddMessageAndMetaData.foreachPartition {
        logInfo("entered forEachPartition")//This gets printed
        partitionOfRecords=> {
          partitionOfRecords.foreach {
            logInfo("entered foreach-partitionOfRecords")//This is not getting printed
            record=>{
              logInfo("record=>" + record.toString+"=>")//This is not getting printed
            }
          }
        }
      }

    });

    ssc.start();
    ssc.awaitTermination();
  }
}
