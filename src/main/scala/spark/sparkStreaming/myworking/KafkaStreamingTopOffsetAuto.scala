package spark.sparkStreaming.myworking

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamingTopOffsetAuto {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStream")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("E:\\project\\bigdata\\scalamv\\data\\checkpoint")

    //定义参数
    val zkQuorum ="master:2181/kafka1.1"
    val topics = Map(
      "topica" -> 0 ,
      "topica" -> 1 ,
      "topica" -> 2
    )
    val groupid = "group1"

    val kafkaDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc , zkQuorum , groupid , topics , StorageLevel.MEMORY_ONLY)
    kafkaDS
      .flatMap(_._2.split("\\s+"))
        .map(word => (word , 1))
        .reduceByKey(_+_)
        .print()


    ssc.start()
    ssc.awaitTermination()
  }
}
