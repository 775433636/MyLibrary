package spark.sparkStreaming.myworking

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamingTop {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStream")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("E:\\project\\bigdata\\scalamv\\data\\checkpoint")
    //3.初始offset
    val fromOffsets: Map[TopicAndPartition, Long] = Map(
      TopicAndPartition("topica", 0) -> 100,
      TopicAndPartition("topica", 1) -> 100,
      TopicAndPartition("topica", 2) -> 100
    )
    //4.消息函数
    val messageHandler: MessageAndMetadata[String, String] => String =
      (x: MessageAndMetadata[String, String]) => (x.message())


    //定义初始参数
    val kafkaParams = Map(
      "group.id" -> "group1",
      "metadata.broker.list" -> "master:9092"
    )

    val kafkaDS: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc, kafkaParams, fromOffsets, messageHandler)

    //    kafkaDS.foreachRDD(rdd => {
    //      rdd.foreachPartition(ite => {
    //        ite.foreach(println)
    //      })
    //    })

    //普通topN
    //    kafkaDS
    //      .flatMap(_.split("\\s+"))
    //      .map(word => (word , 1))
    //      .reduceByKey(_+_)
    //        .transform(rdd => {
    //          rdd.sortBy(_._2 , false)
    //        }).print(3)

    //
    //    kafkaDS
    //        .flatMap(_.split("\\s+"))
    //        .map(word => (word , 1))
    //        .reduceByKeyAndWindow((x:Int,y:Int) => x+y , (k:Int , v:Int) => k-v , Seconds(20) , Seconds(5))
    //        .transform(rdd => {
    //          rdd.sortBy(_._2 , false)
    //        }).print(5)

    kafkaDS
      .flatMap(_.split("\\s+"))
      .map(word => (word, 1))
      //Seq[V], Option[S]) => Option[S]
      .updateStateByKey((lst: Seq[Int], state: Option[Int]) => {
      val sum: Int = lst.sum
      val result: Int = state.getOrElse(0) + sum
      Option(result)
    }).transform(rdd => {
      rdd.sortBy(_._2, false)
    }).print(5)

    ssc.start()
    ssc.awaitTermination()


  }
}
