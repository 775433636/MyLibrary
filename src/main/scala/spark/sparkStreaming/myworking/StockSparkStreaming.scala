package spark.sparkStreaming.myworking

import java.util.regex.{Matcher, Pattern}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object StockSparkStreaming {
  def main(args: Array[String]): Unit = {
    //初始化spark
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("stock_Streaming")
    //初始化context
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    //定义初始化参数
    val topics: Set[String] = Set("stock1", "stock2")
    val brokers = "master:9092"
    val kafkaparams: Map[String, String] = Map("metadata.broker.list" -> brokers)
    //设置checkPoint
    ssc.checkpoint("E:\\project\\bigdata\\sparkmv\\data\\checkPoint")

    //自定义offset
    val fromoffsets: Map[TopicAndPartition, Long] = StockOffsetUtils.verify(topics)


    //转换类型
    val messageHandler: MessageAndMetadata[String, String] => (String, String) =
      (msg: MessageAndMetadata[String, String]) => (msg.key(), msg.message())

    //消费者
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc,
      kafkaparams,
      fromoffsets,
      messageHandler
    )

    //充值总金额
    kafkaDS.map { case (key, value) => {
      value.split("\"")(11).toLong
    }
    }
      .reduce(_ + _)
      .map { value =>
        val timee: Long = System.currentTimeMillis()
        StockHbaseUtils.saveDataToHbase(timee.toString, "sumMoney5", value.toString)
        value
      }
      .print(3)

    //每个批次充值的平均金额（批次提交设置为5秒）
    kafkaDS.map { case (key, value) => {
      (value.split("\"")(11).toLong, 1)
    }
    }.reduce { case ((bf1, bf2), (value1, value2)) => {
      (bf1 + value1, bf2 + value2)
    }
    }.map { case (sum, count) => {
      if (count != 0) {
        sum / count
      } else {
        0
      }
    }
    }
      .map{value => {
        val timee: Long = System.currentTimeMillis()
        StockHbaseUtils.saveDataToHbase(timee.toString , "avgMoney5" , value.toString)
        value
      }}
        .print(3)

    //求最后10分钟的充值总金额，每分钟刷新
    kafkaDS.map { case (key, value) => {
      value.split("\"")(11).toLong
    }
    }.reduceByWindow((k1, v1) => k1 + v1, (k2, v2) => k2 - v2, Seconds(10), Seconds(5))
      .map{value => {
        val timee: Long = System.currentTimeMillis()
        StockHbaseUtils.saveDataToHbase(timee.toString , "sumMoneyWin10" , value.toString)
        value
      }}
      .print(3)

    //存offset
    kafkaDS.foreachRDD((rdd, time) => {
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      StockOffsetUtils.saveOffsetToRedis(ranges)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
