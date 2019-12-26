package spark.sparkStreaming.myworking

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object StockKafkaProducer {
  def main(args: Array[String]): Unit = {
    //producer参数
    val brokers = "master:9092"
    val topic1 = "stock1"
    val topic2 = "stock2"
    val properties = new Properties()
    //producer对象
    properties.put("bootstrap",brokers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    //创建生产者对象
    val producer: KafkaProducer[String, String] = new KafkaProducer[String , String](properties)

    //异步带回调发送数据
    for (key <- 1 to 100){
      val msg1 = new ProducerRecord[String , String](topic1 , key.toString , "\"openid\":\"opEu45VAwuzCsDr6iGIf4qhnUZUI\",\"phoneNum\":\"18334834567\",\"money\":\"100\",\"date\":\"2018-09-13T02:15:16.054Z\",\"lat\":39.688011,\"log\":116.066689")
      val msg2 = new ProducerRecord[String , String](topic2 , key.toString , "\"openid\":\"opEu45VAwuzCsDr6iGIf4qhnUZUI\",\"phoneNum\":\"18334834567\",\"money\":\"122\",\"date\":\"2018-09-13T02:15:16.054Z\",\"lat\":39.688011,\"log\":116.066689")
      val rantime: Double = Math.random()*10000
      Thread.sleep(rantime.toLong)
      producer.send(msg1 , new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null){
            println(msg1)
          }else{
            exception.printStackTrace()
          }
        }
      })

      producer.send(msg2 , new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null){
            println(msg2)
          }else{
            exception.printStackTrace()
          }
        }
      })
    }
    producer.close()
  }
}
