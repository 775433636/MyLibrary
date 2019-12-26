package spark.sparkStreaming.kafka

import java.util.Properties

import org.apache.kafka.clients.producer._

object Kafka {
  def main(args: Array[String]): Unit = {
    //定义kafka的参数
    val brokers = "master:9092"
    val topic = "topica"
    val prop = new Properties()
    //生产者对象，下面三个参数必选
    prop.put("bootstrap", brokers)
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "5000000")
    //构建拦截链
//    val interceptors = new ListBuffer[String]

    val interceptors: List[String] = List("com.bigdata.sparkstreaming.kafka.MyTimeInterceptor", "com.bigdata.sparkstreaming.kafka.MyCounterInterceptor")

//    prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG , interceptors)

//    prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG , "com.bigdata.sparkstreaming.kafka.MyTimeInterceptor")


    //kafkProducer，创建者生产者对象
    val producer = new KafkaProducer[String, String](prop)



    //kafkarecorder(异步发送数据)
    for (i <- 1 to 88) {
      //异步直接将消息send出去发送消息
      val msg = new ProducerRecord[String, String](topic, i.toString, i.toString+ i.toString)
      producer.send(msg, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            println(msg)
          } else {
            exception.printStackTrace()
          }
        }
      })
      
//      producer.send(msg)
      //同步就是调用get方法以阻塞方式获取结果
//      val metadata: RecordMetadata = producer.send(msg).get()
//      println(s"parittion = ${metadata.partition()} ; offset = ${metadata.offset()} ,topic ${metadata.topic()}")
    }
    producer.close()
  }
}
