package spark.sparkStreaming.offset

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.immutable


/**
  * 使用Redis保存offset，数据保存在HASH中，
  * key :前缀 + topic + groupid （分隔符）
  * fields：partition
  * value：offset
  * 1.获取连接
  * 2.从redis读数
  * 3.将数据保存到redis
  * 4.offset的校验（锦上添花， 最难）
  */
object RadisUtils {
  private val predix = "myBigdataOffset"
  private val delimiter = ":"
  private val redisHost = "slave4"
  private val redisPort = 6379

  //定义Redis连接池
  private val config: JedisPoolConfig = new JedisPoolConfig
  //  private val config = new GenericObjectPoolConfig
  //最大连接数

  //最大空闲连接数

  private val jedisPool = new JedisPool(config, redisHost, redisPort)
  private val resource: Jedis = jedisPool.getResource

  //获取key的方法
  def getKey(topic: String, groupId: String = ""): String = {
    s"$predix:$topic:$groupId"
  }

  //从redis中获取offset
  def getOffsetFromRedis(topics: Set[String], groupId: String = ""): Map[TopicAndPartition, Long] = {
    val jedis: Jedis = resource
    //从所有topic遍历
    val redisSetMap: Set[Map[TopicAndPartition, Long]] = topics.map(topic => {
      import scala.collection.JavaConversions._
      val redisMap: Map[String, String] = jedis.hgetAll(getKey(topic, groupId)).toMap
      //      jedis.close()
      //数据类型转换
      redisMap.map { case (partition, num) => {
//        println(s"获取到:${topic}     partiiont :${partition}")
        TopicAndPartition(topic, partition.toInt) -> num.toLong
      }
      }
    })
    jedis.close()
    //这里细节了
    redisSetMap.flatten.toMap
  }

  //将数据保存到redis
  def saveOffsetToRedis(ranges: Array[OffsetRange], groupid: String = ""): Unit = {
    //获取连接
    val jedis: Jedis = resource
    //获取key
    //获取partition， offsets
    //后面再改成一批处理
    //    val mapRes: mutable.Map[String, String] = scala.collection.mutable.Map[String , String]()
    ranges.map(range => {
      val key: String = getKey(range.topic)
//      println(s"当前${range.topic}的offset：${range.untilOffset}   partition：${range.partition}")
      jedis.hset(key, range.partition.toString, range.untilOffset.toString)
    })
    //这里不能关闭
    //        jedis.close()
  }


  //获取集群最大最小offset的方法
  def getMaxMinOffset(topics: String): (Array[OffsetRange], Array[OffsetRange]) = {
    //利用0.8接口中的方法获取offset的最大值最小值
    val kafkaParames = Map("metadata.broker.list" -> "master:9092")
    val kc = new KafkaCluster(kafkaParames)

    //获取最大值需要partition
    val topicsSet: Set[String] = Set(topics)
    val partitions: Set[TopicAndPartition] = kc.getPartitions(topicsSet).right.get

    //利用kc获取最大最小值
    val max: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = kc.getLatestLeaderOffsets(partitions).right.get
    val min: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = kc.getEarliestLeaderOffsets(partitions).right.get

    //数据类型转换为和redis获取中的类型一致
    //Map[TopicAndPartition , Long]
    //case class LeaderOffset(host: String, port: Int, offset: Long)
    //将最大值转换为需要的形式
    //因为看一下获取offset方法的返回值的key是OffsetRange，那么最后的检验肯定的取值，就需要返回OffsetRange
    val maxOffset: Array[OffsetRange] = max.toArray.map { case (tp, LeaderOffset(host, port, offset)) => {
      OffsetRange(tp, 0, offset)
    }
    }
    //将最小值转换为需要的形式
    val minOffset: Array[OffsetRange] = max.toArray.map { case (tp, LeaderOffset(host, port, offset)) => {
      OffsetRange(tp, 0, offset)
    }
    }
    //返回结果
    (maxOffset, minOffset)
  }

  //校验方法
  def verify(topics: Set[String], groupid: String = ""): Map[TopicAndPartition, Long] = {
    //获取得到的值
    val getOffset: Map[TopicAndPartition, Long] = getOffsetFromRedis(topics)
    val resultRedis: Set[immutable.IndexedSeq[(TopicAndPartition, Long)]] = topics.map { topic => {
      //得到kafka集群的最大最小值
      val (maxOffset, minOffset): (Array[OffsetRange], Array[OffsetRange]) = getMaxMinOffset(topic)
      //此时的情况就是如果消费者挂掉了，那么kafka的操作没法控制，那么如果kafka的partition增加，那么就是kafka的partition会更大
      val result: immutable.IndexedSeq[(TopicAndPartition, Long)] = for (i <- 0 to maxOffset.length - 1) yield {
        val topicAndPartition: TopicAndPartition = TopicAndPartition(maxOffset(i).topic, maxOffset(i).partition)
        //先得到自己存储的数据,但是取值是需要TopicAndPartition的，那么就对集群进行处理，因为是以其为基本操作的↑↑↑↑↑↑
        val offset: Long = getOffset.getOrElse(topicAndPartition, 0)
        //得到集群的最大值最小值
        val max: Long = maxOffset(i).untilOffset
        val min: Long = minOffset(i).untilOffset
//        println(s"maxOffset: ${max}   minOffset: ${min}")
        //逻辑判断，大于给大，小于给小，否则就是offset原值
        offset match {
          case offset if (offset > max) => topicAndPartition -> max
          case offset if (offset < min) => topicAndPartition -> min
          case _ => topicAndPartition -> offset
        }
      }
      result
    }
    }
    resultRedis.flatten.toMap
  }

  //  def main(args: Array[String]): Unit = {
  //    println(getOffsetFromRedis("topica", ""))
  //
  //    //    val ranges: Array[OffsetRange] = Array[OffsetRange](
  //    //      OffsetRange("topica",0,0,2) ,
  //    //      OffsetRange("topica",1,0,2) ,
  //    //      OffsetRange("topica",2,0,2)
  //    //    )
  //    //    saveOffsetToRedis(ranges)
  //
  //    println(getMaxMinOffset("topica"))
  //  }

}
