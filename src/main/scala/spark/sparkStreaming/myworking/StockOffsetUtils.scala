package spark.sparkStreaming.myworking

import java.util

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.{immutable, mutable}

/**
  * 设计
  *   1.key：prefix+topic+groupid
  *   2.field：partition
  *   3.value：offset
  * 功能
  *   1.存offset
  *   2.取offset
  *   3.校验offset
  */

object StockOffsetUtils {
  //常量
  private val prefix = "stockOffset"
  private val dilimeter = "-"
  private val host = "slave4"
  private val port = 6379

  //获得key的方法
  def getKey(topic: String, groupid: String = ""): String = {
    s"$prefix$dilimeter$topic$dilimeter$groupid"
  }

  //redis连接池
  def getRedisConnecter(): Jedis = {
    val pool = new JedisPool(host, port)
    val resource: Jedis = pool.getResource
    resource
  }

  //存offset，不需要传key，被坑过
  def saveOffsetToRedis(ranges: Array[OffsetRange], groupid: String = ""): Unit = {
    val jedis: Jedis = getRedisConnecter()
    //遍历ranges每个分区得到一个map供gmset使用
    ranges.map { case (range) =>
      //获取key
      val key: String = getKey(range.topic, groupid)
      //        println(s"topic ${range.topic}   partition ${range.partition}   offset  ${range.untilOffset}")
      //上传
      jedis.hset(key, range.partition.toString, range.untilOffset.toString)
    }
    jedis.close()
  }

  //取offset
  def getOffsetFromRedis(topics: Set[String], groupid: String = ""): Map[TopicAndPartition, Long] = {
    val jedis: Jedis = getRedisConnecter()
    //得到所有offset
    val set_resultMap: Set[mutable.Map[TopicAndPartition, Long]] = topics.map(topic => {
      val key: String = getKey(topic)
      //从redis
      val field_value: util.Map[String, String] = jedis.hgetAll(key)
      import scala.collection.JavaConversions._
      //得到一个topic的所有offset
      val resultMap: mutable.Map[TopicAndPartition, Long] = field_value.map { case (partition, offset) => {
        println(s"取到的  topic : $topic   partition：$partition   offset : $offset")
        (TopicAndPartition(topic, partition.toInt), offset.toLong)
      }
      }
      resultMap
    })
    //学好了这个操作
    set_resultMap.flatten.toMap
  }

  //获得cluster的最大最小offset,这里放一个topic而不是Set[String]因为后续校验取的是每一个的topic对应的最大最小
  def getMaxMinOffset(topic: String): (Array[OffsetRange], Array[OffsetRange]) = {
    val kafkaParams = Map("metadata.broker.list" -> "master:9092")
    val kc = new KafkaCluster(kafkaParams = kafkaParams)
    val tp: Set[TopicAndPartition] = kc.getPartitions(Set(topic)).right.get
    //KafkaCluster.LeaderOffset中存的offset还有配置参数
    val max: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = kc.getLatestLeaderOffsets(tp).right.get
    val min: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = kc.getEarliestLeaderOffsets(tp).right.get

    //返回Array[OffsetRange]便于后面的需要
    val maxOffset: Array[OffsetRange] = max.map { case (tp, kclo) => {
      OffsetRange(tp, 0, kclo.offset)
    }
    }.toArray

    val minOffset: Array[OffsetRange] = min.map { case (tp, kclo) => {
      OffsetRange(tp, 0, kclo.offset)
    }
    }.toArray
    (maxOffset, minOffset)
  }

  //校验
  def verify(topics: Set[String], groupid: String = ""): Map[TopicAndPartition, Long] = {
    //得到cluster的最大最小offset
    //得到获取的值
    val getTPO: Map[TopicAndPartition, Long] = getOffsetFromRedis(topics)
    val result: Set[immutable.IndexedSeq[(TopicAndPartition, Long)]] = topics.map { topic => {
      //没得一个topic对应的最大最小offset
      val (maxOffset, minOffset): (Array[OffsetRange], Array[OffsetRange]) = getMaxMinOffset(topic)
      for (index <- 0 to maxOffset.length - 1) yield {
        //取到每一个分区对应的range,然后取需要的值
        val rangeMax: OffsetRange = maxOffset(index)
        val max: Long = rangeMax.untilOffset
        val tp: String = rangeMax.topic
        val par: Int = rangeMax.partition

        val rangeMin: OffsetRange = minOffset(index)
        val min: Long = rangeMin.untilOffset
        //随便挑大或小的offset封装找一个组成topicAndPartition就行
        val topicAndPartition = TopicAndPartition(tp, par)
        val myoffset: Long = getTPO.getOrElse(TopicAndPartition(tp, par), 0)
        //做比较
        myoffset match {
          case myoffset if (myoffset > max) => topicAndPartition -> max
          case myoffset if (myoffset < min) => topicAndPartition -> min
          case _ => topicAndPartition -> myoffset
        }
      }
    }
    }
    result.flatten.toMap
  }
}
