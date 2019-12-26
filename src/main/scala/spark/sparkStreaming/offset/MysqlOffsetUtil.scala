package spark.sparkStreaming.offset

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange
import scalikejdbc._

object MysqlOffsetUtil {
  private val jdbcUrl =  "jdbc:mysql://slave4:3306/spark"
  private val jdbcUser = "root"
  private val jdbcPassword = "wang1997"

  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

  def getOffsetsFromSQL(): Map[TopicAndPartition, Long] = {
    //建立连接
    using(DB(ConnectionPool.borrow())) { db =>
      //进行查询
      db.readOnly(implicit session =>
        //sql
        sql"select topic, partid, offset from mytopic"
          .map(r => TopicAndPartition(r.string(1), r.int(2)) -> r.long(3))
          //tomap后留最后的相同key，直接从历史数据中得到了最新的topic集合
          .list.apply().toMap)
    }
  }

  // 批量插入. 在一个事务中保存结果 和 offsets
  def saveResultAndOffsets(keypagsMyTimeVisitCountsMap: Iterator[(String, Option[Int])],diffJourneyMapData:Iterator[(String, Option[Int])], offsets: Array[OffsetRange]): Unit = {
    using(DB(ConnectionPool.borrow())) { db =>
      db.localTx { implicit session =>
        // 增加了异常处理（字符串到数字）. 先过滤，然后从Some中把值取出来
        //5 分钟内小于最短访问间隔（自设）的关键页面查询次数
        val mydataParams: List[Seq[Any]] = keypagsMyTimeVisitCountsMap.filter(_._2.isDefined).map(x => Seq(x._1, x._2.get)).toList
        sql"insert into keypagsMyTimeVisitCountsMap(ip, result) values (?, ?)"
          .batch(mydataParams: _*).apply()

        //不同行程次数的结果
        val mydiffJourneyMapDataParams: List[Seq[Any]] = diffJourneyMapData.filter(_._2.isDefined).map(x => Seq(x._1, x._2.get)).toList
        sql"insert into diffJourneyMapData(ip, result) values (?, ?)"
          .batch(mydiffJourneyMapDataParams: _*).apply()

        //topic结果
        val mytopicParams: Array[Seq[Any]] = offsets.map(x => Seq(x.topic, x.partition, x.untilOffset))
        sql"insert into mytopic (topic, partid, offset) values (?, ?, ?)"
          .batch(mytopicParams: _*).apply()
      }
    }
  }

  // 批量插入,仅保存结果
  def saveResult(data: Iterator[String]): Unit = {
    using(DB(ConnectionPool.borrow())) { db =>
      db.localTx { implicit session =>
        val mydataParams = data.map(x => Seq(x.split(",")(0).trim, x.split(",")(1).trim, x.split(",")(0).trim)).toList
        sql"insert into myorders(name, orderid) values (?, ?) ON DUPLICATE KEY UPDATE name=?"
          .batch(mydataParams: _*).apply()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val a = getOffsetsFromSQL()
    println(a)

//    val b = Iterator("name1, 1", "name2, 2", "name3, 3")
//    saveResult(b)
  }
}
