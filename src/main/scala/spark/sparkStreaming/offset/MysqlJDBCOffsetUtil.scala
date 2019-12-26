package spark.sparkStreaming.offset

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange

object MysqlJDBCOffsetUtil {
  //驱动类
  object DBUtils {

    val url = "jdbc:mysql://slave4:3306/spark"
    val username = "root"
    val password = "wang1997"

    classOf[com.mysql.jdbc.Driver]

    def getConnection(): Connection = {
      DriverManager.getConnection(url, username, password)
    }

    def close(conn: Connection): Unit = {
      try {
        if (!conn.isClosed || conn != null) {
          conn.close()
        }
      }
      catch {
        case ex: Exception =>
          ex.printStackTrace()
      }
    }
  }

  //Select operator
  //查询得到offset
  def query():  Map[TopicAndPartition, Long] = {
    val conn: Connection = DBUtils.getConnection()
    try{
      //定义sql
      val sql: StringBuilder = new StringBuilder()
        .append("select topic , partid , max(offset) as offset")
        .append("  FROM mytopic")
        .append(" group by topic , partid")
      val pstm: PreparedStatement = conn.prepareStatement(sql.toString())
      val rs: ResultSet = pstm.executeQuery()

      val rsmd: ResultSetMetaData = rs.getMetaData
      val size: Int = rsmd.getColumnCount
//      val map :scala.collection.mutable.Map[TopicAndPartition, Long] = scala.collection.mutable.Map(TopicAndPartition("" , 0)-> 0)
      val map: Map[TopicAndPartition, Long] = (for (i <- 1 to 3) yield {
        rs.next()
        val column: Array[Int] = (1 to size).toArray
        TopicAndPartition(rs.getString("topic"), rs.getInt("partid")) -> rs.getLong("offset")
      }).toMap
      map
    }
    finally {
      conn.close()
    }
  }

  //存储offset和结果
  def saveResultAndOffsets(offsets : Array[OffsetRange]): Unit = {
    val conn = DBUtils.getConnection()
    try{
      //offset的存储
      offsets.foreach(or => {
        //是scala的集合
        val offsetSQL = new StringBuilder()
          .append("INSERT INTO mytopic(topic , partid , offset)")
          .append("     VALUES(?, ? , ?)")
        val pstm = conn.prepareStatement(offsetSQL.toString())
        pstm.setString(1 , or.topic)
        pstm.setInt(2 , or.partition)
        pstm.setInt(3 , or.untilOffset.toInt)
        pstm.executeUpdate() > 0
        val res: Array[Int] = pstm.executeBatch()
        res.foreach(r => {
          if (r == 0) {
            conn.rollback()
            println("语句一错误，进行回滚")
          }
        })
      })

      //结果的零时表转存
      //设计思路：临时表的字段不变，为ip和结果，在offset抽结果转存的时候，为了结果表只增不减，只能是将结果抽出来存好后将零时表删除，
      //否则只能在零时表加时间戳等，然后条件查询，比较麻烦，
      val keypagsMyTimeVisitCountsMapSQL ="insert into keypagsMyTimeVisitCountsMap select * from keypagsMyTimeVisitCountsMapTemp"
      val pstm2 = conn.prepareStatement(keypagsMyTimeVisitCountsMapSQL)
      pstm2.executeUpdate() > 0
      val res2: Array[Int] = pstm2.executeBatch()
      res2.foreach(r => {
        if (r == 0) {
          conn.rollback()
          println("语句一错误，进行回滚")
        }
      })

      //每次更新完将零时表数据清空
      val keypagsMyTimeVisitCountsMapTRUNCATESQL ="truncate table keypagsMyTimeVisitCountsMapTemp"
      val pstm3: PreparedStatement = conn.prepareStatement(keypagsMyTimeVisitCountsMapTRUNCATESQL)
      pstm3.executeUpdate() > 0
      val res3: Array[Int] = pstm3.executeBatch()
      res3.foreach(r => {
        if (r == 0) {
          conn.rollback()
          println("语句一错误，进行回滚")
        }
      })


      //问题字段
      conn.setAutoCommit(false)
      //提交事务
      conn.commit()
    }catch {
      case e:SQLException => {
        conn.rollback()
        println("出现错误，回滚")
        throw new SQLException(e)
      }
    }finally {
//      println("last")
    }
  }

  //将结果存为零时表
  def saveResultKeypagsMyTimeVisitCountsMap(keypagsMyTimeVisitCountsMap: collection.Map[(String,String), Int]): Unit = {
    val conn = DBUtils.getConnection()
    try{
      keypagsMyTimeVisitCountsMap.foreach(result => {
        val offsetSQL = new StringBuilder()
          .append("INSERT INTO keypagsMyTimeVisitCountsMapTemp(ip , result)")
          .append("     VALUES(?, ?)")
        val pstm = conn.prepareStatement(offsetSQL.toString())
        pstm.setString(1 , result._1._1)
        pstm.setInt(2 , result._2.toInt)
        pstm.executeUpdate() > 0
        val res: Array[Int] = pstm.executeBatch()
        res.foreach(r => {
          if (r == 0) {
            conn.rollback()
            println("语句一错误，进行回滚")
          }
        })
      })
      conn.setAutoCommit(false)

      conn.commit()
    }catch {
      case e:SQLException => {
        conn.rollback()
        println("出现错误，回滚")
        throw new SQLException(e)
      }
    }finally {
//      println("last")
    }
  }



  def main(args: Array[String]): Unit = {
    println(query())
//    saveResultKeypagsMyTimeVisitCountsMap(Map("192.168.111.520" -> 5))
//    saveResultAndOffsets

  }
}
