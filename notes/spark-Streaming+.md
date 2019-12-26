## work

### result&offset&ACID

#### scalikeJDBC

- scalikeJDBC方法类

```scala
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
}
```

- 调用

```scala
    //TODO 自定义存储offset
    kafkaDS.foreachRDD((rdd,time) => {
      if (!rdd.isEmpty()){
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //存储正确的结果和offset，将数据库改错看看事务是否生效，程序是否终止，offset是否移动，是否成功从开始的offset开始重新计算
        //存了两个结果和一个offset，整个项目全部结果做成事务也是一样的，全添加进来也是一样的
//        有一个隐患，ssc的执行时间是两秒，
        MysqlOffsetUtil.saveResultAndOffsets(
          keypagsMyTimeVisitCountsMap.map{case (ip , count) => (ip , Option(count.toInt))}.iterator ,
          diffJourneyMap.map{case (ip , count) => (ip , Option(count))}.iterator,
          ranges)

        println(s"所有topic的总：  + ${rdd.count()} +  :  + $time")
      }
    })
```

#### JDBC

- 驱动类

```scala
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
```



- 方法类：存储临时表、存储结果表

```scala
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
```

- 结果存临时表

```scala
//TODO 将第六个指标的结果先存到mysql的临时表中
MysqlJDBCOffsetUtil.saveResultKeypagsMyTimeVisitCountsMap(keypagsMyTimeVisitCountsMap)
```

- offset与临时表导入临时表一起存

```scala
//第二种方法，将offset和临时表的数据导入到最终表并且保存offset
MysqlJDBCOffsetUtil.saveResultAndOffsets(ranges)
```

