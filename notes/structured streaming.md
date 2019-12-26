## 介绍

- 基础：基于sparksql的结构流
- 可扩展，可容错，基于Spark SQL执行引擎的流处理引擎。

- 编程模型

  流数据，不断向一张无界表追加数据，表的数据就是要处理的数据流数据。

  ![structured-streaming-stream-as-a-table](D:\software\typoraIMG\structured-streaming-stream-as-a-table.png)

- 支持操作

  可以在Spark SQL上引擎上使用DataSet/DataFrame API处理流数据的聚集，事件时间窗口，和流与批次的连接操作等。Structured Streaming 可以实现端到端的exactly once语义，以及通过检查点机制和WAL机制实现对容错的支持。

- 数据流程

  整个过程是一个时间过程，每一次处理都是一个query

   input → query → result → output    

### output模式

- Complete：对应**upstateByKey**，

  批次所有的结果，不删除任何数据，每次触发输出所有数据

- Append：对应每一个**批次的处理**结果，

  追加，输出这次批次的所有结果，然后删除窗口，保证数据不重复

- update：对应**mapWithState**，

  输出和上一次结果相比而改变的数据，删除不再更新的时间窗口

![structured-streaming-model](D:\software\typoraIMG\structured-streaming-model.png)

![structured-streaming-example-model](D:\software\typoraIMG\structured-streaming-example-model.png)



### Time

- 时间

1. eventTime（）

   ```scala
   streamingDf
     .withWatermark("eventTime", "10 seconds")//等待允许延迟的时间
     .dropDuplicates("guid", "eventTime")
   ```

2. ProcessingTime（）

   ```scala
   df.writeStream
     .format("console")
     .trigger(Trigger.ProcessingTime("2 seconds"))
     .start()
   ```

3. （真正的处理时间）







- 在需求时间范围之前拿到的数据，

- 在需求时间范围内产生但是拿到已经超时。

- 需求9.15到10.15

  真正拿到数据收集的时间不确定性，延迟，

  解决：增加等待时间（natetime），超过阈值还没到的数据呐就不考虑了，

- 通过Watermark（水位线）判断是否超过范围=当前系统能看到的最大eventtime - 延迟时间

  通过Watermark判断哪些数据还可以加入到窗口，哪些数据是要抛的，判断哪些窗口不会有数据再加入

  如果数据的时间小于Watermark，那么丢掉，如果在范围内或者高于Watermark就加入窗口进行计算。

  1. max eventtime：当前窗口内（能看到的所有）的最大值

- **最大eventtime**的判断是当前窗口的最前端的前面所有数据中的eventtime的最大值（水位是按照滑动间隔来算的）

  

  

  - 窗口长度10m
  - 滑动间隔5m
  - 延迟时间10m

  ![structured-streaming-watermark-update-mode](D:\software\typoraIMG\structured-streaming-watermark-update-mode.png)

![structured-streaming-watermark-append-mode](D:\software\typoraIMG\structured-streaming-watermark-append-mode.png)



### 注意事项

- 不支持的操作
  1. 聚合，
  2. limit，take(N)
  3. distinct，
  4. sorting（必须是聚合的结果，还必须是complete的模式做output才能sort）
  5. Outer join：
     - 不支持两个流式进行，
     - 左外要求左边是流，另一边的是离线数据集
     - 右外要求右边是流，另一边的是离线数据集
     - 任何形式都不支持两个流式join
  6. count：无法直接使用，需要先做分组再count
  7. foreach：含义是自定义下沉
  8. show：不支持，想使用要下沉到控制台







## source

### 语法

- writeStream：写streaming
- readStream：读streaming
- .outputMode("append")：输出的类型
- .format("console")：定义输入源source和输出源sink





- 四种数据源

### file

- json跟目录名

```scala
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ss")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    //d定义结构
    val structType: StructType = new StructType()
      .add("name", "string")
      .add("age", "int")
      .add("session", "string")

    //读文件
    val df: DataFrame = spark.readStream
      .schema(structType)
      .option("maxFilePerTrigger", 100)
      .option("latestFirst", value = true)
      .option("fileNameOnly", value = true)
      .json("E:\\project\\bigdata\\sparkmv\\data\\json")

    //建立逻辑结构
    val query: StreamingQuery = df.writeStream.outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
```



### socket

- <font color=red> **append**</font>：可以直接输出

  ```scala
  // 创建监听 localhost:8055 的DataFrame流
      val line: DataFrame = spark.readStream
        .format("socket")
        .option("host", "slave8")
        .option("port", 8055)
        .load()
        
  //没有数据处理直接执行
      val query: StreamingQuery = line.writeStream
        .outputMode("append")
        .format("console")
        .start()
  ```

- <font color=red> **complete**</font>：必须做聚合操作

  ```scala
  // 创建监听 localhost:8055 的DataFrame流
      val line: DataFrame = spark.readStream
        .format("socket")
        .option("host", "slave8")
        .option("port", 8055)
        .load()
  
      import spark.implicits._
      val result: DataFrame = line.as[String]
        .flatMap(_.split("\\s+"))
        .groupBy("value").count()
  
      //没有数据处理直接执行
      val query: StreamingQuery = result.writeStream
        .outputMode("complete")
        .format("console")
        .start()
  
      query.awaitTermination()
  ```

- <font color=red> **update**</font>：可以没有聚合操作直接输出

### rate

每秒指定行数生成数据

```scala
val rate = spark.readStream
.format("rate")
// 每秒生成的行数，默认值为1
.option("rowsPerSecond", 10)
.option("numPartitions", 10)
.option("rampUpTime",0)
.option("rampUpTime",5)
.load()
```



### kafka

#### 流处理

```scala
    val spark: SparkSession = SparkSession.builder()
      .appName("kafkaSS")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._
    val lines: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "master:9092")
      .option("subscribe", "topica")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    lines

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val result: StreamingQuery = wordCounts
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
    
    result
      .awaitTermination()

```



#### 批处理

- 不指定

```scala
    import spark.implicits._
    val lines: Dataset[(String , String)] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "master:9092")
      .option("subscribe", "topica")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String , String)]
```

- 指定offset

```scala
val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1,topic2")
      .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
      .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
```

- 最早和最迟的offset

```scala
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
```





## query

- sparksql，sql和DSL两种形式都支持



## sink

- .format("console")
- .format("parquet")
- .format("csv")
- .format("orc")
- .format("json")

### file

```scala
    val query: StreamingQuery = result.writeStream
      .format("csv")
      .option("path", "E:\\project\\bigdata\\sparkmv\\data\\json\\sorket")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()
```

### Console

- .option("numRows", 30)：显示条数
- .option("truncate", value = false)：是否压缩显示

### Memory



```scala
    //输出到内存,必须指定表名
    val query: StreamingQuery = line.writeStream
      .format("memory")
      .queryName("memoryTable")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()

    //必须定义不然无法重复查询,必须线程
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          spark.sql("select * from memoryTable").show(false)
          Thread.sleep(1000)
        }
      }
    }).start()
```



### Kafka

#### 流处理

```scala
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val kafkaSink = source.select(array(to_json(struct("*"))).as("value").cast(StringType),
      $"timestamp".as("key").cast(StringType)).writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .option("topic", "hiacloud-ts-dev")
      .start()
    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    val ds = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "topic1")
      .start()
    // Write key-value data from a DataFrame to Kafka using a topic specified in the data
    val ds = df
      .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .start()
```



#### 批处理

```scala
    // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "topic1")
      .save()
    // Write key-value data from a DataFrame to Kafka using a topic specified in the data
    df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .save()
```



### Foreach

- foreach的每条记录，继承ForeachWriter[Row]，实现open（目的源、获取连接），process（拿到的row，写到存储），close

```scala
import java.sql.{Connection, DriverManager, Statement}
import java.util.UUID
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.types.StructType

class JDBCSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: Row): Unit = {
    statement.executeUpdate("INSERT INTO zip_test " +
      "VALUES (" + value(0) + "," + value(1) + ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}

object sss {
  def main(args: Array[String]): Unit = {
    // 创建Spark程序入口
    val sparkSession = SparkSession
      .builder()
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    // val frame: DataFrame = lines.toDF("id","name","age")
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val source = sparkSession
      .readStream
      // Schema must be specified when creating a streaming source DataFrame.
      .schema(userSchema)
      // 每个trigger最大文件数量
      .option("maxFilesPerTrigger", 100)
      // 是否首先计算最新的文件，默认为false
      .option("latestFirst", value = true)
      // 是否值检查名字，如果名字相同，则不视为更新，默认为false
      .option("fileNameOnly", value = true)
      .json("jsonres")
    val url = "jdbc:mysql://mysqlserverurl:3306/test"
    val user = "user"
    val pwd = "pwd"
    val writer = new JDBCSink(url, user, pwd)
    val query =
      source.writeStream.foreach(writer)
        .outputMode("update")
        .trigger(ProcessingTime("25 seconds"))
        .start()
  }
}
```



