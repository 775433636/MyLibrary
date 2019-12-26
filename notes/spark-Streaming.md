# Spark-Streaming

## 概述

- Spark Streaming用于流式数据的处理。Spark Streaming支持的数据输入源很多，例如：Kafka、Flume、Twitter、ZeroMQ和简单的TCP套接字等等。数据输入后可以用Spark的高度抽象原语如：map、reduce、join、window等进行运算。而结果也能保存在很多地方，如HDFS，数据库等。

![451](D:\software\typoraIMG\451-1573259348721.png)

- 和Spark基于RDD的概念很相似，Spark Streaming使用离散化流(discretized stream)作为抽象表示，叫作DStream。DStream 是随时间推移而收到的数据的序列。在内部，每个时间区间收到的数据都作为 RDD 存在，而DStream是由这些RDD所组成的序列(因此得名“**离散化**”)。

### 特点

1. 高吞吐(mini batch)
2. 高延迟
3. 易用
4. 容错
5. 易整合到spark体系

### 架构

![4444](D:\software\typoraIMG\4444.png)

- executer：receiver（可选），接收数据，不参与计算；本地放一份，远程节点放一份

- Driver：JobGenerator（定时启动streaming job），ReceiverTrancker（管理receiver接收到的数据）

- RateController 实现自动调节数据的传输速率

  如何启用？
  在 Spark 启用反压机制很简单，只需要将 spark.streaming.backpressure.enabled 设置为 true 即可，这个参数的默认值为 false,还带有一系列的参数调整

  ```scala
  ● spark.streaming.backpressure.initialRate： 启用反压机制时每个接收器接收第一批数据的初始最大
  速率。默认值没有设置。
  ● spark.streaming.backpressure.rateEstimator：速率估算器类，默认值为 pid ，目前 Spark 只支持
  这个，大家可以根据自己的需要实现。
  ● spark.streaming.backpressure.pid.proportional：用于响应错误的权重（最后批次和当前批次之间
  的更改）。默认值为1，只能设置成非负值。weight for response to “error” (change between last
  batch and this batch)
  ● spark.streaming.backpressure.pid.integral：错误积累的响应权重，具有抑制作用（有效阻尼）。
  默认值为 0.2 ，只能设置成非负值。weight for the response to the accumulation of error. This has
  a dampening effect.
  ● spark.streaming.backpressure.pid.derived：对错误趋势的响应权重。 这可能会引起 batch size 的
  波动，可以帮助快速增加/减少容量。默认值为0，只能设置成非负值。weight for the response to the
  trend in error. This can cause arbitrary/noise-induced fluctuations in batch size, but can also help
  react quickly to increased/reduced capacity.
  ● spark.streaming.backpressure.pid.minRate：可以估算的最低费率是多少。默认值为 100，只能设
  置成非负值。
  ```

  

- **作业流程**：
  1. 启动作业，获取资源（executor）
  2. executor会有**receiver**（可选）负责接收数据，作为一个长期运行的task跑在executor上，
  3. 接收的数据备份到其他节点一份，同时会向Driver汇报
  4. Driver负责管理接收达到的数据块
  5. **JobGenerator**负责定时启动job
  6. DAG → Stage →Task，完成后单个的batch才算完成

![搜狗截图20191112170526](D:\software\typoraIMG\搜狗截图20191112170526.png)

### 对比Strom

![搜狗截图20191112171112](D:\software\typoraIMG\搜狗截图20191112171112.png)

- Spark Streaming 与 Strom
  1、同一套系统，安装spark之后就一切都有了

  2、spark较强的容错能力；strom使用较广、更稳定

  3、storm是用Clojure语言去写的，它的很多扩展都是使用java完成的

  4、任务执行方面和strom的区别是：

  - spark steaming数据进来是一小段时间的RDD，数据进来之后切成一小块一小块进行批处理
  - storm是基于record形式来的，进来的是一个tuple，一条进来就处理一下

  5、中间过程实质上就是spark引擎，只不过sparkstreaming在spark之后引擎之上动了一点手脚：对进入spark引擎之前的数据进行了一个封装，方便进行基于时间片的小批量作业，交给spark进行计算

### 离散数据流

- Spark Streaming最主要的抽象是 DStream（Discretized Stream，离散化数据流），表示连续不断的数据流。
- 在内部实现上，Spark Streaming的输入数据 按照时间片（如1秒）分成一段一段，每一段数据转换为Spark中的 RDD，这些分段就是DStream，并且对DStream的操作都最终转变为 对相应的 RDD 的操作。

- Dstream
  1. 一个数据抽象，代表一个数据流，是在时间序列上的封装
  2. 可以做整体转换，一个操作结束后转换另外一种DStream，默认存储为<内存+磁盘>
  3. 可以进行窗口操作
  4. 使用persist()函数进行序列化（KryoSerializer）



### 计算过程

- 对数据的操作是按照RDD为单位进行的

![1111](D:\software\typoraIMG\1111.png)

- 计算过程由Spark engine来完成

![5421](D:\software\typoraIMG\5421.png)



## Work

### 程序基本步骤

1. 创建输入DStream来定义输入源
2. 通过对DStream应用转换操作和输出操作来定义流计算
3. 用streamingContext.start()来开始接收数据和处理流程；start之后不能再添加业务逻辑。
4. 通过streamingContext.awaitTermination()方法来等待处理结束（手动结束或因为错误而结束）
5. 可以通过streamingContext.stop()来手动结束流计算进程

### 基本源

1. **textFileStream**       文件夹
2. **socketTextStream**   网络端口
3. **queueStream**         RDD队列流
   - oneAtATime：缺省为**true**，一次处理一个RDD，设为**false**，一次处理全部RDD；
     RDD队列流可以使用local[1]；涉及到同时出队和入队操作，所以要做同步；

- **wordCount**

```scala
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.通过监控端口创建DStream，读进来的数据为一行行
    val lineStreams = ssc.socketTextStream("slave4", 9999)

    //将每一行数据做切分，形成一个个单词
    val wordStreams = lineStreams.flatMap(_.split(" "))

    //将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    //将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

    //打印
    wordAndCountStreams.print()

    //启动SparkStreamingContext，启动接收数据和处理
    ssc.start()
    //保持等待
    ssc.awaitTermination()
  }
}

$nc -lk 9999

//3.监控文件夹创建DStream
val dirStream = ssc.textFileStream("hdfs://master:8020/fileStream")
```

- RDD队列

```scala
//1.初始化Spark配置信息
val sparkConf = new SparkConf().setMaster("local[4]").setAppName("RDDQueue")
//2.初始化SparkStreamingContext
val ssc = new StreamingContext(sparkConf, Seconds(1))
ssc.sparkContext.setLogLevel("ERROR")
val rddQueue = new mutable.Queue[RDD[Int]]()
//接收一个rdd的队列
val queueStream = ssc.queueStream(rddQueue, false)
val mappedStream = queueStream.map(r => (r % 10, 1))
val reducedStream = mappedStream.reduceByKey(_ + _)
reducedStream.print()
ssc.start()
// 每秒产生一个RDD
for (i <- 1 to 10) {
  rddQueue.synchronized {
    rddQueue += ssc.sparkContext.makeRDD(1 to 100, 2)
    println("监控:::" + i)
  }
  Thread.sleep(120)
}
ssc.stop()
//    ssc.awaitTermination()
```



- socket流

  模仿nc的程序，向固定端口发送数据

```scala
def main(args: Array[String]): Unit = {
  val words = "Hello World Hello Hadoop Hello spark kafka hive zookeeper hbase flume sqoop".split(" ")
  val n = words.length
  //定义端口
  val port = 9999
  val random = new Random()
  //一个socket实例
  val ss = new ServerSocket(port)
  //创建
  val socket = ss.accept()
  println("成功连接到本地主机：" + socket.getInetAddress)
  while (true) {
    val out = new PrintWriter(socket.getOutputStream)
    out.println(words(random.nextInt(n)) + " "+ words(random.nextInt(n)))
    out.flush()
    Thread.sleep(400)
  }
}
```

### 自定义数据源

- 继承Receiver，并实现onStart、onStop方法来自定义数据源采集。

```scala
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  //读数据并将数据发送给Spark
  def receive(): Unit = {

    //创建一个Socket
    var socket: Socket = new Socket(host, port)

    //定义一个变量，用来接收端口传过来的数据
    var input: String = null

    //创建一个BufferedReader用于读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

    //读取数据
    input = reader.readLine()

    //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
    while (!isStopped() && input != null) {
      store(input)
      input = reader.readLine()
    }

    //跳出循环则关闭资源
    reader.close()
    socket.close()

    //重启任务
    restart("restart")4
  }

  override def onStop(): Unit = {}
}
```





### 转换操作

#### DStream无状态转换

- **基操**
  1. **map(func)** ：对源DStream的每个元素，采用func函数进行转换，得到一个新的DStream
  2. **flatMap(func)** ：与map相似，但是每个输入项可用被映射为0个或者多个输出项
  3. **filter(func)** ：返回一个新的DStream，仅包含源DStream中满足函数func的项
  4. **repartition(numPartitions)** ：通过创建更多或者更少的分区改变DStream的并行程度
  5. **reduce(func)** ：利用函数func聚集源DStream中每个RDD的元素，返回一个包含单元素RDDs的新
  6. **DStream**
  7. **count()** ：统计源DStream中每个RDD的元素数量
  8. **union(otherStream)** ：返回一个新的DStream，包含源DStream和其他DStream的元素
  9. **countByValue()** ：应用于元素类型为K的DStream上，返回一个（K，V）键值对类型的新DStream，每个键的值是在原DStream的每个RDD中的出现次数
  10. **reduceByKey(func, [numTasks])** ：当在一个由(K,V)键值对组成的DStream上执行该操作时，返回一个新的由(K,V)键值对组成的DStream，每一个key的值均由给定的recuce函数（func）聚集起来
  11. **join(otherStream, [numTasks])** ：当应用于两个DStream（一个包含（K,V）键值对,一个包含(K,W)键值对），返回一个包含(K, (V, W))键值对的新Dstream
  12. **cogroup(otherStream, [numTasks])** ：当应用于两个DStream（一个包含（K,V）键值对,一个包含(K,W)键值对）

- **important**

  **transform(func)** ：通过对源DStream的每个RDD应用RDD-to-RDD函数，创建一个新的DStream。支持在新的DStream中做任何RDD操作



- 前面的基本源操作都是无状态转换的基本操作

##### 例题

- 黑名单（transform的使用）

  ```scala
  假设：arr1为黑名单数据，true表示数据生效，需要被过滤掉；false表示数据未生效
  val arr1 = Array(("spark", true), ("scala", false))
  val rdd1 = sc.makeRDD(arr1)
  假设：arr2为原始的数据，格式为(time, name)，需要根据arr1中的数据将arr2中的数据过滤。
  如"2 spark"要被过滤掉
  val arr2 = Array("1 hadoop", "2 spark", "3 scala", "4 java", "5 hive")
  val rdd2 = sc.makeRDD(arr2)
  结果：Array(3 scala, 5 hive, 4 java, 1 hadoop)
  ```

Methon1：join操作

```scala
methon1:join操作
//1.初始化Spark配置信息
val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamWordCount")
//2.初始化SparkStreamingContext
val ssc = new StreamingContext(sparkConf, Seconds(2))
ssc.sparkContext.setLogLevel("ERROR")
val arr1 = Array(("spark", true), ("scala", false))
//筛选出黑名单
val blackListMid: RDD[(String, Boolean)] = ssc.sparkContext.makeRDD(arr1)
val blackList: RDD[(String, Boolean)] = blackListMid.filter(_._2)
//从端口读数据
val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
//对流数据进行处理将其转化为k、v形式
val result: DStream[(String, Option[Boolean])] = ds.map(line => {
  val spl: Array[String] = line.split("\\s+")
  (spl(1), spl(0))
}).transform(sourceWord => {//将转换好的数据进行转换为RDD
  //进行左外连接
  val allResult: RDD[(String, (String, Option[Boolean]))] = sourceWord.leftOuterJoin(blackList)
  //筛选出不在黑名单中的数据，生成的数据格式在最下面↓↓↓↓↓↓
  val notBlackResult: RDD[(String, (String, Option[Boolean]))] = allResult.filter(_._2._2.equals(None))
  notBlackResult.map(notBlackWord => (notBlackWord._1, notBlackWord._2._2))
})
result
//执行并阻塞
result.print()
ssc.start()
ssc.awaitTermination()
```



#### DStream有状态转换

##### 滑动窗口

![fdasfdas](D:\software\typoraIMG\fdasfdas.png)

- 重要参数

  1. 滑动窗口的长度：即窗口的持续时间
  2. 滑动窗口的时间间隔：按时间在DStream上滑动
  3. 此时的初始化SparkStreamingContext：每隔n秒生成一个RDD

  注：

  1. 窗口长度、滑动间隔必须是batchDuration的整数倍
  2. 每隔 batchDuration（初始化时候的参数） 时间会生成一个RDD
  3. 窗口长度不能太大
  4. 窗口操作是无状态操作
  5. 一定要建立检查点用于保存数据

- wordCount

```scala
def main(args: Array[String]): Unit = {
  //1.初始化Spark配置信息
  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamWordCount")
  //2.初始化SparkStreamingContext
  //每隔一秒生成一个RDD
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")
  //用接口取数据
  val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
  //检查点，需要将前面的数据保存
  ssc.checkpoint("E:\\project\\bigdata\\scalamv\\data\\checkPoint2")
  ds.flatMap(_.split("\\s+"))
    .map(word => (word, 1))
    //滑动间隔：Streaming程序触发的间隔（6）
    //窗口长度：窗口的持续时间为24s
    .reduceByKeyAndWindow((k: Int, v: Int) => k + v, (x: Int, y: Int) => x - y, Seconds(24), Seconds(1))
    //下面这种是普通的使用，上面的有时候更加高效的，原理为窗口滑动每次肯定中间是有重复计算过的数据，
    //.reduceByKeyAndWindow((k: Int, v: Int) => k + v, Seconds(24), Seconds(1))
    .transform(rdd => {
      rdd.sortBy(_._2)
    })
    .print()
  //启动
  ssc.start()
  ssc.awaitTermination()
}
```

- 滑动窗口的转换操作

  1. **Window** (windowLength,slideInterval)

     返回一个基于源DStream的窗口批次计算后得到新的DStream

  2. **countByWindow**  (windowLength,slideInterval)

     返回基于滑动窗口的DStream中的元素的数量

  3. **reduceByWindow**  (func,windowLength,slideInterval)

     基于滑动窗口对源DStream中的元素进行聚合操作，得到一个新的DStream

  4. **reduceByKeyAndWindow**   (func,windowLength, slideInterval, [numTasks])

     基于滑动窗口对(K，V)键值对类型的DStream中的值按K使用聚合函数func进行聚合操作，得到一个新的Dstream

  5. **reduceByKeyAndWindow**   (func,invFunc,windowLength, slideInterval,[numTasks])

     一个更高效的reduceByKkeyAndWindow()的实现版本，先对滑动窗口中新的时间间隔内数据增量聚合并移去最早的与新增数据量的时间间隔内的数据统计量。例如，计算t+4秒这个时刻过去5秒窗口的WordCount，那么我们可以将t+3时刻过去5秒的统计量加上[t+3，t+4]的统计量，在减去[t-2，t-1]的统计量，这种方法可以复用中间三秒的统计量，提高统计的效率。

  ![搜狗截图20191113175012](D:\software\typoraIMG\搜狗截图20191113175012.png)



##### updateStateByKey

- spark streaming按照批处理划分job，当需求时按照时间周期内的所有数据进行处理，需要根据之前的计算结果和新时间周期的数据计算结果。

  1. 定义状态
  2. 定义状态更新函数：用一个函数指定怎样使用先前的状态。从输入流中的新值更新状态。
  3. 有状态操作，要不断把当前的历史记录进行处理
  4. 一定建立检查点

  注：数据量大不适用。

- 源码参数

  ```scala
  updateFunc: (Seq[V], Option[S]) => Option[S]
  Seq[v]：按照key做规约之后保存的value值
  Option[S]:以前结果保留的状态(状态和输入的类型可以不一样，经过逻辑处理过的结果肯定不一样)
  ```

  - wordcount

    ```scala
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamWordCount")
    //2.初始化SparkStreamingContext
    //每隔三秒生成一个RDD
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("E:\\project\\bigdata\\scalamv\\data\\checkPoint")
    
    // Seq[v]：按照key做规约之后保存的value值
    // Option[S]:以前结果保留的状态(状态和输入的类型可以不一样，经过逻辑处理过的结果肯定不一样)
    // updateFunc: (Seq[V], Option[S]) => Option[S]
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999 , StorageLevel.MEMORY_ONLY)
    ds.flatMap(_.split("\\s+")).map(word => (word , 1))
      .updateStateByKey((lst :Seq[Int] , stats : Option[Int])=>{
        println(s"")
        val sumNum: Int = lst.sum //只是这个批次的值
        val newSumNum: Int = stats.getOrElse(0)+sumNum
        Option(newSumNum)
      }).print()
    ssc.start()
    ssc.awaitTermination()
    ```



##### mapWithState

```scala
三个类型word: String, option: Option[Int], state: State[Int]
word  代表输入的数据，【wordcount中的单词】
option  以前的数据，历史数据
state   状态，是否出现过..
```

- 使用过程

  1. 定义函数：或者方法定义逻辑函数
  2. 定义函数将逻辑函数抛入：val spec = StateSpec.function(mappingFunction1)
  3. 在使用时直接    .mapWithState(spec)
  4. .stateSnapshots()//显示的结果会有所变化（也就是打印状态的快照）否则就只有到目前有的才会显示。

- wordcount

  ```scala
  //1.初始化Spark配置信息
  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamWordCount")
  //2.初始化SparkStreamingContext
  //每隔三秒生成一个RDD
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")
  ssc.checkpoint("E:\\project\\bigdata\\scalamv\\data\\checkPoint")
  val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
  //k、v、状态、输出结果方法
  def mappingFunction(word: String, one: Option[Int], state: State[Int]) ={
    val sum: Int = one.getOrElse(0) + state.getOption().getOrElse(0)
    val output: (String, Int) = (word , sum)
    state.update(sum)
    output
  }
  //函数
  val mappingFunction1=(word: String, one: Option[Int], state: State[Int]) => {
    val sum: Int = one.getOrElse(0) + state.getOption().getOrElse(0)
    val output: (String, Int) = (word , sum)
    state.update(sum)
    output
  }
  //先定义后使用
  val spec = StateSpec.function(mappingFunction1)
  //mapWithState 仅仅显示截止到目前出现的
  ds.flatMap(_.split("\\s+"))
    .map(word => (word ,1))
    .mapWithState(spec)
    .stateSnapshots()//显示的结果会有所变化（也就是打印状态的快照）
    .print()
  ssc.start()
  ssc.awaitTermination()
  ```

  



### 输出

1. **print()**

   在Driver中打印出DStream中数据的前10个元素

2. **foreachRDD(func)**

   DS的数据输出到外部存储

   最常用的输出操作，将func函数应用于DStream中的RDD上，这个操作会输出数据到外部系统，比如保存RDD到文件或者网络数据库等。需要注意的是func函数是在运行该streaming应用的Driver进程里执行的。

3. **saveAsTextFiles(prefix, [suffix])** 

   将DStream中的内容以文本的形式保存为文本文件，其中每次批处理间隔内产生的文件以prefix-TIME_IN_MS[.suffix]的方式命名

4. **saveAsObjectFiles(prefix, [suffix])**

   将DStream中的内容按对象序列化并且以SequenceFile的格式保存。其中每次批处理间隔内产生的文件以prefix-TIME_IN_MS[.suffix]的方式命名

5. **saveAsHadoopFiles(prefix, [suffix])** 

   将DStream中的内容以文本的形式保存为Hadoop文件，其中每次批处理间隔内产生的文件以prefix TIME_IN_MS[.suffix]的方式命名



#### 到mysql

- 建立jdbc工具类

```scala
//接受数组
def wordcountSaveAsMySQL(iter: Array[(String, Int)]): Unit = {
  val url = "jdbc:mysql://slave4:3306/spark"
  val user = "root"
  val password = "wang1997"
  var conn: Connection = null
  var stmt: PreparedStatement = null
  try {
    //每批连接一次，得到一条语句实例
    conn = DriverManager.getConnection(url, user, password)
    stmt = conn.prepareStatement("insert into wordcount values (?, ?)")
    iter.foreach(record => {
      //取出数据并且放为批处理
      stmt.setString(1, record._1)
      stmt.setInt(2, record._2)
      stmt.addBatch()
    })
    //一批处理一次，效率为普通的十倍还多
    stmt.executeUpdate()
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    
  }
}
```

- 进行socket的批消费并且将结果保存到JDBC(wordcount)

```scala
//1.初始化Spark配置信息
val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamWordCount")
//2.初始化SparkStreamingContext
val ssc = new StreamingContext(sparkConf, Seconds(1))
ssc.sparkContext.setLogLevel("ERROR")
//用接口取数据
val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
//转换逻辑结果
val resLogist: DStream[(String, Int)] = ds.flatMap(_.split("\\s+"))
  .map((_, 1))
  .reduceByKey(_ + _)
//直接使用Dstream的输出算子
resLogist
  .foreachRDD(rdd => { //foreachRDD就是一个输出操作
    //重分区并且发送
    rdd.coalesce(1).foreachPartition(iter => {
      //将内部的RDD迭代器分别发送而不是转为一个大数组，防止数组过大造成内存的负担
      iter.foreach { case (word, num) => {
        com.bigdata.bigdataUtil.BigDataUtils.wordcountSaveAsMySQL(Array((word, num)))
      }
      }
    })
    rdd
  })
//将结果抓换结果收回并且写入mysql
//        resLogist
//        .transform(rdd => {
//          val result: RDD[(String, Int)] = rdd
//          //重分区并且收回
//          val arrRus: Array[(String, Int)] = rdd.coalesce(1).collect()
//          com.bigdata.bigdataUtil.BigDataUtils.wordcountSaveAsMySQL(arrRus)
//          rdd
//        })
//          .print()
println("发送成功")
//    resultDS.print()
//启动
ssc.start()
ssc.awaitTermination()
```



### CheckPoint

两种

- **Metadata**（元数据）→保存计算逻辑到HDFS，用来恢复driver↓↓↓↓↓↓↓↓元数据包括

  1. 配置 - 用于创建该 streaming application 的所有**配置**
  2. DStream 操作 - DStream 一系列的**操作**
  3. 未完成的 **batches** - 那些提交了 job 但尚未执行或未完成的 batches
  4. yarn模式下yarn会自动启动ApplicationMaster
  5. 如果检查点目录存在那么将导入检查点数据，如果不存在那么重新创建context

- **Data checkpoint**，用于转换操作(窗口函数)

  1. 在转换过程中定期将RDDS保存到磁盘来切断依赖链，对于转换操作是必要的

     在使用状态变化时建立检查点   ssc.checkpoint(checkpointDirectory)

- 用于恢复driver，保存Metadata的书写
  1. 定义方法，将新的ssc返回并且处理逻辑在此方法内
  2. 定义检查点路径
  3. 然后调用方法，从检查点中取，如果取不到那就创建

```scala
//定义方法
def createContext(path : String): StreamingContext = {
  println("creating new Streaming context")
  val conf: SparkConf = new SparkConf().setAppName("RecoverableFileWordCount").setMaster("local")
  val newSSC = new StreamingContext(conf , Seconds(5))
  newSSC.sparkContext.setLogLevel("Error")
  newSSC.checkpoint(path)
  //定义数据处理的逻辑
  val inesDS: DStream[String] = newSSC.textFileStream("hdfs://ljwha/user/spark/data/fslog")
  inesDS.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_).print()
  newSSC
}
//定义 checkpoint的路径
val checkpointPath = "E:\\project\\bigdata\\scalamv\\data\\checkPoint"
//从checkpoint中取，取不到创建
val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointPath,
  () => createContext(checkpointPath))
ssc.sparkContext.setLogLevel("ERROR")
ssc.start()
ssc.awaitTermination()
```

- **checkpoint的内容**

![搜狗截图20191113202800](D:\software\typoraIMG\搜狗截图20191113202800.png)

- **checkpoint的缺点**
  1. 重编译再去反序列化数据就会失败，必须创建ssc，重建ssc意为这数据丢失
  2. 依赖，版本不能变化



### 优雅关闭

- 管理员在约定的位置（hdfs）创建文件，streaming程序周期性的检查文件是否存在，存在就主动关闭程序。

```scala
def main(args: Array[String]): Unit = {
//    spark配置
//    if (args.length < 1) {
//      println("需要1个参数： hadoop_file 标志程序退出的文件")
//      System.exit(1)
//    }
//    val hadoop_file = args(0)
  val hadoop_file = "hdfs://node1:8020/stop"
  val conf = new SparkConf().setMaster("local[*]").setAppName("gracefulCloseDemo")
    .set("spark.streaming.stopGracefullyOnShutdown", "true")
  //等待数据处理完后，才停止任务，以免数据丢失
  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.sparkContext.setLogLevel("warn")

  val ds = ssc.socketTextStream("wangsu", 9999, StorageLevel.MEMORY_ONLY)
  val ds2 = ds.map((_, 1)).reduceByKey(_ + _)
  ds2.print()

  //启动
  ssc.start()
  stopByMarkFile(ssc, hadoop_file)
}

def stopByMarkFile(ssc: StreamingContext, hadoop_file: String): Unit = {
  val intervalMills = 1 * 1000 // 每隔1秒扫描一次消息是否存在
  var isStop = false
  while (!isStop) {
    isStop = ssc.awaitTerminationOrTimeout(intervalMills)
    if (!isStop && isExistsMarkFile(hadoop_file)) {
      println("1 秒后开始关闭sparstreaming程序.....")
      Thread.sleep(1000)
      ssc.stop(true, true)
      delHdfsFile(hadoop_file)
    } else {
      println("***********未检测到有停止信号*****************")
    }
  }

  // 判断给定文件是否存在
  def isExistsMarkFile(hdfs_file_path: String): Boolean={
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs = path.getFileSystem(conf)
    fs.exists(path)
  }

  // 删除指定的文件
  def delHdfsFile(hdfs_file_path: String): Unit={
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs = path.getFileSystem(conf)
    if (fs.exists(path)) fs.delete(path,true)
  }
}
```





### KafkaStreaming

- 使用Receivers和kafka高层次API，简单配置，offset会被自动存储起来
- 使用Direct可以定义定期的topic+partition中查询最新的偏移量、
- 两个版本
  1. 0.8 （0.8-0.10）→receiver（kafka高阶API）direct（kafka）低阶API
  2. 0.10以上
- 两种方式对比
  1. Receiver：（资源的占用，数据放两份[本地一分其他节点一份]；WAL；kafka高阶API（简单offset不用管理））
  2. Driver：（直连，kafka低阶API；生产环境使用，offset的维护）

#### 基于Receiver（08）

- 占用资源，接收数据，这部分资源不参与计算，offset自动管理

  启动多个receiver接收数据，收到多个DS，DS之间要做union

  系统管理offfset容易造成数据的丢失

  保证接受数据（在receiver中）的不丢失，开启WAL将数据存放在hdfs（严重降低系统的吞吐量）

- 利用接受器来接受kafka中的数据，存储在executor中（StorageLevel可用户指定），之后提交job处理

- Receiver-based
  1. 提交任务后，Spark集群会划出指定的**Receivers来专门**、持续不断、异步读取Kafka的数据，读取时间间隔以及每次读取offsets范围可以由参数来配置；
  2. driver触发batch任务后，Receivers → 其他Executor → 更新zk中的offset

- 注意：
  1. spark和kafka的partition不是相关的，那么增加topic的partition，仅仅是增加线程处理单一的Receiver消费的主题。
  2. 对不同的group和topic可以使用多个Receiver创建不通的Dstream并行接受数据
  3. 如果启用了Write Ahead Logs复制到文件系统如HDFS，那么storage level需要设置成StorageLevel.MEMORY_AND_DISK_SER；

- EG

  1. 使用的是KafkaUtils的createStream方法，提供了重载，多种形式来定义

     ```scala
     // 使用Kafka高阶API，offset自动管理，存放在__consumer_offsets中
     // 程序重启后，接着保存的offset开始消费
     // 注意参数，不能指定从哪里开始消费
     
     //1.初始化Spark配置信息，切记local的core要大于1，不然receiver就占满了
     val sparkConf = new SparkConf().setMaster("local[4]").setAppName("kafkaStream")
     
     //2.初始化SparkStreamingContext
     val ssc = new StreamingContext(sparkConf, Seconds(1))
     ssc.sparkContext.setLogLevel("ERROR")
     
     //定义参数
     val zkQuorum = "master:2181,slave1:2181/kafka1.1"
     val groupId = "group1"
     val topics = Map("topica" -> 1)
     
     //创建ds
     val kafkaDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
       ssc,
       zkQuorum,
       groupId,
       topics
     )
     
     kafkaDS.foreachRDD(rdd => {
       println("rdd.count:  " + rdd.count())
     })
     
     //启动
     ssc.start()
     ssc.awaitTermination()
     ```

- **Driver HA**

  1. 将数据组装成block元数据（配置项，Dstream操作，未完成的Batch状态，个生成的RDD数据），
  2. Driver失败后
     1. 使用checkpoint数据重启driver，重启构造接收器，恢复block元数据，恢复未完成的作业，再次产生RDD和job提交集群操作。





- **WAL配置**

  1. 设置checkpoint目录，用来保存WAL的HDFS目录
  2. spark.streaming.receiver.writeAheadLog.enable 设置为 true；

  就是备份多给可恢复的缓冲数据源，这个功能会减少处理数据的吞吐，加重了Receiver的压力

#### Direct Approach（08）

- 自己管理offset，启动作业时，从kafka获取数据，

  数据处理，保存结果，保存offset（先存结果，宁重复不丢失）

- 低阶版本，可以自己处理offset管理，也就无需receiver读取数据，计算时才读数据，所以对内存的要求也不高，只需要考虑batch的内容，任务堆积也不会影响数据堆积。

- Direct方式中，Kafka中的partition与RDD中的partition是一一对应的并行读取Kafka数据，这种映射关系也更利于理解和优化；

- 优点：
  1. 高效（直接从kafka恢复数据）
  2. 精确一次（自定义）

- EG:

  1. 初始化spark配置及其ssc

  2. 定义初始参数（kafkaParams、topics.....）

  3. 自定义offset    →   Map**[** TopicAndPartition , Long **]**（哪个topic的哪个分区从 → offset开始）

  4. 定义一个消息函数，用于将每个消息和元数据转换为所需的类型

  5. 创建Director消费（ssc，参数列表，初始offset，消息转换函数）

  6. 对消费者进行遍历foreachRDD，然后对rdd进行操作,操作offset到外部存储可以将

     rdd.asInstanceOf[HasOffsetRanges].offsetRanges然后对这个迭代器进行操作，里面都是每个分区的offset

```scala
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStream")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    //定义初始参数
    val kafkaParams = Map("metadata.broker.list" -> "master:9092")
    val topic = "topica"
    val topics = Set(topic)

    //自定义offset
    val fromOffsets: Map[TopicAndPartition, Long] = Map(
      TopicAndPartition(topic, 0) -> 400,
      TopicAndPartition(topic, 1) -> 300,
      TopicAndPartition(topic, 2) -> 200
    )

    //用于将每个消息和元数据转换为所需的类型
    val messageHandler: MessageAndMetadata[String, String] => String = (x: MessageAndMetadata[String, String]) => (x.message())

    //创建消费
    val kafkaDS = KafkaUtils.
      createDirectStream[String , String , StringDecoder,StringDecoder , String](ssc, kafkaParams, fromOffsets, messageHandler)

    kafkaDS.foreachRDD((rdd, time) => {
      //这里的count是从定义的offset开始计算的
      println(s"rdd.count: ${rdd.count()} ; time: ${time} ")
      //操作offset到外部存储
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (range <- ranges){
        println(s"topic = ${range.topic};partition = ${range.partition}; offset = ${range.fromOffset}")
      }
    })
    ssc.start()
    ssc.awaitTermination()
```





#### kafka offset管理redis（08）

- 需要保证offset和result同时成功，（事务）
  1. Hbase → 行事务（可以保证一行可以同时成功）
     1. 行 → 分区，offset ， result
  2. 数据库

- 外部存储
  1. **hdfs**:产生的小文件太多（极其多）

  2. **Checkpoint**：本身就缺点多（重编译的序列化问题丢数据，版本依赖不能轻易更换）

  3. **mysql** ：还行   (结果+offset=>事务)   **ok**    （幂等，事务100%）

  4. **redis**   ： **ok**  常用（速度快）

  5. **HBase**     ： topic+partition+timestamp   **ok**   

  6. **Mogondb**  :    **ok**   没用过不太清楚 

  7. **hive**  不适合  ： 

  8. **zk**：不适合频繁的读写

  9. **kafka**（不推荐） 0.11之前： group消费组 → topica的消息 → zk  

     ​		最新：kafka的消息队列中 ___consumer_offset

- 下面例子是我们公司的offset管理（使用的是redis）

  **思路：**

  1. 设计使用redis的hash保存offset，那么每次都会重刷掉，只保留一次记录
  2. 具体设计：
     1. key：前缀+topic+groupid
     2. fields：partition
     3. value：offset
  3. 实现功能：
     1. 获取key
     2. 从redis中获取offset
     3. 获取集群最大最小offset方法
     4. 校验获取的offset的方法
     5. 将数据保存到redis

- 主程序

  ```scala
      //1.初始化Spark配置信息
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStream")
  
      //2.初始化SparkStreamingContext
      val ssc = new StreamingContext(sparkConf, Seconds(2))
      ssc.sparkContext.setLogLevel("ERROR")
  
      val brokers = "master:9092"
      val topics = Set("topica", "topicb", "topicc")
      val kafkaParams = Map(("metadata.broker.list", brokers))
  
      //这里的函数一定别写错，注意语法，标点符号等等，踩坑了
      val fromOffsets: Map[TopicAndPartition, Long] = com.bigdata.sparkstreaming.manageOffset.RadisUtils.verify(topics)
  
      val messageHandler: MessageAndMetadata[String, String] => (String, String) =
        (megmetadata) => (megmetadata.key(), megmetadata.message())
  
      val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc,
        kafkaParams,
        fromOffsets,
        messageHandler
      )
  
      kafkaDS.foreachRDD((rdd, time) => {
        if (!rdd.isEmpty()) {
          println("所有topic的总： " + rdd.count() + " : " + time)
          val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          RadisUtils.saveOffsetToRedis(ranges)
          ranges.foreach(println)
        }
      })
  
      ssc.start()
      ssc.awaitTermination()
  ```

- redis工具类

  1. 定义

     ```scala
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
     ```

  2. 从redis中获取offset

     ```scala
     def getOffsetFromRedis(topics: Set[String], groupId: String = ""): Map[TopicAndPartition, Long] = {
       val jedis: Jedis = resource
       //从所有topic遍历
       val redisSetMap: Set[Map[TopicAndPartition, Long]] = topics.map(topic => {
         import scala.collection.JavaConversions._
         val redisMap: Map[String, String] = jedis.hgetAll(getKey(topic, groupId)).toMap
         //      jedis.close()
         //数据类型转换
         redisMap.map { case (partition, num) => {
           println(s"获取到:${topic}     partiiont :${partition}")
           TopicAndPartition(topic, partition.toInt) -> num.toLong
         }
         }
       })
       jedis.close()
       //这里细节了
       redisSetMap.flatten.toMap
     }
     ```

  3. 将数据保存到redis

     ```scala
     //将数据保存到redis
     def saveOffsetToRedis(ranges: Array[OffsetRange], groupid: String = ""): Unit = {
       //获取连接
       val jedis: Jedis = resource
       //获取key
       //获取partition， offsets
       //后面再改成一批处理
       //    val mapRes: mutable.Map[String, String] = scala.collection.mutable.Map[String , String]()
       import scala.collection.JavaConversions._
       ranges.map(range => {
         val key: String = getKey(range.topic)
         println(s"当前${range.topic}的offset：${range.untilOffset}   partition：${range.partition}")
         jedis.hset(key, range.partition.toString, range.untilOffset.toString)
       })
       //这里不能关闭
       //        jedis.close()
     }
     ```

  4. 获取集群最大最小offset的方法

     ```scala
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
     ```

  5. 校验方法

     ```scala
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
           println(s"maxOffset: ${max}   minOffset: ${min}")
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
     ```



#### hbase存offset历史

```scala
/**
  * 需求：要求保留历史offset
  * 实现功能：
  *   1.保存
  *   2.取值
  *   3.校验
  * 设计：
  *   1.rowkey：前缀+topic+groupid+timestamp
  *   2.colFamily ： 固定 offsets
  *   3.col ： partition
  *   4.value：offset
  */
object SaveOffsetToHBase {
  private val hbaseTableName = "myhb:kafka_offsets"
  private val prefix = "myOffset"
  private val colFamily = "offsets"

  //建表
  def createTable(name: String): Unit = {
    val tableName: TableName = TableName.valueOf(name)
    val descriptor = new HTableDescriptor(tableName)
    val columnDescriptor = new HColumnDescriptor("offsets")
    columnDescriptor.setInMemory(true)
    columnDescriptor.setVersions(1, 10)
    columnDescriptor.setTimeToLive(24 * 60 * 60)
    descriptor.addFamily(columnDescriptor)
    Utils.getAdmin.createTable(descriptor)
  }

  //获取rowkey的方法
  def getRowKey(topic : String , groupid : String = "" ,timestamp : Time): String ={
    s"$prefix:${topic}:${groupid}:${timestamp.milliseconds}"
  }

  //保存
  def saveOffsetToHbase(ranges :OffsetRange , topics :Set[String] , groupid :String = "" , time : Time ): Unit ={
    val admin: Admin = Utils.getAdmin
    val connection: Connection = Utils.getConnection
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
    //遍历topics组进行获取rowkey，创建put，添加列值
    topics.map{topic => {
      val rowKey: String = getRowKey(topic , groupid , time)
      val put: Put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(
        Bytes.toBytes(colFamily),
        Bytes.toBytes(ranges.partition.toString),
        Bytes.toBytes(ranges.untilOffset.toString)
      )
      table.put(put)
    }}
    admin.close()
    //不能关闭呢
//    connection.close()
  }

  //获取
  def getOffsetFromHbase(topics : Set[String] , groupid : String = ""): Map[TopicAndPartition, Long] ={
    val admin: Admin = Utils.getAdmin
    val connection: Connection = Utils.getConnection
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
    val scan: Scan = new Scan()
    var scanner: ResultScanner = null
    var resMap: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()

    //myOffset:topica::1573908197000   存储的rowkey
    topics.map(topic => {
      //獲得开始和结束的rowkey
      val startRow =  s"$prefix:$topic:$groupid:${String.valueOf(System.currentTimeMillis())}"
      val stopRow =  s"$prefix:$topic:$groupid:0"
      //取最后的值
      scanner = table.getScanner(scan.setStartRow(startRow.getBytes()).setStopRow(stopRow.getBytes()).setReversed(true))
      val resultScanner: Result = scanner.next()
      if (resultScanner != null){
        resultScanner.rawCells().foreach(cell => {
          val partition: String = Bytes.toString(CellUtil.cloneQualifier(cell))
          val offset: String = new String(CellUtil.cloneValue(cell) , "UTF-8")
//          println(offset)
//          println(partition)
          val tp: TopicAndPartition = TopicAndPartition(topic , partition.toInt)
//          println(tp)
          resMap += (tp -> offset.toLong)
        })
      }
      resMap
    })
//    scanner.close()
//    connection.close()
    resMap.toMap
  }

  def main(args: Array[String]): Unit = {
    //建表
//        createTable("myhb:kafka_offsets")
//    println(getOffsetFromHbase(Set("topicb")))
  }
}
```



#### 基于Direct（10）

- 系统offset执行

  ```scala
  //创建基本配置
  val conf: SparkConf = new SparkConf().setAppName("kafka010Direct").setMaster("local")
  val ssc = new StreamingContext(conf, Seconds(5))
  ssc.sparkContext.setLogLevel("error")
  //配置参数
  val topics = Set("topica")
  val kafkaParams = Map(
    "bootstrap.servers" -> "master:9092",//主机
    "key.deserializer" -> classOf[StringDeserializer],//key反序列化
    "value.deserializer" -> classOf[StringDeserializer],//value反序列化
    "group.id" -> "group1",//组
    "auto.offset.reset" -> "earliest",//重设
    "enable.auto.commit" -> "true"//是否自动管理offset
  )
  val assign: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
  //创建连接
  val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferBrokers,
    assign
  )
  kafkaDS.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      println("count : " + rdd.count())
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      ranges.foreach(println)
    }
  })
  ssc.start()
  ssc.awaitTermination()
  ```

- 指定offset执行

  ```scala
  //创建基本配置
  val conf: SparkConf = new SparkConf().setAppName("kafka010Direct").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))
  ssc.sparkContext.setLogLevel("error")
  //配置参数
  val kafkaParams = Map(
    "bootstrap.servers" -> "master:9092",//主机
    "key.deserializer" -> classOf[StringDeserializer],//key反序列化
    "value.deserializer" -> classOf[StringDeserializer],//value反序列化
    "group.id" -> "group1",//组
    "auto.offset.reset" -> "earliest",//重设
    "enable.auto.commit" -> "true"//是否自动管理offset
  )
  val topicPartitionsOffset: Map[TopicPartition, Long] = Map(
    new TopicPartition("topica", 0) -> 500,
    new TopicPartition("topica", 1) -> 500,
    new TopicPartition("topica", 2) -> 500,
    new TopicPartition("topicb", 0) -> 20,
    new TopicPartition("topicb", 1) -> 20,
    new TopicPartition("topicb", 2) -> 20
  )
  val topicPartitions: Iterable[TopicPartition] = topicPartitionsOffset.keys
  val assign = ConsumerStrategies.Assign[String, String](
    topicPartitions,
    kafkaParams,
    topicPartitionsOffset
  )
  //获取连接
  val kafkaDS: InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      assign
    )
  kafkaDS.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      println("count : " + rdd.count())
      rdd.foreach(println)
    }
  })
  ssc.start()
  ssc.awaitTermination()
  ```








# project

- 公司现需要将业务产生的数据由kafka做缓冲层进行生产，使用sparkStreaming进行消费，需求数据进行动态展示，时间暂定，要求kafka的offset需要自己来保存，零时数据可以放到mysql、redis、HBase具体暂定。

  kafka使用消费者组：（stock1，stock2）

  ![1574043587522](D:\software\typoraIMG\1574043587522.png)

- StockKafkaProducer

  ```scala
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
  ```

- StockSparkStreaming

  ```scala
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
  ```

- StockOffsetUtils

  ```scala
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
  
  ```

- StockHbaseUtils

  ```scala
  /**
    *将业务数据存到HBase
    * value :
    *   1. 总金额
    *   2. 平均值
    *   3. 滑动总金额
    * rowkey :
    *   1. 事件 + 时间戳
    * colFamily：
    *   primary_need
    * colName:
    *   sumMoney5
    *   avgMoney5
    *   sumMoneyWin10
    */
  object StockHbaseUtils {
    //基本属性
    private val hbaseTableName = "myhb:stockResult"
    private val colFamily = "primary_need"
  
    //建表
    def createTable(name: String): Unit = {
      val tableName: TableName = TableName.valueOf(name)
      val descriptor = new HTableDescriptor(tableName)
      val columnDescriptor = new HColumnDescriptor(colFamily)
      columnDescriptor.setInMemory(true)
      columnDescriptor.setVersions(1, 10)
      columnDescriptor.setTimeToLive(24 * 60 * 60)
      descriptor.addFamily(columnDescriptor)
      Utils.getAdmin.createTable(descriptor)
    }
  
    //存数据
    def saveDataToHbase(time : String , event : String , value : String): Unit ={
      //获取rowkey
      val rowkey: String = event+"-"+time
      //获取连接和实例table
      val connection: Connection = Utils.getConnection
      val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
      val admin: Admin = Utils.getAdmin
      //实例化put
      val put = new Put(rowkey.getBytes())
      put.addColumn(colFamily.getBytes , event.getBytes , value.getBytes)
      //上传
      table.put(put)
  //    connection.close()
  //    table.close()
    }
  
    def main(args: Array[String]): Unit = {
  //    createTable(hbaseTableName)
    }
  }
  ```

  