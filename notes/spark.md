spark

- 解压运行一个简单的workcount

```scala
./bin/spark-shell
sc.textFile("/home/hadoop/sparkdatatest/test1").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect
```

## 简述

### mr缺点

1. 表达能力有限
2. 磁盘IO开销大，任务延迟高
3. MR和hadoop紧密耦合在一起，无法动态替换



### spark优点

1. spark计算模式也属于MapReduce的范畴但不局限于mr，还提供了多种数据集操作类型，编程模型更灵活
2. spark提供了内存计算，可将中间结果放到内存，迭代运算效率更高
3. spark基于DAG的任务调度执行机制，优于mr
4. all in one     （core离线      sql交互  streaming事实    MLLib机器学习    GraphX图计算）

![搜狗截图20191028165633](D:\software\typoraIMG\搜狗截图20191028165633.png)

- **spark架构**：主从
  1. cluster  manager+worker （slaver）【executer】 + Driver
- 三种形式
  1. Standalone    yarn     Mesos
- 管理方式
  1. 粗粒度的管理方式：初始一次分配，用完回收（Standalone 、 yarn都是粗粒度）
  2. 细粒度的管理方式：按需分配

组件：

- Application：执行在spark上的应用程序，包括driver+许多executor
- Application jar：应用程序打包
- Driver Program：spark应用，包含main，初始化SparkContext，认为sc就是Driver
- Cluster manager：集群资源的管理者。Standalone模式：master，yarn：resourcemanager
- Deploy mode：cluster，client
  1. 如果driver在client（提交作业的节点）上，就是client模式，此时可以在driver端看见输出的结果
  2. 如果driver在cluster上，是cluster模式，此时看不见最终的输出结果，用于生产环境
- Worker Node：可以运行应用的工作节点
- Executor：在worker为运行应用程序启动的一个JVM，在executor中封装了资源（cpu，memory）
- Job：action算子触发job（一个Action算子就会生成一个Job）
- Stage：job按照shuffer（宽依赖）划分为多个stage
- ：task是spark任务分配的最小单位，多个task组成一个stage，Task之间处理的逻辑相同，处理的数据不同，task在Executor中执行，由Driver发送(一个task是一个线程)







![搜狗截图20191028165655](D:\software\typoraIMG\搜狗截图20191028165655.png)

![搜狗截图20191028193901](D:\software\typoraIMG\搜狗截图20191028193901.png)

![搜狗截图20191028194052](D:\software\typoraIMG\搜狗截图20191028194052.png)



### Spark的三大误解

- 误解一：Spark是一种内存技术
- 误解二：Spark要比Hadoop快 10x-100x
- 误解三：Spark在数据处理方面引入了全新的技术



## 安装&配置

解压缩配置环境变量

配置slaves，env，spark-defaults.conf，log4j.properties，修改快速启动的命令

- slaves文件配置所有worker节点
- env中添加

```
export JAVA_HOME=/opt/apps/jdk1.8.0_191/
export SPARK_MASTER_HOST=master
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=1g
export HADOOP_CONF_DIR=/opt/apps/hadoop-2.7.6/etc/hadoop/
```

- default.conf

```shell
spark.master                     spark://master:7077
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory              512m
```

- log4j.properties改名即可

- 启动(和Hadoop的群启动命令冲突，将一个更名)

start-all.sh    stop.all.sh



| spark.cores.max = 6 应用程序分配的最大核数                   |
| ------------------------------------------------------------ |
| **启动命令 spark-shell --total-executor-cores 2**            |
| l**og4j.rootCategory=INFO, console                    可以改成WARN为不打印** |
|                                                              |

- **History Server**

  - spark-defaults.conf

  ```shell
  spark.eventLog.enabled true
  spark.eventLog.dir hdfs://ljwha:8020/sparkhistory
  spark.eventLog.compress true
  ```

  - spark-env.sh

  ```shell
  export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=50 -Dspark.history.fs.logDirectory=hdfs://ljwha:8020/sparkhistory"
  ```

  







## 集群模式

### standalone

- standalone模式，即独立模式，自带完整的服务，可单独部署到一个集群中，无需依赖任何其他资源管理系统。从一定程度上说，该模式是其他两种模式的基础。

### Yarn

1. 目前仅支持粗粒度模式（Coarse-grained Mode）；
2. 由于YARN上的Container资源是不可以动态伸缩的，一旦Container启动之后，可使用的资源不能再发生变化；
3. YARN拥有强大的社区支持，且逐步已经成为大数据集群资源管理系统的标
   准；
4. spark on yarn 的支持两种模式：
   yarn-cluster：适用于生产环境
   yarn-client：适用于交互、调试，希望立即看到app的输出



### 运行模式

#### 本地

- 仅启动hdfs服务

local：本地模式。在本地启动一个线程来运行作业；

 local[N]：也是本地模式。启动了N个线程；

local[*]：还是本地模式。用了系统中所有的核；

local[N,M]：第一个参数表示用到核的个数；第二个参数表示容许该作业
失败的次数。上面的几种模式没有指定M参数，其默认值都是1；

#### 分布式部署

##### Onspark

- 必须先启动Spark的Master和Worker守护进程

- 不用启动Hadoop服务，除非要使用HDFS的服务

###### 测试一（standalone-client）

```shell
spark-submit \
--deploy-mode client \
--class com.bigdata.sparkcore.wordCount.wordcount scalamv-1.0-SNAPSHOT.jar /ljwdata/test001
```

- 此时

  Master进程做为cluster manager，用来对应用程序申请的资源进行管理；
  SparkSubmit作为Client端和运行driver程序；
  **CoarseGrainedExecutorBackend** 用来并发执行应用程序；

###### 测试二（standalone-cluster）

```shell
spark-submit \
--deploy-mode cluster \
--class com.bigdata.sparkcore.wordCount.wordcount scalamv-1.0-SNAPSHOT.jar /ljwdata/test001


spark-submit \
--deploy-mode client \
--driver-memory 512m \
--driver-cores 4 \
--class com.bigdata.sparkcore.exec.ThreadTest sparkmv.jar /ljwdata/agent.log
```

- 此时

  客户端的SparkSubmit进程会在应用程序提交给集群之后就退出（区别1）；
  Master会在集群中选择一个 Worker 进程生成一个子进程 **DriverWrapper** 来启动 driver 程序（区别2）；
  而该 DriverWrapper 进程会占用Worker进程的一个core，所以同样的资源下配置下，会少用1个core来参与计算（区别3）；
  应用程序的结果，会在执行 driver 程序的节点的 stdout 中输出，而不是打印在屏幕上（区别4）；

###### standalone-client和standalone-cluster的区别

- client：cluster manager负责调度，启动线程作为client运行driver程序，**CoarseGrainedExecutorBackend** 并发执行程序
- cluster：进程将application提交集群后退出，master在集群中选择worker生成子进程**riverWrapper** 启动driver，其会占用一个core，同资源情况下会少参与一个core，

```
cluster在集群中，没有返回结果，适用于生产环境
client及时返回结果，适用于测试环境
spark-shell：只能运行在client模式下
```





##### OnYarn

打开集群hdfs的yarn

- 运行在yarn上的时候不需要开启spark，不然长时间会出现调度作业混乱

###### yarn-client

```shell
spark-submit \
--master yarn \
--deploy-mode client \
--class com.bigdata.sparkcore.wordCount.wordcount scalamv-1.0-SNAPSHOT.jar /ljwdata/test001
```

- 此时

  提交应用程序，会生成**SparkSubmit**进程，该进程会执行driver程序；
  RM会在集群中的某个NodeManager上，启动一个**ExecutorLauncher**进程，来做为ApplicationMaster；
  会在多个NodeManager上生成**CoarseGrainedExecutorBackend**进程来并发的执行应用程序。



###### yarn-cluster

```shell
spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.bigdata.sparkcore.wordCount.wordcount scalamv-1.0-SNAPSHOT.jar /ljwdata/test001
```

- 此时：

  提交应用程序，会生成**SparkSubmit**进程，该进程只用来做Client端，应用程序提交给集群后，就会删除该进程；
  Resource Manager在集群中的某个NodeManager上启动**ApplicationMaster**，该AM同时会执行driver程序；
  会在各NodeManager上运行**CoarseGrainedExecutorBackend**来并发执行应用程序；

###### yarn-client&yarn-cluster的区别

- yarn-cluster模式下:
  driver运行在AppMaster(Application Master)中，它负责向YARN申请资源，并监督作业的运行状况。当用户提交了作业之后，就可以关掉Client，作业会继续在YARN上运行。
- yarn-client模式下，Application Master仅仅向YARN请求executor，client会和请求的container通信来调度他们工作，也就是说Client不能离开。

![搜狗截图20191028194206](D:\software\typoraIMG\搜狗截图20191028194206.png)



### 配置idea直接存hdfs









## spark submit



| 参数名                | 格式        | 参数说明                                                     |
| --------------------- | ----------- | ------------------------------------------------------------ |
| --master              | MASTER_URL  | local、yarn、默认standalone                                  |
| --deploy-mode         | DEPLOY_MODE | Client或者master，默认是client                               |
| --class               | CLASS_NAME  | 应用程序的主类                                               |
| --name                | NAME        | 应用程序的名称                                               |
| --jars                | JARS        | 逗号分隔的本地jar包，包含在driver和executor的classpath下     |
| --repositories        |             | 逗号分隔的远程仓库                                           |
| --py-files            | PY_FILES    | 逗号分隔的”.zip”,”.egg”或者“.py”文件                         |
| --files               | FILES       | 逗号分隔的文件，这些文件放在每个executor的工作目录下面       |
| --conf                | PROP=VALUE  | 固定的spark配置属性，默认是conf/spark-defaults.conf          |
| --properties-file     | FILE        | 加载额外属性的文件                                           |
| **--driver-memory**   |             | Driver内存，默认1G                                           |
| --driver-java-options |             | 传给driver的额外的Java选项                                   |
| --driver-library-path |             | 传给driver的额外的库路径                                     |
| --driver-class-path   |             | 传给driver的额外的类路径                                     |
| **--executor-memory** |             | 每个executor的内存，默认是1G                                 |
| **--executor-cores**  |             | 每个executor的core                                           |
| --proxy-user          |             | 模拟提交应用程序的用户                                       |
| --driver-cores        |             | Driver的核数，默认是1。这个参数仅仅在standalone集群deploy模式下使用 |
| --queue               | QUEUE_NAME  | 队列名称。在yarn下使用                                       |
| --num-executors       |             | 启动的executor数量。默认为2。在yarn下使用                    |
|                       |             |                                                              |



## RDD

### 概述

- RDD（Resilient Distributed Dataset）叫做**弹性分布式数据集**，是Spark中最基本的数据抽象。代码中是一个抽象类，它代表一个**不可变**、**可分区**、里面的元素可**并行计算**的**集合**。
- 弹性表现：
  1. 可以分区（数据量大小，获取的资源状况）
  2. 内存和磁盘中
  3. 任务失败RDD数据可以重算（容错）
- 缺点：
  1. 不能做细粒度分区

#### 说明

1. RDD可能包含一个分区的列表

2. 对于一个分片，有一个计算函数compute，对每个分区进行计算

3. 对其他RDDs的依赖（宽依赖、窄依赖）列表

4. 对key-value RDDs来说，可能存在分区器（Partitioner）【可选的】

5. 对每个RDD可能存在位置优先的列表【可选的】

#### 特点

- 不能嵌套

- 依赖
- 持久化
- 只读

 通过对象上的方法调用来对RDD进行转换；
 最后输出结果 或是 向存储系统保存数据；
 RDD转换算子被称为Transformation；
 只有遇到Action算子，才会执行RDD的计算（懒执行）



#### spark编程模型

- 从外部



#### 两类算子

1. Transformation（做标记、懒执行、返回RDD[T]）
2. Action（真正计算、runjob、返回一个值不返回RDD）

#### 组件

1. sparkcontext(包含一个重要的对象，sparkEnv)
   - 是Spark的主要入口点,它负责和整个集群的交互
   -  Driver就是客户端，SparkContext则是客户端的核心
   - SparkContext是Spark的对外接口，负责向调用者提供Spark的各种功能
2. sparkConf
   - 用于连接Spark集群、创建RDD、累加器、广播变量
3. SparkEnv



### RDD

- 创建

  1. 内存创建（makeRDD，parallelize）

  ```scala
  //makeEDD的源：参数为seq有序序列，返回分区的seq和数量地层就是parallelize
  //此处的numSlices的为分区并行度，看源码就是如果设置就使用设置的，没设置就看当前剩余的core和2谁大，谁大分区数就是谁，小数据情况下也就是可以分几个文件
  def makeRDD[T: ClassTag](
        seq: Seq[T],
        numSlices: Int = defaultParallelism): RDD[T] = withScope {
      parallelize(seq, numSlices)
    }
  //使用
  val rdd1: RDD[Int] = sc.makeRDD(List(1,2,5,6,5))
  val rdd2: RDD[Int] = sc.parallelize(Array(5,6,8,9))
  ```

  2. 外部创建（textFile）

  ```scala
  //textFile
  //其源码是取最小的那个分区情况，也就是最小为多少，结果可能比其大，按照data字节数/分区数
  val rdd: RDD[String] = sc.textFile("path")
  ```

- 设置分区

  ```scala
  val rdd1: RDD[Int] = sc.makeRDD(List(1,2,5,6,5),5)
  //5就是分区
  ```



## ![6875321](D:\software\typoraIMG\6875321.png)Transformation

- 做标记

| 算子                       | 解释                            |
| -------------------------- | ------------------------------- |
| **map**                    | **遍历**                        |
| **mapPartitions**          | **分区遍历**                    |
| **mapPartitionsWithIndex** | **分区和值**                    |
| **glom**                   | **分区形成一个数组**            |
| **groupby**                | **条件分组**                    |
| **filter**                 | **过滤**                        |
| **repartition**            | **重分区**                      |
| **sample**                 | **抽样**                        |
| **distinct**               | **去重**                        |
| **coalesce**               | **缩减分区**                    |
| **sortBy**                 | **按不同规则排序**              |
|                            |                                 |
| **union**                  | **两RDD并集**                   |
| **subtract**               | **两RDD差集**                   |
| **intersection**           | **两RDD交集**                   |
| **cartesian**              | **笛卡尔积**                    |
| **zip**                    | **两RDD形成一个(k,v)RDD**       |
|                            |                                 |
| **groupByKey**             | **按key分区**                   |
| **reduceByKey**            | **按key做逻辑**                 |
| **sortByKey**              | **按key排序**                   |
| **join**                   | **连接（还有左右外）(K,(V,W))** |
| **cogroup**                | **连接为value两列**             |
| **aggregateByKey**         | **区内区外分别计算**            |
| **foldByKey**              | **区内区外共同计算**            |



### map&mapPartitions

- **map**

  常规操作，不进行shuffer

- **mapPartitions**

  1. 可以对一个RDD中所有的分区进行遍历
  2. 效率优于map，减少了发送到执行器的交互次数
  3. 可能会出现（OOM）（大于10亿条）
  4. 内存大时建议使用

  ```scala
  val data: RDD[Int] = sc.makeRDD(1 to 20)
  data.mapPartitions(word => {
    word.map(_*6)
  }).foreach(println)
  ```

- **mapPartitionsWithIndex**

  得到的值为分区号和值

  ```scala
  data.mapPartitionsWithIndex{
    case (parNum , value) => {
      value.map((_,"分区号为："+parNum))
    }
  }.foreach(println)
  ```


### glom&groupby&filter

- **glom**

  将每一个分区形成一个数组，形成新的RDD类型，便于观察数据分区情况

  ```scala
  val glomRdd: RDD[Array[Int]] = data.glom()
  glomRdd.foreach(x => x.foreach(println))
  ```

- **groupby**

  ```scala
  val groupby: RDD[(Int, Iterable[Int])] = data.groupBy(x => x%2)
  ```

- **filter**

  ```scala
  val filter: RDD[Int] = data.filter(x => x%2==0)
  ```


### repartition

- 这两个都是可以直接给值使用

- 重分区

- repartitionAndSortWithinPartitions

  必须是可排序的二元组 会根据key值排序参数可以是系统分区 也可以是自定义分区官方建议，如果需要在repartitio**n重分区**之后，还要进行**排序**，建议直接使用repartitionAndSortWithinPartitions算子。因为该算子可以一边进行重分区的shuffle操作，一边进行排序。shuffle与sort两个操作同时进行，比先shuffle再sort来说，性能可能是要高的

### coalesce

- 作用：缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
- 需求：创建一个4个分区的RDD，对其缩减分区，可以简单的理解为**合并分区**，不进行shuffer

### partitionBy

- 更改分区repartition可以从少量分区改变为多分区因为会发生shuffle根据分区数，从新通过网络随机洗牌所有数据。

- 重分区

```scala
var rdd2 = rdd.partitionBy(new HashPartitioner(2))
```



### sample

- sample(withReplacement,fraction, seed) 

作用：以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。

```scala
data.sample(false,0.4 , 1).foreach(println)
```

### distinct

- 作用：对源RDD进行去重后返回一个新的RDD。默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。进行shuffer
- Spark中所有的转换算子中没有shuffle的算子，性能比较快

![搜狗截图20191029083028](D:\software\typoraIMG\搜狗截图20191029083028.png)





### union

(otherDataset)：

返回一个新的dataset，包含源dataset和给定dataset的元素的集合





### intersection

- 交集



## Pair RDD

| 算子            | 解释                   |
| --------------- | ---------------------- |
| **groupBykey**  | **按key分组**          |
| **reduceByKey** | **按key聚合**          |
| **sortByKey**   | **按照key排序**        |
| **join**        | **连接（左右外连接）** |
| **cogroup**     |                        |
| cartesian       | **笛卡尔积**           |
| **keys**        | **得到key**            |
| **values**      | **得到value**          |
| **mapValues**   | **对value进行遍历**    |
| aggregateByKey  | **区内区外分别计算**   |
| foldByKey       | **区内区外共同计算**   |
| flatMapValues   | 遍历压平value          |

### groupByKey

- 返回(K,Seq[V])，也就是hadoop中reduce函数接受的key-valuelist
- 使用其，spark将所有的键值对都移动
- 不适用于大数据量

### reduceByKey

- 就是用一个给定的reducefunc再作用在groupByKey产生的(K,Seq[V]),比如求和，求平均数
- 性能优于groupByKey，



### sortByKey

- 按照key来进行排序，是升序还是降序，ascending是boolean类型



### join

- 必须是**kv**对，并且**类型要匹配**

```scala
val rdd1: RDD[(Long, Int)] = sc.makeRDD(0 to 26).zipWithIndex().map{case (k,v) => (v,k)}
val rdd2: RDD[(Long, Char)] = sc.makeRDD('A' to 'Z').zipWithIndex().map{case (k,v) => (v,k)}
rdd1.join(rdd2).foreach(println)
```





### cogroup

- 当有两个KV的dataset(K,V)和(K,W)，返回的是(K,Seq[V],Seq[W])的dataset,numTasks为并发的任务数

在join的基础上将value分开

```scala
val rdd1: RDD[(Long, Int)] = sc.makeRDD(0 to 26).zipWithIndex().map{case (k,v) => (v,k)}
val rdd2: RDD[(Long, Char)] = sc.makeRDD('A' to 'Z').zipWithIndex().map{case (k,v) => (v,k)}
rdd1.join(rdd2).cogroup(println)
```



### cartesian

- 笛卡尔积就是m*n



### aggregateByKey

- 参数：

  ```scala
  (zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V)=> U,combOp: (U, U) => U)
  ```

  1. 作用：在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。

  2. 参数描述：

     1. zeroValue：给每一个分区中的每一个key一个初始值；
     2. seqOp：函数用于在每一个分区中用初始值逐步迭代value；
     3. combOp：函数用于合并每个分区中的结果。

  3. 需求：创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加

     ```scala
     val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
     rdd.glom().collect().foreach(_.foreach(println))
     rdd.aggregateByKey(0)(math.max(_,_) , _+_).collect.foreach(println)
     ```

### foldByKey

- 参数

  ```scala
  (zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
  ```

- 需求：创建一个pairRDD，计算相同key对应值的相加结果

  ```scala
  val agg = rdd.foldByKey(0)(_+_)
  ```

- 说明：分区内和分区间在一起运算了

### flatMapValues

- 对value进行遍历压平

![搜狗截图20191101190752](D:\software\typoraIMG\搜狗截图20191101190752-1572606530227.png)

将数据转化为

![搜狗截图20191101190825](D:\software\typoraIMG\搜狗截图20191101190825.png)











## ![321378654](D:\software\typoraIMG\321378654.png)Action

- 触发真正的计算

  Spark程序执行到行动操作时，才会执行真正的计算，从文件中加载数据，完成一次又一次转换操作，最终，完成行动操作得到结果。

| 算子                   | 解释                                |
| ---------------------- | ----------------------------------- |
| **reduce**             | **逻辑操作**                        |
| **collect**            | **数据回收**                        |
| **stats**              | **类似summary**                     |
| **count**              | **统计**                            |
| **first**              | **RDD第一个元素**                   |
| **take**               | **前N个元素**                       |
| **takeSample**         | **按数量抽样**                      |
| **countByKey**         | **按照key进行聚合**                 |
| **foreach**            | **遍历**                            |
| **top**                | **取对应数的值，默认降序**          |
| fold                   | 折叠                                |
| **takeOrdered**        | **取对应数的值，默认升序**          |
| **filterByRange**      | **过滤返回范围内数据**              |
| **flatMapValues**      | **按照value进行扁平化**             |
| **keyBy**              | **分区内遍历**                      |
| **collectAsMap**       | **二元组转map**                     |
| **saveAsTextFile**     | **保存至文件**                      |
| **saveAsSequenceFile** | **以Hadoop sequencefile的格式保存** |
| **saveAsObjectFile**   | **序列化成对象**                    |



### reduce

- 传入的函数是两个参数输入返回一个值，传入函数必须满足交换律和结合律

### collect

- 把数据收回，有风险，生产环境禁用

### count

- 返回的是RDD中的element的个数

### first

- 返回的是RDD中的第一个元素

### take

- 返回前n个elements

### takeSample

- takeSample(withReplacement，num，seed)

- 抽样返回一个dataset中的num个元素备注：与sample类似，但第二个参数不是百分比

### saveAsTextFile

- 把dataset写到一个textfile中，或者hdfs，或者hdfs支持的文件系统中，spark把每条记录都转换为一行记录，然后写到file中

### countByKey

- 少用count类的，和collect性质类似
- 返回的是key对应的个数的一个map，作用于一个RDD

### countByValue

### foreach

- 对dataset中的每个元素都使用func

### top

- 取出对应数量的值 默认降序, 若输入0 会返回一个空数组

### flod

- 折叠

- 将该RDD所有元素相加得到结果

```scala
rdd.fold(0)(_+_)
```

### takeOrdered

- 顺序取出对应数量的值 默认升序

### filterByRange

- 对RDD中的元素进行过滤,返回指定范围内的数据

### flatMapValues

- 按照value进行扁平化

### foreachPartition

- 循环的是分区数据，一般应用于数据的持久化,存入数据库,可以进行分区的数据存储

### keyBy

- 以传入的函数返回值作为key ,RDD中的元素为value 新的元组

### collectAsMap

- 将需要的二元组转换成Map







## RDD控制算子

- 控制算子：cache，persist【持久化、缓存】、checkpoint【容错】
- 都是lazy的，需要action算子触发才能执行

### 缓存(持久化)

- source

```scala
def cache(): this.type = persist()
```

```scala
def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)
```

- 通过缓存，Spark避免了RDD上的重复计算，能够极大地提升计算速度；

- cache，persist两个做缓存，checkpoint做容错

  每一次action都从全面所有的标记重新开始计算包括读文件，尽量避免，例如catch

  MEMORY_AND_DISK:先保存在内存，内存不够放磁盘

  ...2本地一份，其他节点放一份

- **缓存的单位**

  做cache（缓存）的时候，以partition为单位

- **缓存级别的选择**

1. 优先选默认级别
2. 其次选序列化的方式（MEMORY_ONLY_SER）;
3. 不要选...2

- 缓存占用的磁盘空间spark负责管理

- **用缓存的时机**

  ​	需要对空间和速度进行权衡，垃圾回收开销的问题让情况变的更复杂。一般情况下，如果多个动作需要用到某个 RDD，而它的计算代价又很高，那么就应该把这个 RDD 缓存起来；

  是否磁盘，是否内存，是否多份

![搜狗截图20191030161219](D:\software\typoraIMG\搜狗截图20191030161219.png)

![搜狗截图20191030161606](D:\software\typoraIMG\搜狗截图20191030161606.png)

### 容错

```scala
//source
def checkpoint(): Unit = RDDCheckpointData.synchronized {
    if (context.checkpointDir.isEmpty) {
      throw new SparkException("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {
      checkpointData = Some(new ReliableRDDCheckpointData(this))
    }
  }
```

checkpoint（容错）：数据要放在安全的地方（hdfs），操作也是lazy的

当出现计算失败的情况，RDD中的lineage信息常用来在task失败后重计算使用，为了防止计算失败后从头开始计算造成的大量开销

- 是为了缩短依赖链条的

- 做checkpoint之前先做catch

- checkPoint**斩断依赖**（后续action直接从此处开始）
- checkPoint缓存的数据需要自己清除
- 最好将结果 checkpoint 到 hdfs，便于集群所有节点进行访问；

#### 什么时候 checkpoint

1. 在发生 shuffle 之后做 checkpoint
2. 经过复杂计算之后

#### checkpoint 的步骤

1. 建立 checkpoint 存储目录：

   sc.setCheckpointDir(“hdfs://node01.9000/ck”)

2. rdd1.cache()

3. rdd1.checkpoint()

4. // 做checkpoint之前做cache，避免重复计算



## 自定义排序





## 分区

- 目的：

  设置合理的并行度，提高数据处理的性能。

- 和运行模式有关，和资源有关，和集合，文件有关
- 两个原则
  1. 使分区等于集群核心数（资源完全利用）
  2. 分区中数据保持一致

- 数据要做合理的分区提高处理性能，
- hdfs的分区（128M块），读文件设置的分区数只能设置的比默认的块数多，后续也可以继续改

- repartition（有shuffer），coalesce（无shuffer）
- standalone模式下，小于128M的文件分两个区

### 分区数

1. local模式，默认分区个数等于本地机器的 CPU 核心总数
   分区的计算任务交付给单个核心执行，能够保证最大的计算效率；

2. Standalone 或者 Yarn，默认分区个数等于集群中所有核心数目的总和，或者 **2**，取两者中的较大值

3. 如果没有手动改变过那么从生成后永远都不会变。‘

4. textFile分片过程

   由指定cpu核数+指定分区数+block块大小+文件个数经过分片算法得到最终的分区数

### 宽依赖和窄依赖

- 窄依赖是1:1或者n:1的（map....）
- 宽依赖式n:n的（reduceByKey...）
- 宽依赖（shuffer）是job划分stage的依据
- 在宽依赖后丢数据重新计算会有很多重复计算

![搜狗截图20191030110629](D:\software\typoraIMG\搜狗截图20191030110629.png)

- 不同依赖的数据丢失问题

  窄依赖能够更有效地进行失效节点的恢复，即只需重新计算丢失RDD分区的父分区，而且不同节点之间可以并行计算；而对于一个宽依赖关系的Lineage图，单个节点失效可能导致这个RDD的所有祖先丢失部分分区，因而可能导致整体重新计算。

### stage的划分![搜狗截图20191030170416](D:\software\typoraIMG\搜狗截图20191030170416.png)



### 分区器

- 分区器决定了
  1. Rdd中分区的个数
  2. RDD中每条数据经过Shuffle过程属于哪个分区
  3. reduce的个数

- 只有Key-Value类型的RDD才有分区器，非Key-Value类型的RDD分区器的值是None。

#### HashPartitioner 

- 对于key-value的RDD，仅仅是可能存在分区器
- 分区器是在**shuffer**阶段对数据做分区
- 适用于大多数场景
- key若变，分区器就没了
- 内部按照key的hashCode模与分区数决定分区



#### RangePartitioner

- 适用于排序，来确定分区的边界值
- new RangePartitioner会产生job，但是仍然认为soryByKey属于transformation
- RangePartitioner在堆RDD进行采样（水塘采样，减少对RDD的遍历）的过程中产生了job
- 水塘采样也就是对于一个水塘遍历一次得到值，必须是均匀的



#### 自定义分区器

- 定义分区器（大于100为分区器1否则为0）

```scala
class MyPartitioner extends Partitioner {
  override def numPartitions: Int = 2
  override def getPartition(key: Any): Int = {
    val k = key.toString.toInt
    if (k > 100) 1 else 0
  }
}
```

- 测试

```scala
val rdd: RDD[Int] = sc.parallelize(List(10,2, 150,560 10, -500), 4)
val rdd1: RDD[(Int, Int)] = rdd.map((_, 1)).partitionBy(new MyPartitioner)
val str1: Array[String] = getElement(rdd1)
str1.foreach(println)
```



## 库

### mysql

- 连接

```xml
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.27</version>
</dependency>
```

- 查询

jdbcRDD需要上下限的原因是RDD需要分区的操作，只有限制范围才能更好的体现RDD

```scala
def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("rdd run").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://slave4:3306/rdd"
    val userName = "root"
    val passWd = "wang1997"

    val sql = "select *  from user where id >= ? and id <= ?"

    val JdbcRDD = new JdbcRDD(
      sc,   //上下文对象
      () => {  //需要一个连接对象
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      sql,//sql语句
      1,//上限
      3,//下限
      2,//分区
      (rs) => {
        println(rs.getString(1),rs.getString(2)+","+rs.getInt(3))//结果语句，结果的处理
      }
    )
    JdbcRDD.collect()
    sc.stop()
  }
```

- 操作数据库

```scala
val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("spark" , 22) , ("hadoop" , 30) , ("flink" , 33)))
dataRDD.foreachPartition(data => {
  Class.forName(driver)
  val connection: Connection = java.sql.DriverManager.getConnection(url , userName , passWd)
  data.foreach{
    case (username , age) => {
      val sql = "insert into user (name , age) values(? , ?)"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1 , username)
      statement.setInt(2 , age)
      statement.executeUpdate()
      statement.close()
    }
  }
  connection.close()
})
sc.stop()
```



## 共享变量

- 累加器和广播变量都由driver管理；通常用来提高优化spark程序
- 广播变量定义后只读；不能修改，由driver分发到每个executor上，减少数据传输，提高了性能
- 累加器由executor修改，driver只读，减少数据的扫描，提高性能
- 缺省情况下广播变量的大小为4M（压缩格式）

### 累加器

- 是lazy的，遍历一次拿到多结果，减少对数据的扫描
- 一次action触发一次，注意使用

- 累加器用来对信息进行聚合，通常在向 Spark传递函数时，比如使用 map() 函数或者用 filter() 传条件时，可以使用驱动器程序中定义的变量，但是集群中运行的每个任务都会得到这些变量的一份新的副本，更新这些副本的值也不会影响驱动器中的对应变量。如果我们想实现所有分片处理时更新共享变量的功能，那么累加器可以实现我们想要的效果。

```scala
例如reduce(_+_)这样一个累加操作，这样做的话会进行shuffer，那么使用累加器实现这样的功能就可以提高效率
```

```scala
val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4) , 2)
//创建累加器
val accumulator: LongAccumulator = sc.longAccumulator
rdd.foreach{
  case i => {
    accumulator.add(i)
  }
}
```

#### 自定义累加器1

- 自定义类继承AccumulatorV2

```scala
val total = new Total   //创建累加器
    sc.register(total)   //注册累加器
    rdd.foreach{
      case word => {   //执行累加器的累加功能
        total.add(word)
      }
    }
    println("sum = " + total.value)//获取累加器的值
    
class Total extends AccumulatorV2[String, util.ArrayList[String]] {
  private val list = new util.ArrayList[String]()

    //判断初始值是否为空
  override def isZero: Boolean = list.isEmpty

    //类型是返回一个累加器，进行拷贝
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new Total
  }

    //累加器的重新赋值，清空
  override def reset(): Unit = {
    list.clear()
  }

    //分区内合并
  override def add(v: String): Unit = {
    if (v.contains("h")){
      list.add(v)
    }
  }

    //分区间合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

    //返回
  override def value: util.ArrayList[String] = list
    
}
```

#### 自定义累加器2

- 实现一道题目需要多个计数器进行汇总结果（自定义累加器将每个需要的计数放到key中，value累加）

```scala
class CustomerAcc extends AccumulatorV2[String, mutable.Map[String, Int]]{
  private val bufferMap = mutable.Map[String, Int]()

  override def isZero: Boolean = bufferMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val thatAcc = new CustomerAcc()
    bufferMap.synchronized{
      thatAcc.bufferMap ++= bufferMap
    }
    thatAcc
  }

  override def reset(): Unit = bufferMap.clear()

  // 分区内合并
  override def add(word: String): Unit = {
    val value = bufferMap.getOrElse(word, 0) + 1
    bufferMap += (word -> value)
  }

  // 分区间合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    for ((key: String, value: Int) <- other.value){
      val newValue: Int = value + bufferMap.getOrElse(key, 0)
      bufferMap(key) = newValue
    }
  }

  override def value: mutable.Map[String, Int] = bufferMap
}

object Demo111 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val arr = List("hadoop spark", "java scala", "hive hbase",
      "hadoop spark", "java scala", "hive hbase",
      "hadoop spark", "java scala", "scala hadoop")

    val rdd = sc.makeRDD(arr)
    val newAcc = new CustomerAcc
    sc.register(newAcc, "newAccaaaaaa")

    rdd.flatMap(_.split("\\s+"))
      .foreach(word => newAcc.add(word))

    println(newAcc)

    sc.stop()
  }
}
```



### 广播变量

- 允许在每个机器上缓存一个只读的变量，而不是为机器上的每个任务都生成一个副本；

- 调优

- 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，甚至是机器学习算法中的一个很大的特征向量，广播变量用起来都很顺手。 在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。

```scala
scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(35)

scala> broadcastVar.value
res33: Array[Int] = Array(1, 2, 3)
```

使用广播变量的过程如下：

(1) 通过对一个类型 T 的对象调用 SparkContext.broadcast 创建出一个 Broadcast[T] 对象。 任何可序列化的类型都可以这么实现。 

(2) 通过 value 属性访问该对象的值(在 Java 中为 value() 方法)。 

(3) 变量只会被发到各个节点一次，应作为只读值处理(修改这个值不会影响到别的节点)。

#### 动态变化

- 从redis读取状态标志，如果变化重新删除读取新规则广播
- 直接将redis读取规则，每次动态获取



## Shuffer

```
有几个节点，画图，缓冲只有32k
```

- 优化
  1. 能少则少
  2. 减少shuffer传输的数据量（用的多）
- spark中的shuffle与MR中shuffer从高层上看，非常类似
- spark shuffle：将job切分为stage的依据

- spark Shuffer版本演进![搜狗截图20191102104623_看图王](D:\software\typoraIMG\搜狗截图20191102104623_看图王.png)

### hash Shuffer

- 缺点：过程中产生海量小文件

#### v1

- Shuffle件 过程中会生成海量的小文件－－同时打开过多文件 及 及  低效的随机 IO

![搜狗截图20191106083358](D:\software\typoraIMG\搜狗截图20191106083358.png)

#### v2

- 核心思想：允许不同的task复用同一批磁盘文件，有效将多个task的磁盘文件进行一定程度上的合并，从而大幅度减少磁盘文件的数量，进而提升shuffle write的性能。一定程度上解决了Hash V1中的问题，但不彻底

![搜狗截图20191106083429](D:\software\typoraIMG\搜狗截图20191106083429.png)



### Sort Shuffer

#### 普通形式

- sortShufferManager管理

- shuffer最终产生两个文件：数据文件+索引文件，彻底解决了小文件的问题

  过程

  - 将数据**保存到内存的数据结构**中，kv存为map，普通存为Array
    - 溢写之前先**排序**(默认batch10000)，使用java的**缓冲流**，内存**满溢后写**磁盘减少IO，
      - 最后将这些临时磁盘文件合并，称为**merge**，一个task只对应一个磁盘文件，因此还会单独写一份**index**文件，表示了下游各task数据在文件中的start offset和stop offset

  

![+595](D:\software\typoraIMG\+595.png)

#### by pass

- shuffer map task小于200执行bypass

  过程

  - 非聚合shuffer类算子按照key进行hash写入对应磁盘文件（先写到缓冲区最后将临时文件合并并且常见一个index文件）
    - 与普通的hashshuffer一样，只是最后会合并，shuffer writer过程中不进行排序，节省了开销。

![搜狗截图20191130203528](D:\software\typoraIMG\搜狗截图20191130203528.png)

#### 二者区别

- bypass：写机制不同，不会进行排序，

### Unsafe sort Shuffle

Shuffle在传递数据到Reducer端的过程中，数据存在序列化和反序列化操作，
对内存、CPU、GC的压力较大。Unsafe sort shuffle（坞丝计划）就是很好的解决方案。



- 速度测试

  ```
  单分区2g文件，wordcount最优化
  ```

  



### 思考

- 问题1：从整体上看Spark shuffle 与 Hadoop shuffle有什么区别？

  从high level来看，没有本质区别，实现（细节）上有区别
  Hadoop有明确的map、reduce的划分（spark没有）

- 问题2：为什么shuffle的过程中一定要写盘，能cache到内存中吗？

  1、shuffle在Map端的数据要存储到磁盘上，以防止 容错触发重算带来的庞大开销（如果保存到Reduce端内存中，一旦Reduce Task挂掉了，所有Map Task需要重算）
  2、减少对内存的占用，内存总是不够用的。

- 问题3：shuffle过程中的临时文件写到哪里？

  ```
  spark.local.dir
  SPARK_LOCAL_DIRS
  ```

  

### 重分区解释

- repartition  有shuffer
- coalesce     无shuffer （多变少无shuffer，少变多有shuffer）

### bypass触发

- 首选mapSideCombine，如果发生了map端的预聚合那么就不继续，如果没有，那么判断分区数，如果分区数小于200就触发bypass，进行hash的shuffer过程，如果分区数（task）大于200进行排序，hash一定程度上时可以充当查找功能的。

![搜狗截图20191103144643](D:\software\typoraIMG\搜狗截图20191103144643.png)

![4612](D:\software\typoraIMG\4612.png)



### ShuffleMapStage与ResultStage

- ShuffleMapStage的结束伴随着shuffle文件的写磁盘。

- ResultStage基本上对应代码中的action算子，即将一个函数应用在RDD的各个partition的数据集上，意味着一个job的运行结束。





## 作业

框架本身 → 组件交互 → 执行

### 作业提交

spark-submit用来提交spark作业（standalone）

目的是将任务划分为task交给executor，中间由三大组件完成。

- 三大组件
  1. **DAGScheduler** ：最主要的职责是对用户Job所形成DAG划分成若干个Stage；
  2. **TaskScheduler** ：负责管理task的分配及状态管理
     在分配task时，taskScheduler会将DAGScheduler提交的taskset进行优先级排序，这个排序算法目前是两种：FIFO（缺省）或FAIR。
     得到这一份待运行的tasks后，接下里就是要把schedulerBackend交过来的worker资源信息合理分配给这些tasks。
  3. **SchedulerBackend** ：定义了许多与Executor事件相关的处理，包括：新的executor注册进来的时候记录executor的信息，增加全局的资源量(核数)；executor更新状态，若任务完成的话，回收core；其他停止executor、remove executor等事件

- 作业

  application→DAG→job→stage→task→executor

- SparkContext（driver）包含三大组件：

  DAGScheduler（输入:DAG，输出:stage）

  TaskScheduler（输入：stage，输出：Task）

  SchedulerBackend（输入：Task，（与executor的关系））

- 过程
  1. 创建RDD对象；
  2. SparkContext负责计算RDD之间的依赖关系，构建DAG；DAG图提交给DAGScheduler进行解析；
  3. **DAGScheduler**负责把DAG图分解成多个Stage，每个Stage中包含了多个Task，最后将Stage交给
     TaskScheduler
  4. **TaskScheduler**将Stage分解为Task，每个Task会被TaskScheduler分发给各个Worker node上的Executor去执行

- 提交作业的四个阶段(全部发生在driver中)
  1. 构建DAG。提交的**job**首先被转换成一系列的**RDD**，并通过RDD之间的依赖关系构建**DAG**；
  2. DAGScheduler 将 DAG 切分为**stage**，将stage中生成的task以taskset的形式发送给TaskScheduler
  3. Scheduler调度**task**（根据资源情况将task调度到Executors）；
  4. **Executors**接收task，然后将task提交到线程池执行；

![搜狗截图20191104101649](D:\software\typoraIMG\搜狗截图20191104101649.png)







### 作业调度

- 过程
  1. Driver启动，初始化sparkContext，向master注册，申请资源
  2. 应用程序注册，集群有足够资源，
  3. master通知worker，启动executor
  4. executor注册到Driver
  5. driver发送任务（DAG→stage→task）到executor进行执行
  6. 执行过程中executor给driver反馈任务执行状态（driver监控executor执行情况）
  7. 应用程序执行完毕driver通知master注销应用，反馈给executor让其回收资源

![搜狗截图20191104103041](D:\software\typoraIMG\搜狗截图20191104103041.png)



### 作业执行

- CoarseGrainedExecutorBackend.run

  - executor.launchTask

    threadPool.execute(tr)



- action算子
  - runjob(RDD，SparkContext)一路runjob
    - DAGScheduler → submitJob提交作业 → eventProcessLoop.post（将JobSubmitted作业放到这个队列中，点进入的EventLoop中有run方法→方法中有onReceive，实现是在DAGScheduler 中） → EventLoop → eventQueue → BlockingQueue（进数据（进jobSubmiter），满了阻塞，出数据，满了阻塞）

      - DAGScheduler 的onReceive → doOnReceive → dagScheduler.handleJobSubmitted → createResultStage（创建一个完成的过程） → getOrCreateParentStages（获取或者创建一个阶段）→ 创建一个**ResultStage**

        → getShuffleDependencies（获取shuffer的描述器，有无shuffer依赖取决于最终的parents结果，判断就是看当前的RDD有没有shuffer依赖）

        → 判断是否有shuffer，从最后开始，如果有就加一（parents += shuffleDep，向栈里面push），如果没有一层一层向前寻找，没找到返回,parrent就是多个阶段

        → getOrCreateShuffleMapStage获取或者创建shuffer的map阶段（已经拿到了shuffer依赖）

        → 根据依赖，创建一个**ShuffleMapStage**

        → 将阶段划分完统一创建一个ResultStage赋值给finalStage → 传入ActiveJob → job中包含着stage，其实只有一个resultstage，只是里面还有其他的依赖

      - submitStage提交resultStage → getMissingParentStages继续判断依赖关系

        → 递归判断依赖，如果不为空一直调用，跳出条件就是为空

        → 判断到最前面，那么提交的任务就是最开始的shufferMapStage → submitMissingTasks进行提交 → 判断具体是那种stage，一共就两种

      - 匹配到stage，开始划分task → stage.pendingPartitions计算分区 → 遍历将每一个分区转换成task → 判断task（两种stage对应着**shufferMapTask**和**resulttask**），如果数量大于0那么开始提交任务

      - submitTasks进行提交 → 将作业实例化为一个TaskSet → 提交后在TaskSchedulerImpl中执行 → addTaskSetManager添加 → rootPool.addSchedulable将所有任务添加到池中（等待资源准备好从池中取），

        → 有先进先出和公平调度两种

      - backend.reviveOffers()提交，在CoarseGrainedSchedulerBackend中实现launchTasks发送task，进行序列化，executorData.executorEndpoint.send(LaunchTask（Driver端将启动的任务发送给executor，进行通信交互）

      - 随即在CoarseGrainedExecutorBackend中LaunchTask进行启动任务，将受到的任务进行反序列化，将任务 → executor.launchTask发送给计算对象进行计算

- 公平调度

  公平排序算法，依据三个参数：

  runningTasks值（正在运行的Task数）、minShare值、weight值

  - runningTasks一大一小，虽然有的执行的比较多，但是任务也多，执行的比例就比较小
  - runningTasks一样看比率，比率低排前面
  - 权重小排前面

- 失败重试与黑名单机制

  - Driver向executor发送数据由失败的概率，失败后继续重试
  - 发送失败次数过多就进入黑名单，不在给其发送

![提交划分](D:\software\typoraIMG\提交划分.png)

### shuffle

#### 读写操作的存在

- **shufferMapTask**和**resulttask**两种task

- 从作业的划分结束将任务 → 在CoarseGrainedExecutorBackend中executor.launchTask发送给计算对象进行计算

  → 在Executor的launchTask中执行线程池中的任务threadPool.execute，

  → run → task.run （此处的task就是Driver端传来的task），shufferMapTask和resulttask两种task任务都调用的父类task的run 方法，最后都是调用runTask

  - → resulttask一定伴随着读操作，func中将rdd迭代，判断存储级别是否为NONE，如果不为None那么进行调用计算，

    → getOrCompute调用到RDD中，接着调用computeOrReadCheckpoint计算或者读检查点，然后执行compute（每一个RDD都会有一个计算的功能），（RDD可以进行计算吗）

    → 在一个ShuffledRDD中进行实现，就有了read操作

  - → shuffleMapTask调用runtask方法中直接就有一个writer.write写方法，在写方法中包含着与resultTask类似的写操作，因为此时的读意味着要有数据源头，无非就是文件或者是上一个阶段的数据

  - → 在SortShuffleWriter中存在writer方法，

    → shuffleBlockResolver.writeIndexFileAndCommit 在 writeIndexFileAndCommit类中就伴随着熟知的两个文件，index和data

    ```scala
    一个新文件进来 → 删除旧文件 → 新文件更名 → 覆盖旧文件
    ```

    shuffle左右对应读写，map和reduce对应着写和读，那么resultTask呢

  - → 在ShuffleMapTask中调用manager.getWriter，对应的实现到SortShuffleManager中 → handle决定了具体使用writer来写入

    对应着 unsafeShuffleHandle ， bypassMergeSortHandle ， other (普通的sortshuffer)

  - 在registerShuffle方法中有三种

    对应  SerializedShuffleHandle ， BypassMergeSortShuffleHandle ， BaseShuffleHandle

    - 当来的shuffer对应shouldBypassMergeSort走bypass

      当dep.partitioner.numPartitions <= bypassMergeThreshold，任务数小于参数进行bypass，但是首选如果是dep.mapSideCombine，如果算子有map端预聚合就不会有bypass

    - 当来的shuffer对应canUseSerializedShuffle走unsafeShuffle

      只要有aggregator类预聚合操作都不进行shuffer

      否则进行shuffer

    - 在SortShuffleWriter对应的writer

      判断是否预聚合，然后做排序，接着insertAll，接着判断是否预聚合

      溢写maybeSpillCollection → maybeSpill判断是否溢写 → spill(collection)溢写集合 

      在ExternalSorter中有一个溢写实现 spillMemoryIteratorToDisk，将内存的数据溢写到磁盘中

#### HashShuffle

#### sortShuffer

- 为什么任务数量多了就得是sort，不是效率低吗



## 常见错误

1. driver中的类无法传到executor问题，序列化问题

   ```scala
   class T1(val id: Int, name: String)
   val rdd1 = sc.makeRDD(1 to 10)
   val t1 = new T1(10, "andy")
   rdd1.map(x=> x + t1.id)
   ```

2. RDD无法嵌套

   ```scala
   val rdd1 = sc.makeRDD(1 to 10)
   val rdd2 = sc.makeRDD(11 to 20)
   rdd1.map(elem => rdd2.map(x=>x+elem))
   ```

3. 数组不能作为key，但是list可以（当key为集合时不能使用array）

   因为数组是用的内存地址(hashcode)做key

4. 线程不安全：

   1. 可变的集合线程不安全

5. spark编译

6. 序列化优化

   1. 在调用算子的过程中，函数里最好不要new对象，如果数据量大的话，会生成很多对象占用大量的内存
   2. 在计算的过程中，尽量避免将大量的对象从driver端传输的executor
   3. 涉及到对象的时候最好用单例对象（样例类），这样就可以在同一个JVM中公用这个对象了，而不会生成多个对象

   - RDD计算过程中，调用算子和传入算子的



## 提升

### 编译

- 参考官方文档

### 集群搭建

1. 集群瓶颈

   磁盘io：**小盘多块普通**：绑定存储的时候可以绑定多块盘同时io

   网络io：交换机，光前

   cpu：机器学习，复杂的计算

2. 导致集群增长的原因

   数据量

   内存

   计算性能

3. 集群规模

   数据膨胀和数据压缩差不多可以抵消,

   - 数据规模

     hdfs副本数，保存周期，每日数据增量，历史数据量，预留磁盘空间，数据保存格式，数据膨胀

4. 节点配置

   1. 从节点

      小盘多块，

   2. 主节点

      双电源,双网卡,使用RAID阵列备份数据,配置高，

### RPC原理

- RPCenv ，
- RPCendpoint ，:消息通信体，收发消息，处理消息，master，worker都是消息通信体
- RPCendpointRef：远程消息通信体



- 远程过程调用

- 不是多线程,是actor模型,通过消息传递,进程通信使用akka，spark将akka换成了netty

- 2.0新特性，akka换成netty，sql等等，

- spark的整体架构**RPCenv**(**通信指挥官**)，里面的endpoint是消息通信体，收消息，发消息RPCendpoint的生命周期：constructor→onstart→receive→onstop

  ​		构造		启动	接受	停止

  **receive**，**receiveAndReply**（标识优先等级高），它俩分别接收来自RpcEndpoint 的**send**或**ask**过来的消息

  ask是同步，   send是异步

  endpointRef是远程通信体，内部有地址

  - RPCenv
    1. setupEndpoint  注册
    2. setupEndpointRef   获得引用

- 在spark框架中，RpcEndpoint就是用来接受消息并处理



### master&worker的通信

- 通信框架（）→master&worker中先创建RpcEnv的实例，因为通信框架，所有实例化的master是线程安全的，startRpcEnvAndEndpoint为启动消息框架的方法

- master启动
  - onstart
    1. 预写日志，启动web
    2. 在高可用启动恢复master
       1. zookeeper ， 网络文件系统(FileSystem) ， custom（自定义），直接new
       2. 挂掉拉起过程中，正在运行的作业正常运行，因为资源已经分配完了，但是新作业无法申请到资源。

- worker启动

  - onstart

    （registerWithAllMaster）

    1. 判断注册，
    2. 绑定web ui
    3. worker启动时会创建通信环境**RpcEnv**和终端点**EndPoint**，随即向master注册（**setupEndpointRef**），开一个线程池**registerMasterThreadPool**同时向多个master注册(高可用)，发送注册worker的消息**RegisterWorker**
    4. 发送消息后等待回应（消息体为case class  里面有host，port，id，cores，memory.....），发送的消息在master的receive中
    5. 发送到了master的**receiverAndReply**中
       1. master如果是standby，就发送一个case object出去
       2. 如果是已经注册过，返回一条消息，
       3. 如果正常，那么回应一条**receiveredAndReply**消息,
       4. 已注册为**RegisteredWorker**
    6. worker接受到消息，在**RegisteredWorker**中，然后会定期的发送心跳，master如果一段时间接受不到心跳，（**sendHeartbeat**），那么master就把这个worker移掉

- **自定义master和worker通信**

  - master

    伴生对象伴生类

    - 伴生对象系统名，endpoint名，主机，端口，RpcEnv创建实例，然后注册，内部半生类的参数
    - 半生类选择需要的参数，重写onstart，和receiver接收方法，接受到worker的消息

  - worker

    伴生对象和半生类

    - 伴生对象创建RpcEnv，注册和获取引用，发送消息
    - 伴生类定义需要的参数，重写onstart，和receiver接收方法，收到最终结果

  

![搜狗截图20191127214631](D:\software\typoraIMG\搜狗截图20191127214631.png)

### 内存管理

- 目前是**统一内存管理**，以前是**静态内存管理**

- 分为堆内内存和堆外内存

- 堆内内存
  1. **存储内存（Storage）**：放RDD
  2. **执行内存（Execution）**：放shuffer有关数据
  3. **其他（other）**：与RDD无关的对象，集合，（其他信息）...
  4. 使用序列化的方式，拿不到具体内存大小

- 静态：

  不允许越界，会造成某一块的OOM，

- **统一内存管理**

  - 四块一起叫系统内存，前三块叫

  **系统预留区**占用300M内存

  **存储内存**和**执行内存**对半（执行内存的**优先级别**更高）

  **其他内存**40%

  存储越界执行饱和可刷盘

  执行越界存储饱和不需立刻刷盘

  ![搜狗截图20191128150426](D:\software\typoraIMG\搜狗截图20191128150426.png)

  - 执行内存突破限制后，如果存储内存有新的内存需求，执行内存不会立刻释放资源

  ![搜狗截图20191127214743](D:\software\typoraIMG\搜狗截图20191127214743.png)

### 资源调优

- 资源提交示例

  所有参数都可以在sparkConf中调为硬编码，也可以是submit提交的灵活参数

  ```
  ./bin/spark-submit \
  --master yarn-cluster \
  --num-executors 100 \
  --executor-memory 6G \
  --executor-cores 4 \
  --driver-memory 1G \
  --conf spark.default.parallelism=1000 \
  --conf spark.memory.fraction=0.3 \
  --conf spark.memory.storageFraction=0.7
  ```

- 资源的估算（估算数据需要多少个core）

  - 官网建议

    每个core分配2G左右内存

    为每个core分配2-4个Task

  

  HDFS大小 → HDFS分块数 → spark job共多少task → 2-3Task/core → spark app需要多少core → Executor

- 其他因素

  压缩格式问题

  大宽表：是否全读，读多少

- 量级：

  日活450万，月活1200万，总用户数1.2亿，但是超过十分钟的只占了50%，

  一个人1k，每天的日增大概在250w×50K+200w×20K=125000000条（K）=157G，除去基础运动的加上现在的圈子，电商，广告，450w×100k=450000000条k = 500G , 主要是移动端，加上端上总和大概**700G**，峰值**1T**，

- 资源计算：

  HDFS700G →  5500task →  2000core →  每个executor配4个core，8G内存 →  按照执行3轮  →  200个executor →  集群50台节点16core，

- 一个executor10个core，一个core跑一个task，10个core50个task跑五轮



#### 作业调度基本原理

- 大方向
  - spark-submit → 根据不同部署模式启动Driver → 占用一定的资源人为设定
    - 申请资源(Executor进程)，yarn根据设定的参数在各节点启动Executor
      - 申请到后Driver进行stage、task级别调度，将最小计算单元task分配到各Executor
        - 一个stage全部执行完毕出结果，Driver运行下一个stage，全部执行完结束、
          - 一个executor上分配多个task，都是以每个task一条线程的方式，多线程并发执行。

- SparkSubmit提交
  - 启动一个Driver（本地或其他节点），根据参数占用资源
    - DAG→job→stage→task发给executor，executor内存根据设置占用三块，每个task一条线程多线程并发执行、

作业 → runjob → dagScheduler.runJob → submitJob





### 数据本地性

1. **PROCESS_LOCAL**:数据和计算在一个JVM（一个JVM代表一个executor）少见
2. **NODE_LOCAL**:数据和计算在一个节点
3. **NO_PREF**:数据从外部访问
4. **PACK_LOCAL**:跨机架
5. **ANY**:

- spark是PROCESS_LOCAL，如果没有就等待，如果没有拿到再降级，依次向下（这种方式叫做**延迟调度**）

### 数据倾斜

- 数据倾斜决策树

![数据倾斜](D:\software\typoraIMG\数据倾斜.png)

- 什么是数据倾斜，什么时候发送数据倾斜，为什么会发生数据倾斜，数据倾斜的表现，如何定位数据倾斜的代码，代码如何修改

- 在shuffer阶段由于数据中key分布的不均匀，导致分配给到具体executor的分区中的数据数量不同，造成不同executor之间运行效率偏差过大的现象。（有某数据块过大的情况也算），

- shuffer，基本上都是hashpartitionner造成的

- 对key进行hash后的某些key太多

- 数据倾斜的表现

  处理速度慢

  OOM

- 定位：

  shuffer算子，web界面

- 代码修改

  1. hive表数据倾斜（X）
  2. 过滤少数倾斜的key（X）
  3. 提高shuffer并行度（需要不多，）
     - 调整分区数，找准发生的地方，在之前进行分区数调整，成本低
  4. 两阶段聚合（局部聚合+全局聚合）
     - 前面撒盐聚合，后面拆聚合
  5. map jion（广播小表）（RDD转DS进行join，那就自动调整了）
  6. 采样倾斜key分拆join操作
     - 倾斜表和非倾斜表同时做过滤，然后再撒盐，扩容
     - 如果非倾斜表过滤成了小表，两表又可以做map join
  7. 随机前缀和扩容RDD进行join（注意撒盐数量）
  8. 多种方案组合
  9. 改变key的定义（数据打散还要保证结果不变）





- **两阶段聚合**

  ```scala
  //拿wordcount进行举例
      val parAggregateKey: RDD[(String, Int)] = words.mapPartitions(ite => ite.map { case word => {
          //局部聚合加随机数已经将数据打散了
        val rdm: Int = new Random().nextInt(8)
        (rdm + "_" + word, 1)
      }
      })
        .aggregateByKey(0)(
          (buf, elem) => {
            buf + elem
          }
          ,
          (buf1, buf2) => {
            buf1 + buf2
          }
        )
  
      println("***********************")
  
      //全局聚合overall
      //撒盐
      parAggregateKey.filter(_._1.split("_")(0)!="").mapPartitions(ite => ite.map{case word => {
        (word._1.split("_")(1) , word._2)
      }})
          .aggregateByKey(0)(
        (buf, elem) => {
          buf + elem
        }
        ,
        (buf1, buf2) => {
          buf1 + buf2
        }
      ).foreach(println)
  ```

- **随机前缀并扩容**

  ```scala
  //普通数据
      val data: RDD[(String, Int)] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\words.txt")
        .mapPartitions(ite => {
          ite
        }.flatMap(line => {
          line.split(" ").map(word => (word, new Random().nextInt(1000)))
        })
        ).filter(_._1 != "")
  
  
      //倾斜数据
      val data2: RDD[(String, Int)] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\words2.txt")
        .mapPartitions(ite =>
          ite.flatMap(line => {
            line.split(" ").map(word => (word, new Random().nextInt(100000)))
          })
        ).filter(_._1 != "")
  
      //对非倾斜数据集进行扩容
      val newData: RDD[(String, Int)] = data.mapPartitions(ite => {
        ite.map { case (key, value) => {
          //扩容了20倍
          for (i <- 0 to 19) yield {
            (i + "_" + key, value)
          }
        }
        }
      }).flatMap(x => x)
  
      //对倾斜数据集进行撒盐
      val newData2: RDD[(String, Int)] = data2.mapPartitions(ite => {
        ite.map { case (key, value) => {
          (new Random().nextInt(20) + "_" + key, value)
        }
        }
      })
  
  
      newData2.join(newData)//.foreach(println)
  
      data.join(data2)//.foreach(println)
  ```

  



### 动态资源分配

- 开始为standalone模式和mesos提供了动态管理，如果executor在一段时间内空闲就会移除这个executor。默认是false，也就是静态的，一直占用。

- **动态调节**，空闲时并且超过一定时间时释放，默认false      

  ```scala
  spark.dynamicAllocation.enabled=false
  ```

  **executor**空闲时间超过spark.dynamicAllocation.executorIdleTimeout设置的值(默认**60s**)，该executor会被移除，除非有缓存数据。

- **动态申请**

  如果有新任务处于等待状态，并且等待时间超过<Spark.dynamicAllocation.schedulerBacklogTimeout>默认为1s，则会依次启动executor，每次启动1..2..4..8..个executor，启动间隔为spark.dynamicAllocation.sustainedSchedulerBacklogTimeout，默认与上面相同。

- 在thrift中是有



### shuffer调优

- 编程方面

  减少shuffer的数量，规避shuffer

- 参数方面‘

  ```scala
  编程有关的shuffle调整：减少shuffle传输的数据量、规避shuffle
  
  shuffle参数调整，与编码无关。个人的观点：收益有限！！！
  
  shuffer writer 过程中buffer的大小；默认32K => 64K甚至更大
  
  shuffer reader过程中buffer的大小 ；默认48M => 96M甚至更大
  
  调整spark执行内存；调大。spark.memory.storageFraction (0.5)
  
  spark  reader过程中拉取数据的次数；默认3次
  
  spark  reader过程中重新拉取数据等待的时间；默认5s
  
  shuffle过程中是否为byPass的方式；默认值：200
  ```

### Spark Streaming反压（背压）

<font color=red> **反压，与数据积压，下游处理慢**</font>

- 最想要的是处理时间小于滑动时间，不能大于，也不能远小于

- 产生的原因是在这一个时间段一次拉取的数据大于自己能处理的数据量，就产生了batch processing time > batch interval 的时候，也就是每个批次数据处理的时间要比 Spark Streaming 批处理间隔时间长；越来越多的数据被接收，但是数据的处理速度没有跟上，导致系统开始出现数据堆积，可能进一步导致 Executor 端出现 OOM 问题而出现失败的情况。

- 调整

  <font color=red>**receiver**</font>

  ```scala
  spark.streaming.receiver.maxRate ，
  单位为数据条数/秒
  ```

  <font color=red>**Direct**</font>

  ```scala
  spark.streaming.kafka.maxRatePerPartition 来限流，单位是每分区的数据条数/秒。
  ```

  <font color=red>**直接打开反压机制**</font>

  ```scala
  能够根据当前系统的处理速度智能地调节流量阈值，名为反压（back pressure）机制。
  spark.streaming.backpressure.enabled 设为true就可以（默认值为false）
  ```

  还有情况，那如果是第一次启动就有大批数据呢，需要设下面参数

  ```
  spark.streaming.kafka.maxRatePerPartition
  此参数代表了 每秒每个分区最大摄入的数据条数。
  该参数代表了整个应该用声明周期的最大速率，背压调整也不会超过该参数。
  spark.streaming.backpressure.initialRate
  启用反压机制时每个接收器接收第一批数据的初始最大速率。默认值没有设置。
  ```

  

### spark on yarn

- <font color=blue>**Standalone**</font>：

  1、sparkContext初始化，向mater注册并申请资源

  2、master通知worker启动executor

  3、worker启动executor

  4、executor向driver反向注册

  5、drvier => DAG => TaskScheduler => SchedulerBackend，发送Task到executor

  6、executor 执行 task，并向driver汇报执行情况

  7、driver 注销，资源回收

- <font color=blue>**Spark On Yarn --client**</font>

  1、sparkContext初始化(Driver)，申请启动resource manager

  2、resource manager通知 node manager 启动 container

  3、node manager 启动 container，在 container中启动AppMaster

  4、AppMaster 向 RM 申请资源

  5、RM 通知 NM 启动 container

  6、container 向 Driver 注册

  7、... ...

- <font color=blue>**Spark On Yarn --Cluster**</font>

  1、提交脚本，sparkcontext申请启动resourcemanager

  2、resourcemanager通知NM启动continue

  3、continue中启动AppMaster

  4、在AppMaster中启动Driver

  5、Driver向resourcemanager申请资源

  6、Driver通知NM启动Executor进程

  7、Executor反向注册与Driver

  8、... ...



- 在yarn模式中

  AppMaster的作用：注册应用程序，申请资源

  Driver：executor的反注册，Task的分发，监控。





- Thrift

  启动一个后台线程

  瘦客户端，胖客户端





enum类似枚举

