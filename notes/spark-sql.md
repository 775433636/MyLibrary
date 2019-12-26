sql

## 概述

spark2.0新特性

- 新的入口点
- 数据结构
- shuffer

### 介绍

Spark SQL是Spark用来处理结构化数据的一个模块，它提供了2个编程抽象：DataFrame和DataSet，并且作为分布式SQL查询引擎的作用。
我们已经学习了Hive，它是将Hive SQL转换成MapReduce然后提交到集群上执行，大大简化了编写MapReduc的程序的复杂性，由于MapReduce这种计算模型执行效率比较慢。所有Spark SQL的应运而生，它是将Spark SQL转换成RDD，然后提交到集群执行，执行效率非常快！

- 特点
  1. 易整合
  2. 统一数据访问方式
  3. 兼容hive
  4. 标准的数据连接



### SparkSession

- SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。
- spark2.0之后的新入口点，内部有sparkContext

### Schema

- 有哪些列，数据的列名，数据的类型，是否允许为空，列的注释





### 运行架构

SparkSQL对SQL语句的处理和关系型数据库采用了类似的方法，SparkSQL会先将SQL语句进行解析Parse形成一个Tree，然后使用Rule对Tree进行绑定、优化等处理过程，通过模式匹配对不同类型的节点采用不同的操作。而SparkSQL的查询优化器是Catalyst，它负责处理查询语句的解析、绑定、优化和生成物理计划等过程，Catalyst是SparkSQL最核心的部分，其性能优劣将决定整体的性能。

​    SparkSQL由4个部分构成

1. Core：负责处理数据的输入/输出，从不同的数据源获取数据（如RDD、Parquet文件），然后将查询结果输出成DataFrame
2. Catalyst：负责处理查询语句的整个过程，包括解析、绑定、优化、物理计划等
3. Hive：负责对Hive数据的处理
4. Hive-thriftserver：提供CLI和JDBC/ODBC接口等

### 运行流程

1. 将SQL语句通过词法和语法解析生成未绑定的逻辑执行计划（Unresolved LogicalPlan），包含Unresolved Relation、Unresolved Function和Unresolved Attribute，然后在后续步骤中使用不同的Rule应用到该逻辑计划上
2. Analyzer使用Analysis Rules，配合元数据（如SessionCatalog 或是 Hive Metastore等）完善未绑定的逻辑计划的属性而转换成绑定的逻辑计划。具体流程是县实例化一个Simple Analyzer，然后遍历预定义好的Batch，通过父类Rule Executor的执行方法运行Batch里的Rules，每个Rule会对未绑定的逻辑计划进行处理，有些可以通过一次解析处理，有些需要多次迭代，迭代直到达到FixedPoint次数或前后两次的树结构没变化才停止操作。
3. Optimizer使用Optimization Rules，将绑定的逻辑计划进行合并、列裁剪和过滤器下推等优化工作后生成优化的逻辑计划。
4. Planner使用Planning Strategies，对优化的逻辑计划进行转换（Transform）生成可以执行的物理计划。根据过去的性能统计数据，选择最佳的物理执行计划CostModel，最后生成可以执行的物理执行计划树，得到SparkPlan。
5. 在最终真正执行物理执行计划之前，还要进行preparations规则处理，最后调用SparkPlan的execute执行计算RDD。



## 数据结构

### DataFrame

- 与RDD类似，DataFrame也是一个分布式数据容器。然而DataFrame更像传统数据库的二维表格，除了数据以外，还记录数据的结构信息，即schema。同时，与Hive类似，DataFrame也支持嵌套数据类型（struct、array和map）。从API易用性的角度上看，DataFrame API提供的是一套高层的关系操作，比函数式的RDD API要更加友好，门槛更低。

- 特点

  1. Schema : 包含了以ROW为单位的每行数据的列的信息
  2.  Spark通过Schema就能够读懂数据,因此在通信和IO时就只需要序列化和反序列化数据

  2. 懒执行的，性能比RDD高

  3. 查询计划通过Spark catalyst optimiser进行优化。

### DataSet

- 内部是case class

1. 是Dataframe API的一个扩展，是Spark最新的数据抽象。
2. 用户友好的API风格，既具有类型安全检查也具有Dataframe的查询优化特性。
3. Dataset支持**编解码器**，当需要访问非堆上的数据时可以避免反序列化整个对象，提高了效率。
4. 样例类被用来在Dataset中定义数据的结构信息，样例类中每个属性的名称直接映射到DataSet中的字段名称。
5. Dataframe是Dataset的特例，DataFrame=Dataset[Row] ，所以可以通过as方法将Dataframe转换为Dataset。Row是一个类型，跟Car、Person这些的类型一样，所有的表结构信息我都用Row来表示。
6. DataSet是强类型的。比如可以有Dataset[Car]，Dataset[Person].
7. DataFrame只是知道字段，但是不知道字段的类型，所以在执行这些操作的时候是没办法在编译的时候检查是否类型失败的，比如你可以对一个String进行减法操作，在执行的时候才报错，而DataSet不仅仅知道字段，而且知道字段类型，所以有更严格的错误检查。就跟JSON对象和类对象之间的类比。

- Dataset是一个类（RDD是一个抽象类，而Dataset不是抽象类），其中有**三个参数**：
  1. SparkSession（包含环境信息）
  2. QueryExecution（包含数据和执行逻辑
  3. Encoder[T]：数据结构编码信息（包含序列化、schema、数据类型）

## take

![搜狗截图20191105085454](D:\software\typoraIMG\搜狗截图20191105085454.png)

![QQ截图20191031205433](D:\software\typoraIMG\QQ截图20191031205433.png)

- 在后期的Spark版本中，DataSet会逐步取代RDD和DataFrame成为唯一的API接口。

- 转换

| source    | sink      | coding                        |
| --------- | --------- | ----------------------------- |
| RDD       | DataFrame | DataFrame = RDD[Row] + schema |
|           |           | .toDF("name","age")           |
| RDD       | DataSet   | RDD[case class].toDS          |
| DataFrame | RDD       | .rdd                          |
| DataFrame | DataSet   | DF.as[case class]             |
| DataSet   | RDD       | .rdd                          |
| DataSet   | DataFrame | .toDF                         |

导包  	 

```scala
import spark.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```



### DataFrame

- 在Spark SQL中SparkSession是创建DataFrame和执行SQL的入口，创建DataFrame有三种方式：通过Spark的数据源进行创建；从一个存在的RDD进行转换；还可以从Hive Table进行查询返回。

#### 创建

- 从spark数据源创建

  支持的数据格式

  ```scala
  csv   format   jdbc   json   load   option   options   orc   parquet   schema   table   text   textFile
  ```

  1. 读json文件创建DataFrame

  ```scala
  val df = spark.read.json("/test.json")
  df.show
  ```

#### sql风格

- 对DataFrame创建一个临时表

  ```scala
  df.createOrReplaceTempView("people")
  ```

- 通过SQL语句实现查询全表

  ```
  val sqlDF = spark.sql("SELECT * FROM people")
  ```

  注：临时表是Session范围内的，Session退出后，表就失效了。如果想应用范围内有效，可以使用全局表。注意使用全局表时需要全路径访问

- DataFrame创建一个全局表

  ```scala
  df.createGlobalTempView("people")
  ```

- SQL语句实现查询全表

  ```scala
  spark.sql("SELECT * FROM global_temp.people").show()
  ```

  ```scala
  spark.newSession().sql("SELECT * FROM global_temp.people").show()
  ```

#### DSL风格

- 查看schema的信息

  ```scala
  df.printSchema
  ```

- 只查看”name”列数据

  ```scala
  df.select("name").show()
  ```

- 查看”name”列数据以及”age+1”数据

  ```scala
  df.select($"name", $"age" + 1).show()
  ```

- 查看”age”大于”21”的数据

  ```scala
  df.filter($"age" > 21).show()
  ```

- 按照”age”分组，查看数据条数

  ```scala
  df.groupBy("age").count().show()
  ```

#### RDD转换为DateFrame

- 注意：如果需要RDD与DF或者DS之间操作，那么都需要引入 **import spark.implicits._**  【spark不是包名，而是sparkSession对象的名称】

```scala
import spark.implicits._
val df = spark.read.json("/test.json")
```

- 通过手动确定转换

```scala
peopleRDD.map{x=>val para = x.split(",");(para(0),para(1).trim.toInt)}.toDF("name","age")
```

- 通过反射确定

1. 创建一个样例类

   ```
   case class People(name:String, age:Int)
   ```

2. 根据样例类将RDD转换为DataFrame

   ```scala
   peopleRDD.map{ x => val para = x.split(",");People(para(0),para(1).trim.toInt)}.toDF
   ```

- ```scala
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  val arr = Array(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
  val rdd1 = sc.makeRDD(arr).map(f=>Row(f._1, f._2, f._3))
  val schema = StructType( StructField("name", StringType, false) :: StructField("age", IntegerType, false) ::
  StructField("height", IntegerType, false) :: Nil)
  val rddToDF = spark.createDataFrame(rdd1, schema)
  rddToDF.orderBy(desc("name")).show(false)
  ```

  



#### DateFrame转换为RDD

- 直接调用rdd即可

  1. 创建一个DataFrame

     ```scala
     val df = spark.read.json("/test.json")
     ```

  2. 将DataFrame转换为RDD

     ```SCALA
     val dfToRDD = df.rdd
     ```

### DataSet

#### row

```scala
import org.apache.spark.sql.Row
val row1 = Row(1,"abc", 1.2)
// Row 的访问方法
row1(0)
row1(1)
row1(2)
row1.getInt(0)
row1.getString(1)
row1.getDouble(2)
row1.getAs[Int](0)
row1.getAs[String](1)
row1.getAs[Double](2)
```



#### Schema

- DataFrame（即带有Schema信息的RDD）Spark通过Schema就能够读懂数据

```scala
import org.apache.spark.sql.types._
val schema = (new StructType).
add("id", "int", false).
add("name", "string", false).
add("height", "double", false)
```

- // 多种方式定义schema，其核心是**StructType**
  import org.apache.spark.sql.types._

  源码中有很多StructType的不同格式

  ```scala
  val schema6 = (new StructType).
  add("name", "string", false).
  add("age", "integer", false).
  add("height", "integer", false)
  ```

- 创建Dataset

  

#### 创建

1. 创建一个样例类

   ```scala
   case class Person(name: String, age: Long)
   ```

2. 创建DataSet

   ```scala
   val caseClassDS = Seq(Person("Andy", 32)).toDS()
   或者
   val ds3 = spark.createDataset(List(Person("ljw" , 22),Person("lyy" , 22)))
   或者
   val ds = spark.range(5 , 100 , 5)
   ```

#### 读取文件

```scala
val iris: DataFrame = spark
  .read
  .option("header", true)//文件有没有头
  .option("inferschema" , true)//是否自动类型推断
  .option("delimiter" , ",")//指定分隔符
  .csv("E:\\project\\bigdata\\scalamv\\data\\iris.csv")
```



#### 集合转ds

```scala
val seq1 = (("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
val df1 = spark.createDataFrame(seq1).withColumnRenamed("_1", "name1").withColumnRenamed("_2", "age1").withColumnRenamed("_3", "height1")
df1.orderBy(desc("age1")).show(10)
或者
val df2 = spark.createDataFrame(seq1).toDF("name", "age", "height")
```

#### RDD转换为DataSet

- SparkSQL能够自动将包含有case类的RDD转换成DataFrame，case类定义了table的结构，case类属性通过反射变成了表的列名。

1. 创建一个RDD

   ```scala
   val peopleRDD = sc.textFile("/text")
   ```

2. 创建一个样例类

   ```scala
   case class Person(name: String, age: Long)
   ```

3. 将RDD转化为DataSet

   ```scala
   peopleRDD.map(line => {val para = line.split(",");
   Person(para(0),para(1).trim.toInt)}).toDS()
   ```

#### DataSet转换为RDD

- 调用rdd方法即可。

  ```scala
  DS.rdd
  ```

#### DataFrame与DataSet的互操作

- DataFrame转换为DataSet

  1. 创建一个DateFrame

     ```scala
     val df = spark.read.json("/test.json")
     ```

  2. 创建一个样例类

     ```scala
     case class Person(name: String, age: Long)
     ```

  3. 将DateFrame转化为DataSet

     ```
     df.as[Person]
     ```

- . DataSet转换为DataFrame

  1. 创建一个样例类

     ```scala
     case class Person(name: String, age: Long)
     ```

  2. 创建DataSet

     ```scala
     val ds = Seq(Person("Andy", 32)).toDS()
     ```

  3. 将DataSet转化为DataFrame

     ```scala
     val df = ds.toDF
     ```

#### DataSet转DataFrame

1. 导入隐式转换

   ```scala
   import spark.implicits._
   ```

2. 转化

   ```scala
   val testDF = testDS.toDF
   ```

#### DataFrame转DataSet

1. 导入隐式转换

   ```scala
   import spark.implicits._
   ```

2. 创建样例类

   ```scala
   case class Coltest(col1:String,col2:Int)extends Serializable //定义字段名和类型
   ```

3. 转换

   ```scala
   val testDS = testDF.as[Coltest]
   ```

- 这种方法就是在给出每一列的类型后，使用as方法，转成Dataset，这在数据类型是DataFrame又需要针对各个字段处理时极为方便。在使用一些特殊的操作时，一定要加上 import spark.implicits._ 不然toDF、toDS无法使用。

### 三者的共性

1、RDD、DataFrame、Dataset全都是spark平台下的分布式弹性数据集，为处理超大型数据提供便利

2、三者都有惰性机制，在进行创建、转换，如map方法时，不会立即执行，只有在遇到Action如foreach时，三者才会开始遍历运算。

3、三者都会根据spark的内存情况自动缓存运算，这样即使数据量很大，也不用担心会内存溢出。

4、三者都有partition的概念

5、三者有许多共同的函数，如filter，排序等

6、在对DataFrame和Dataset进行操作许多操作都需要这个包进行支持

​	import spark.implicits._

7、DataFrame和Dataset均可使用模式匹配获取各个字段的值和类型



### 三者的区别

1. RDD:

   1）RDD一般和spark mlib同时使用

   2）RDD不支持sparksql操作

2. DataFrame:

   1）与RDD和Dataset不同，DataFrame每一行的类型固定为Row，每一列的值没法直接访问，只有通过解析才能获取各个字段的值，如：

   2）DataFrame与Dataset一般不与spark mlib同时使用

   3）DataFrame与Dataset均支持sparksql的操作，比如select，groupby之类，还能注册临时表/视窗，

   4）DataFrame与Dataset支持一些特别方便的保存方式，比如保存成csv，可以带上表头，这样每一列的字段名一目了然

3. Dataset:

   1）Dataset和DataFrame拥有完全相同的成员函数，区别只是每一行的数据类型不同。

   2）DataFrame也可以叫Dataset[Row],每一行的类型是Row，不解析，每一行究竟有哪些字段，各个字段又是什么类型都无从得知，只能用上面提到的getAS方法或者共性中的第七条提到的模式匹配拿出特定字段。而Dataset中，每一行是什么类型是不一定的，在自定义了case class之后可以很自由的获得每一行的信息

   可以看出，Dataset在需要访问列中的某个字段时是非常方便的，然而，如果要写一些适配性很强的函数时，如果使用Dataset，行的类型又不确定，可能是各种case class，无法实现适配，这时候用DataFrame即Dataset[Row]就能比较好的解决问题







### idea

- 创建
- 创建sparkSession内部提供builder方法然后设置参数，对象是私有的不能直接new

```scala
val conf: SparkConf = new SparkConf().setAppName("sql one").setMaster("local")
//创建session对象
val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//读取json文件
val frame: DataFrame = spark.read.json("E:\\project\\bigdata\\scalamv\\data\\data\\json")
//采用sql的语法访问数据
frame.createOrReplaceTempView("user")
spark.sql("select name , age from user where age >= 20 order by age desc").show()
//    展示数据
//    frame.show()
//释放资源
spark.stop()
```

- RDD&DataFrame&DataSet之间相互转换
- 将RDD相互转化必须引一个隐式转换函数

```scala
def main(args: Array[String]): Unit = {
  val conf: SparkConf = new SparkConf().setAppName("sql one").setMaster("local")
  //创建session对象
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  //进行转换之前，需要引入隐式转换规则
  //这里的spark不是包名的含义，是sparkSession对象的名字
  import spark.implicits._
  //创建RDD
  val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("ljw" , 22) , ("lyy" , 22) , ("spark" , 25)))
  //转化为DF
  val dataFrame: DataFrame = rdd.toDF("name" , "age")
  //转换为DS（df转ds）
  val dataSet: Dataset[dsOpthin] = dataFrame.as[dsOpthin]
  //转换为DF
  val dataFrame2: DataFrame = dataSet.toDF()
  //转换为RDD
  val rdd2: RDD[Row] = dataFrame2.rdd
  rdd2.foreach(line => {
    println(line.getString(0))
  })
  //RDD - DataSet（RDD直接转DataSet）
  val dsRDD: RDD[dsOpthin] = rdd.map {
    case (name, age) => {
      dsOpthin(name, age)
    }
  }
val dsds: Dataset[dsOpthin] = dsRDD.toDS()
  //释放资源
  spark.stop()
}
case class dsOpthin(name:String , age : Int)
```





## 算子DSL



### Transformation

- **describe**(类似summary)

- **map**

  内部是row，直接row(0)取值，是any类型，必须给具体类型才行

```scala
case class Peoples(age:Int, names:String)
val seq1 = Seq(Peoples(30, "Tom, Andy, Jack"), Peoples(20, "Pand, Gate, Sundy"))
val ds1 = spark.createDataset(seq1)
val ds2 = ds1.map(x => (x.age+1, x.names))
ds2.show

val ds3 = ds1.flatMap{ x =>
val a = x.age
val s = x.names.split(",").map(name => (a, name.trim))
s
}
ds3.show
```



#### select&selectExpr

- **select**→跟列名

  ```sql
  df.select("empno").show
  df.select($"empno"+1).show  查出结果+1
  df.select(col("empno")).show
  df.select(df("empno")).show
  ```

- **selectExpr**→跟字符串

  ```sql
  df.selectExpr("empno" , "empno+1000").show  查出结果+1000
  df.selectExpr("empno as name").show   更名
  df.selectExpr("power(sal , 2)" , "sal").show  平方
  df.selectExpr("round(sal , 2) as newsal" , "sal").show   取整
  ```

- **drop**→删除列

  ```sql
  df.drop("mgr").show
  ```

- **withColumnRenamed**

  ```scala
  df1.withColumnRenamed("sal", "newsal")
  ```

- **withColumn**

  可以增加一列

  ```sql
  val df2 = df1.withColumn("sal", $"sal"+1000)
  ```

- **cast**

  类型转换

  ```sql
  df.selectExpr("cast(empno as name")).printSchema
  ```



#### where

- **while**

  ```scala
  df1.where("sal>1000 and job=='MANAGER'").show
  
  where内部可以写成全部的字符串，hive的形式
  ```

- **filter**

  ```scala
  df1.filter("sal>1000 and job=='MANAGER'").show
  ```


- ```scala
  //必须使用三等于才可以
  data.filter(length($"userid")===1).show
  ```



#### lit

- 用于在函数中，例如需要一个int的值，不能直接放，就用lit(5)这样的形式进行转义



#### groupBy

- **groupBy**

  ```sql
  df.groupBy("deptno").sum("sal").as("newsal").show
  df.groupBy("deptno").agg(sum("sal").as("newsal"),count("empno")).show
  ```

- **agg**

  ```SCALA
  sqlData.groupBy("DEPTNO")
        .agg(sum("SAL").as("sal") ,count("ENAME"))
        .show()
  ```

  

#### orderBy

- **orderBy**

- **sort**

  ```sql
  df.orderBy($"empno".desc).show
  df1.orderBy("sal").show
  df1.orderBy($"sal").show
  df1.orderBy($"sal".asc).show
  df1.orderBy('sal).show
  df1.orderBy(col("sal")).show
  df1.orderBy(df1("sal")).show
  df1.orderBy($"sal".desc).show
  df1.orderBy(-'sal).show
  df1.orderBy(-'deptno, -'sal).show
  ```



#### join

- **crossJoin**

- **join**

  ```sql
  df.join(df1,"demp")
  df.join(df,"df1",Seq("xx1","xx2"))
  df.join(df1,"id","left")
  df.join(df1,"id","left_outer")
  df.join(df1,"id","right")
  df.join(df1,"id","outer")       外连接
  df.join(df1,"id","full")        全连接
  df.join(df1,"id","left_semi")   类似集合求交(df的id   in  df2的id)
  df.join(df1,"id","left_anti")   类似集合求差(df的id   not in  df2的id)
  
  data_195a.join(data_195b , Seq("user_id") , "left")
  ```



#### 集合

- 并集
  1. union
  2. unionAll

- 交集
  1. intersect
- 差
  1. except

```scala
val ds1: Dataset[Row] = sqlData.orderBy($"SAL".desc).where("SAL>2000")
val ds2: Dataset[Row] = sqlData.orderBy($"SAL".desc).where("SAL<2000")
ds1.union(ds2).show()
ds1.intersect(ds2).show()
ds1.except(ds2).show()
```



#### 空值处理

- **.isNaN()**

  ```sql
  math.sqrt(-1.0);
  math.sqrt(-1.0).isNaN()
  ```

- **.na**→判断有没有空值

  ```sql
  df1.na.drop.show  
  df1.na.drop(Array("mgr")).show
  ```

- **.na.fill(1000)**

  ```sql
  df1.na.fill(1000).show
  df1.na.fill(1000, Array("comm")).show
  df1.na.fill(Map("mgr"->2000, "comm"->1000)).show
  ```

- **.na.replace**→替换指定值

  ```sql
  df1.na.replace("comm" :: "deptno" :: Nil, Map(0 -> 100, 10 -> 100)).show
  ```

- **.filter**

  ```sql
  df1.filter("comm is null").show
  sqlData.filter("COMM is not null").show()
  df1.filter($"comm".isNull).show
  df1.filter(col("comm").isNull).show
  ```



#### 时间日期函数

```sql
// 各种时间函数
df1.select(year($"hiredate")).show
df1.select(weekofyear($"hiredate")).show
df1.select(minute($"hiredate")).show
df1.select(date_add($"hiredate", 1), $"hiredate").show
df1.select(current_date).show
df1.select(unix_timestamp).show
val df2 = df1.select(unix_timestamp as "unixtime")
df2.select(from_unixtime($"unixtime")).show
// 计算年龄
df1.select(round(months_between(current_date, $"hiredate")/12)).show
```

#### json

- get_json_object

  ```sql
  get_json_object($"exts", "$.idcard"),
  ```

- 读

  ```sql
  val df2 = spark.read.json("data/employees.json")
  ```

- 写

  ```scala
  df1.select("age", "name").write.
  option("header", true).
  format("csv").save("data/t2")
  ```

  

#### Window

```scala
import org.apache.spark.sql.expressions.Window
val w1 = Window.partitionBy("cookieid").orderBy("createtime")
val w2 = Window.partitionBy("cookieid").orderBy("pv")
val w3 = w1.rowsBetween(Window.unboundedPreceding, Window.currentRow)
val w4 = w1.rowsBetween(-1, 1)
// 聚组函数【用分析函数的数据集】
df.select($"cookieid", $"pv", sum("pv").over(w1).alias("pv1")).show
df.select($"cookieid", $"pv", sum("pv").over(w3).alias("pv1")).show
df.select($"cookieid", $"pv", sum("pv").over(w4).as("pv1")).show
// 排名
df.select($"cookieid", $"pv", rank().over(w2).alias("rank")).show
df.select($"cookieid", $"pv", dense_rank().over(w2).alias("denserank")).show
df.select($"cookieid", $"pv", row_number().over(w2).alias("rownumber")).show
// lag、lead
df.select($"cookieid", $"pv", lag("pv", 2).over(w2).alias("rownumber")).show
df.select($"cookieid", $"pv", lag("pv", -2).over(w2).alias("rownumber")).show
// cube、rollup【备注：使用cube的数据集】
df.cube("id", "name").agg(sum("num")).show
```

- 分区

  partitionBy

- 排序

  orderBy

- 窗口范围

  rowsBetween

  currentRow

  unboundedPreceding

  unboundedFollowing

- 排名

  rank().over(w2)
  dense_rank().over(w2)
  row_number().over(w2)

- 前后

  lag、lead

- cube

  // cube、rollup【备注：使用cube的数据集】
  df.cube("id", "name").agg(sum("num")).show



#### 字符串相关&正则

- 模糊匹配     LIKE

  ```scala
  df.filter("like(name, 'M%')").show
  ```

- split(str, reg)
- regexp_replace

- 正则匹配提取    regexp_extract

  ```sql
  data.select(
    regexp_extract($"str" , "(\\S*) ([^0-9]*) .+" , 1).as("s"),
    regexp_extract($"str" , "(\\S*) ([^0-9]*) .+" , 2).as("d")
  ).show()
  ```

  ```sql
  8.35.201.160 - - [16/May/2018:17:38:21 +0800] "GET
  /uc_server/data/avatar/000/01/54/22_avatar_middle.jpg HTTP/1.1" 200 5396
  对数据提取ip和2018-5-16
  
  data2.select(
    regexp_extract($"value" , "(\\S*) - - \\[(.*)\\] .* " , 1).as("a"),
    from_unixtime(unix_timestamp(regexp_extract($"value" , "(\\S*) - - \\[(.*)\\] .* " , 2),"dd/MMM/yyyy:HH:mm:ss"),"yyyy-MM-dd").as("a")
  ).show
  ```

  

#### array&map

- 炸开explode

  ```scala
  data16_1.select(explode(split($"Tags" , ",")).as("Id"))
  ```

- 聚合

  在agg中进行collect_ws连接和转lsit或者set

  ```sql
  .agg(concat_ws(",", collect_list("lab")) as "fla")
  ```





#### 条件

- when(列,值).when.....otherwise()

```scala
when($"subject".equalTo("语文"), $"score").as("语文"),
```



#### DF、DS的SQL语句

```scala
// 有两种形式：createOrReplaceTempView / createTempView
df1.createOrReplaceTempView("temp1")
spark.sql("select * from temp1").show
df1.createTempView("temp2")
spark.sql("select * from temp2").show
// 使用下面的语句可以看见注册的临时表
spark.catalog.listTables.show
```











### Action

- show
  1. 默认缺省20行
  2. 默认截断长字符（true）

- printSchema

- explain         执行计划
- columns、
- dtypes、
- col
- collectAsList、
- head()、
- first、
- count、
- take()、
- takeAsList、
- reduce

### 存储相关

- checkpoint

- cache

- unpersist在程序结束够释放掉cache

- sql的表cache

  spark.catalog.cacheTable("t1")

- sql的表解除cache

  spark.catalog.uncacheTable("t1")



## spark与hive集成

- Spark的配置：
  1. 将jdbc的驱动程序软链接到$SPARK_HOME/jars目录下（使用hive和sparksql节点的客户端）
  2. 将hive-site.xml程序软链接到$SPARK_HOME/conf 目录下（至少安装hive的节点或程序运行的节点，建议所有节点）

### idea集成

```scala
//实例化并且读数据
val spark = SparkSession
  . builder ()
  .appName( "CreateDataset")
  .master( "local[*]")
  .enableHiveSupport()
  .getOrCreate()
spark.sparkContext.setLogLevel( "error")
spark.sql( "show databases").show
spark.table( "dm_adver.dm_model").show
spark.stop()

保存
df.write.option("header" , true).mode(SaveMode.Append).格式.saveAsTable("mydb.result_table")
//分区表的存储
write.partitionBy("字段名")
//保存方式
insertInto
saveAsTable
//存储形式
.mode("append")//追加，如果用"overwrite"那么系统默认删除了，就不存在存储格式问题
//存储格式默认parquet,如果存储为hive默认的textfile的话
.format("Hive")
//如果出现严格模式问题，可以将配置为非严格模式的配置加入到hive.xml中

```

- hive配置文件的写法

  





## 自定义函数

### UDF

- UDF(User Defined Function)，自定义函数。函数的输入、输出都是一条数据记录，类似于Spark SQL中普通的数学或字符串函数，从实现上看就是普通的Scala函数

- 函数要用于下面的select或者where中，必须是udf定义

- 用Scala编写的UDF与普通的Scala函数几乎没有任何区别，唯一需要多执行的一个步骤是要在SQLContext注册它。

  ```scala
  def len(bookTitle: String):Int = bookTitle.length
  spark.udf.register("len", len _)
  val booksWithLongTitle = spark.sql("select title, author from books where len(title) > 10")
  ```

- 编写的UDF可以放到SQL语句的fields部分，也可以作为where、groupBy或者having子句的一部分

- 常用于spark sql形式的

  ```scala
  def mySub2(in:String) = in.substring(0,2)//创建函数，没有用udf定义
  spark.udf.register("mySub2" , mySub2 _)//进行注册
  sqlData.createOrReplaceTempView("t1")//建立临时表用于sql类型查询
  spark.sql("select mySub2(ENAME) , SAL from t1").show//查询
  ```

- 用于spark sql的DSL形式

  ```scala
  import org.apache.spark.sql.functions._    //导包
  def mySub = udf((in : String) => in.substring(0,2))//定义udf函数
  sqlData.select(mySub($"JOB") , $"SAL").show()//DSL查询操作
  ```

  





### UDAF

- UDAF（User Defined Aggregation Funcation），用户自定义聚合函数。函数本身作用于数据集合，能够在聚合操作的基础上进行自定义操作（多条数据输入，一条数据输出）；类似于在group by之后使用的sum、avg等函数

#### 普通的udaf

1. 输入数据类型
2. buffer数据类型
3. 输出数据类型
4. 函数是否返回确定的结果
5. 数据的初始化
6. 分区内合并
7. 分区间合并
8. 最终计算

- 实例

```scala
数据如下：
id, name, sales, discount, state, saleDate
1, "Widget Co", 1000.00,  0.00, "AZ", "2014-01-01"
2, "Acme Widgets", 2000.00, 500.00, "CA", "2014-02-01"
3, "Widgetry", 1000.00, 200.00, "CA", "2015-01-11"
4, "Widgets R Us", 2000.00, 0.0, "CA", "2015-02-19"
5, "Ye Olde Widgete", 3000.00, 0.0, "MA", "2015-02-28"
最后要得到的结果为：
(2015年的合计值 – 2014年的合计值) / 2014年的合计值
(6000 - 3000) / 3000 = 1
执行以下SQL得到最终的结果：
select userFunc(sales, saleDate) from table1;
即计算逻辑在userFunc中实现
```

```scala
package com.bigdata.sparksql.udf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructType}

object MyUDAF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("word")
      .master("local")
      .getOrCreate()
    spark

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window

    val udafData: DataFrame = spark.read
      .option("header", true)
      .option("inferschema", true)
      .option("delimiter", ",")
      .csv("E:\\project\\bigdata\\scalamv\\data\\sparksql\\udaftestText")
    udafData

    udafData.show()

    udafData.printSchema()

    udafData.createOrReplaceTempView("t1")

    val udafFun = new MyUDAF
    spark.udf.register("udafF", udafFun)
    spark.sql("select  udafF(sales,saleDate) from t1").show()

    spark.stop()
  }

}


class MyUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = new StructType()
    .add("sales", DoubleType)
    .add("saleDate", StringType)

  override def bufferSchema: StructType = new StructType()
    .add("sales2014", DoubleType)
    .add("sales2015", DoubleType)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
  }

  //分区内数据进行合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val sales: Double = input.getDouble(0)
    val saleDate: String = input.getString(1).take(4)
    saleDate match {
      case "2014" => buffer(0) = buffer.getDouble(0) + sales
      case "2015" => buffer(1) = buffer.getDouble(1) + sales
      case _ => buffer(1) = buffer.getDouble(1) + 0.0
    }
  }

  //分区间进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer1.getDouble(0)
    buffer1(1) = buffer1.getDouble(1) + buffer1.getDouble(1)
  }

  override def evaluate(buffer: Row): Any = {
    val buffer2014: Double = buffer.getDouble(0)
    val buffer2015: Double = buffer.getDouble(1)
    (buffer2015 - buffer2014) / buffer2014
  }
}
```

#### 类型安全的UDAF

1. 初值
2. 分区内
3. 分区间
4. 输出逻辑
5. 编码转换
6. 编码转换

```scala
case class SalesInfo(sales: Double, saleDate: String)
case class SalesBuffer(sales2014: Double, sales2015: Double)

class UDAFDemo9 extends Aggregator[SalesInfo, SalesBuffer, Double]{
  override def zero: SalesBuffer = SalesBuffer(0.0, 0.0)

  override def reduce(buffer: SalesBuffer, row: SalesInfo): SalesBuffer = {
    val sales = row.sales
    val year = row.saleDate.take(4)
    year match {
      case "2014" => SalesBuffer(buffer.sales2014 + sales, buffer.sales2015)
      case "2015" => SalesBuffer(buffer.sales2014, buffer.sales2015 + sales)
      case _ => buffer
    }
  }

  override def merge(b1: SalesBuffer, b2: SalesBuffer): SalesBuffer =
    SalesBuffer(b1.sales2014 + b2.sales2014, b1.sales2015 + b2.sales2015)

  override def finish(reduction: SalesBuffer): Double = {
    assert(reduction.sales2014 != 0.0)
    (reduction.sales2015 - reduction.sales2014) / reduction.sales2014
  }

  override def bufferEncoder: Encoder[SalesBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object MyTest01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDFDemo")
      .master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val lst = List(
      (1, "Widget Co", 1000.00, 0.00, "AZ", "2014-01-01"),
      (2, "Acme Widgets", 2000.00, 500.00, "CA", "2014-02-01"),
      (3, "Widgetry", 1000.00, 200.00, "CA", "2015-01-11"),
      (4, "Widgets R Us", 2000.00, 0.0, "CA", "2015-02-19"),
      (5, "Ye Olde Widgete", 3000.00, 0.0, "MA", "2015-02-28")
    )

    import spark.implicits._
    val salesDS: Dataset[SalesInfo] = spark.createDataFrame(lst)
      .toDF("id", "name", "sales", "discount", "state", "saleDate")
      .select("sales", "saleDate").as[SalesInfo]

    val customerSummer = new UDAFDemo9().toColumn.name("udaf")
    salesDS.select(customerSummer).show

    spark.stop()
  }
}
```



## 其他源连接

### mysql

- 从mysql读取数据

```scala
val spark: SparkSession = SparkSession.builder()
  .appName("word")
  .master("local")
  .getOrCreate()
spark
spark.sparkContext.setLogLevel("WARN")

import spark.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


val jdbcDF = spark.read.format("jdbc").
  option("url", "jdbc:mysql://slave4:3306/spark").
  option("driver","com.mysql.jdbc.Driver").
  option("dbtable", "student").
  option("user", "root").
  option("password", "wang1997").load()
jdbcDF.show
jdbcDF.printSchema

spark.stop()
```

备注：

1. 将jdbc驱动拷贝到$SPARK_HOME/conf目录下，是最简单的做法；
2. 明白每一个参数的意思，一个参数不对整个结果出不来；
3. 从数据库从读大量的数据进行分析，不推荐；读取少量的数据是可以接受的，也是常见
   的做法



### parquet

- spark sql 读文件默认的数据源就是parquet

- 列式存储，压缩率高，

```scala
val usersDF = spark.read.load("data/users.parquet")
usersDF.show
usersDF.select("name", "favorite_color").write.
save("data/namesAndFavColors.parquet")
```

- Parquet的特性（重要）:

  Parquet是列式存储格式的一种文件类型，列式存储有以下的核心：

  A.可以跳过不符合条件的数据，只读取需要的数据，降低IO数据量

  B.压缩编码可以降低磁盘存储空间。由于同一列的数据类型是一样的，可以使用更高效的压缩编码进一步节约存储空间

  C.只读取需要的列，支持向量运算，能够获取更好的扫描性能

- df直接存为parquet

  ```scala
  data.write.parquet("data/test.parquet")
  ```

- 直接读取parquet文件

  ```scala
  spark.read.parquet("data/test.parquet").show()
  ```

  

### json

- 不能分行







## 执行流程

### 流程

- 选投连

- 语句  →  解析 →绑定→执行        → 优化

  query→parse→bind→optimize→execute

  1. sql的查询引擎首先对查询语句进行解析，也就是Parse过程，解析的过程是将查询语句进行分割，把project，DataSource和Filter三个部分解析出来从而形成一个逻辑解析tree。在解析的过程中还会检查sql语法是否有错误，比如缺少指标字段、数据库中不包含这张数据表等。当发现有错误时立即停止解析，并报错。当顺利完成解析时，会进入到Bind过程。
  2. 绑定（Bind）。bind过程其实就是把Parse过程后形成的逻辑解析tree，与数据库的数据字典绑定的过程。绑定后会形成一个执行tree，从而让程序知道表在哪里，需要什么字段等等
  3. 优化（Optimize）。完成了Bind过程后，数据库查询引擎会提供几个查询执行计划，并且给出了查询执行计划的一些统计信息，既然提供了几个执行计划，那么有比较就有优劣，数据库会根据这些执行计划的统计信息选择一个最优的执行计划。
  4. 执行（Execute）。最后执行的过程和我们解析的过程是不一样的，当我们知道执行的顺序，对我们以后写sql以及优化都是有很大的帮助的.执行查询后，他是先执行where部分，然后找到数据源之数据表，最后生成select的部分，我们的最终结果。

![搜狗截图20191108124946](D:\software\typoraIMG\搜狗截图20191108124946.png)

-  内存列式缓存：内存列式（in-memory columnar format）缓存（再次执行时无需重复读取），仅扫描需要的列，并自动调整压缩比使内存使用率和GC 压力最小化；
- 动态代码和字节码生成技术：提升重复表达式求值查询的速率；
-  Tungsten 优化：
  1. 由 Spark 自己管理内存而不是 JVM，避免了 JVM GC 带来的性能损失；
  2. 内存中 Java 对象被存储成 Spark 自己的二进制格式，直接在二进制格式上计算，省去序列化和反序列化时间；此格式更紧凑，节省内存空间







### 执行计划

```
// 查询 Unresolved LogicalPlan
query.queryExecution.logical
// 查询 analyzed LogicalPlan
query.queryExecution.analyzed
// 查询 优化后的LogicalPlan
query.queryExecution.optimizedPlan
// 查询 物理执行计划
query.queryExecution.sparkPlan
```

- 一个复杂的逻辑语法树

  ```scala
  val selectExecution: QueryExecution = data_195a.select($"user_id")
    .join(data_195b.select($"user_id", $"read_num"), "user_id")
    .join(data_195c, Seq("user_id"), "left")
    .filter($"price".isNotNull)
    .groupBy("ds")
    .agg(
      sum("price").as("sum_price"),
      concat_ws(",", collect_list("read_num")) as "read_num"
    )
    .queryExecution
  selectExecution
  println(selectExecution)
  ```

  

## join形式

- **Broadcast  Hash Join** ：适合一张很小的表和一张大表进行 Join ；

  10M > 小表的尺寸 => 广播变量

  ```scala
  被广播的表需要小于 spark.sql.autoBroadcastJoinThreshold 所配置的信息，默认是10M；
  ```

- **Shuffle  Hash Join** ：适合一张小表( ( 比上一个大一点) ) 和一张大表进行Join ；

  大表（读数据时做了分区） => shuffer =>  分成n个区（仍然放不下）

  小表1G => shuffer => 分成了n个区 （内存中可以放下）

  对于任何一个分区而言：key是一致的

- **Sort  Merge Join** ：适合两张大表进行 Join

  大表1 =>  shuffer =>  分成了n个区（仍然放不下）

  大表1 =>  shuffer =>  分成了n个区（仍然放不下）

  对于任何一个分区而言，在同一个Executor中，并且key是一致的

  然后两种表分别排序，然后按照两个桶一样分别按照指针向下匹配join

- 前两者都是基于hash join的，只不过hash join之前需要选shuffer还是先broadcast







# 题目

## 列转行

```scala
+---+-----------------+
|age|            names|
+---+-----------------+
| 30|  Tom, Andy, Jack|
| 20|Pand, Gate, Sundy|
+---+-----------------+
转
+---+-----+
| _1|   _2|
+---+-----+
| 30|  Tom|
| 30| Andy|
| 30| Jack|
| 20| Pand|
| 20| Gate|
| 20|Sundy|
+---+-----+
将单元格中的数据压平切割然后遍历出来，再改名

val seq1 = Seq(Peoples(30, "Tom, Andy, Jack"), Peoples(20, "Pand, Gate, Sundy"))
spark.createDataset(seq1).flatMap { x =>
  x.names.split(",").map(name => (x.age, name.trim))
}.withColumnRenamed("_1" , "age")
  .withColumnRenamed("_2" , "name").show
```

## TopN

```scala
val win1: WindowSpec = Window.partitionBy("DEPTNO").orderBy(desc("SAL"))
sqlData
  .select($"*",rank().over(win1).as("rank"))
  .where("rank<=3")
  .show()
```



## 单列炸开的wordcount

```scala
+---+--------------------+
|uid|            contents|
+---+--------------------+
|  1|        i|love|china|
|  2|china|is|good|i|i...|
+---+--------------------+
到
+---+-----+-----+
|uid| word|count|
+---+-----+-----+
|  1|    i|    1|
|  2|   is|    1|
|  2|china|    1|
|  1| love|    1|
|  2|    i|    2|
|  1|china|    1|
|  2| good|    1|
|  2| like|    1|
+---+-----+-----+

import spark.implicits._
val words: DataFrame = spark.read
  .option("header", true)
  .option("inferschema", true)
  .option("delimiter", " ")
  .csv("E:\\project\\bigdata\\scalamv\\data\\sparksql\\wordsCount")
words
val lines: Dataset[wordsO] = words.as[wordsO]
//将文件压平对需要的列进行切分，然后遍历输出，做分组，count输出
lines.flatMap{x=> {
  val line: mutable.ArrayOps[String] = x.contents.split("\\|")
  line.map(y => (x.uid , y))
}}
  .select($"_1"as("uid") , $"_2".as("word"))
  .groupBy("uid","word")
  .count()
  .show()

//样例类用于将df转ds
case class wordsO(uid : Int , contents :String) extends Serializable
```

## 每个用户的连续登陆的最大天数

- 时间如何减字段

```scala
uid,date
1,2019-08-01
1,2019-08-02
1,2019-08-03
2,2019-08-01
2,2019-08-02
3,2019-08-01
3,2019-08-03
4,2019-07-28
4,2019-07-29
4,2019-08-01
4,2019-08-02
4,2019-08-03



val regist: DataFrame = spark
  .read
  .option("inferschema", true)
  .csv("E:\\project\\bigdata\\scalamv\\data\\sparksql\\maxregist")
regist
val time_rank: DataFrame = regist.withColumnRenamed("_c1", "timee")
  .withColumnRenamed("_c0", "id")
  .select(
    $"id",
    $"timee",
    row_number().over(Window.partitionBy("id").orderBy("timee")).as("rank")
  ).cache()
time_rank
time_rank.printSchema()
time_rank.select(
  $"id",
  $"timee",
  date_sub($"timee" , $"rank")
)
```

## 经典表格变形

![行列互换](E:\BigDataQf\自建笔记\hadoop\hive\img\行列互换.png)

```mysql
id uid subject score
1 001 语文 90
2 001 数学 91
3 001 英语 80
4 002 语文 88
5 002 数学 90
6 002 英语 75.5
7 003 语文 70
8 003 数学 85
9 003 英语 90
10 003 政治 82



1       90.0    91.0    80.0    0.0
2       88.0    90.0    75.5    0.0
3       70.0    85.0    90.0    82.0


val score_t1: DataFrame = data14.select($"userid",
  when($"subject".equalTo("语文"), $"score").as("语文"),
  when($"subject".equalTo("数学"), $"score").as("数学"),
  when($"subject".equalTo("英语"), $"score").as("英语"),
  when($"subject".equalTo("政治"), $"score").as("政治")
)
  .groupBy("userid")
  .agg(
    sum("语文").as("语文"),
    sum("数学").as("数学"),
    sum("英语").as("英语"),
    sum("政治").as("政治")
  )
  .na.fill(0)
  .cache()
score_t1

score_t1.show()

val score_t2: DataFrame = score_t1.select(
  $"*",
  ($"语文" + $"数学" + $"英语" + $"政治").as("total")
)
score_t2
score_t2.show()

  score_t2
  .union(
    score_t2.select(
      sum($"语文"),
      sum($"语文"),
      sum($"数学"),
      sum($"英语"),
      sum($"政治"),
      sum($"total")
    )
  )
  .show()
```



## access_log日志

```sql
(1)、每天整体的访问UV、PV?

log2.select(//计算pv
  regexp_extract($"str", "(\\S*) - - \\[(.*)\\] .+", 1).as("ip"),
  from_unixtime(unix_timestamp(regexp_extract($"str", "(\\S*) - - \\[(.*)\\] .+", 2),"dd/MMM/yyyy:HH:mm:ss"),"yyyy-MM-dd").as("day")
)
  .filter($"day".isNotNull)
  .groupBy("day")
  .agg(
    count("*").as("day_pv")
  )
  .join(
    log2.select(//计算uv
      regexp_extract($"str", "(\\S*) - - \\[(.*)\\] .+", 1).as("ip"),
      from_unixtime(unix_timestamp(regexp_extract($"str", "(\\S*) - - \\[(.*)\\] .+", 2),"dd/MMM/yyyy:HH:mm:ss"),"yyyy-MM-dd").as("day")
    )
      .dropDuplicates(Seq("ip"))
      .filter($"day".isNotNull)
      .groupBy("day")
      .agg(
        count("*").as("day_uv")
      )
    ,"day"
  )
  .show()
```

## 每个用户连续登陆的最大天数？

```scala
数据:
login表
uid,date
1,2019-08-01
1,2019-08-02
1,2019-08-03
2,2019-08-01
2,2019-08-02
3,2019-08-01
3,2019-08-03
4,2019-07-28
4,2019-07-29
4,2019-08-01
4,2019-08-02
4,2019-08-03
结果如下：
uid cnt_days
1 3
2 2
3 1
4 3
```



```sql
//1.根据每个人的登陆时间进行降序rank
    val time_rank: DataFrame = regist.withColumnRenamed("_c1", "timee")
      .withColumnRenamed("_c0", "id")
      .select(
        $"id",
        $"timee",
        row_number().over(Window.partitionBy("id").orderBy("timee")).as("rank")
      ).cache()
    time_rank
    //2.用时间减去得到的rank，相同的新时间就是连续登陆,接着对连续天数进行开窗rank取出最大连续登陆天数
    time_rank.selectExpr(
      "id",
      "timee",
      "date_sub(timee,rank) new_date"
    )
      .groupBy("new_date", "id")
      .agg(
        count("*").as("count_Num")
      )
      .selectExpr(
        "id",
        "count_Num",
        "row_number() over(partition by id sort by count_Num desc) rank"
      )
      .where($"rank" === 1)
      .show()
```



## 用户标签连接查询

```scala
数据：
T1表:
Tags
1,2,3
1,2
2,3
T2表:
Id lab
1 A
2 B
3 C
根据T1和T2表的数据，编写sql实现如下结果：
ids tags
1,2,3 A,B,C
1,2 A,B
2,3 B,C


//先把tags的数据进行炸裂，然后两表join进行然后根据tags进行分组
 val tags: DataFrame = data16_1.select(
   $"Tags",
   explode(split($"Tags", ",")).as("Id")
 )

 data16_2.show()

 data16_2.join(tags, "Id")
   .groupBy("Tags")
   .agg(concat_ws(",", collect_list("lab")) as "fla")
   .show()
```

## corejoin&map端combine和sparksql三种形式测试

```
普通join开始时间   2019-11-08 21:25:58
普通join结束时间   2019-11-08 21:26:27
普通join间隔时间  29

mapjoin开始时间   2019-11-08 21:26:27
mapjoin开始时间   2019-11-08 21:26:29
mapjoin开间隔时间  2

sparksql join开始时间   2019-11-08 21:23:23
sparksql join结束时间   2019-11-08 21:23:23
sparksql join间隔时间  0

```

![三种join对比](D:\software\typoraIMG\三种join对比-1573219760715.jpg)

- 代码实现

##### 造数据

```scala
val conf: SparkConf = new SparkConf().setAppName("rdd run").setMaster("local")
val sc = new SparkContext(conf)
sc.setLogLevel("WARN")
  //访问数据
  val lst: ListBuffer[(String, Int, String, String)] = ListBuffer[(String , Int , String ,String)]()
  val sessionid= List(0 to 80000).flatten
  val httpdata=List(
    "76.6.24.56"
    ,"180.76.15.152"
    ,"36.149.198.213"
    ,"113.108.133.50"
    ,"140.205.201.38"
    ,"140.205.201.38"
    ,"140.205.201.32"
    ,"216.244.66.249"
    ,"180.76.15.27"
    ,"111,111,111,111"
  )
  for (i <- 1 to 3000000){
     lst.append((
       sessionid(math.round(Math.random()*79999).toInt).toString()
       ,i
       ,httpdata(math.round(Math.random()*9).toInt)
       ,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
     ))
  }
    lst.foreach(println)
  val rddLst: RDD[(String, Int, String, String)] = sc.makeRDD(lst)
  rddLst.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\data.txt")
  //user的数据
  val lstuser: ListBuffer[(Int, String, String)] = ListBuffer[(Int , String ,String)]()
  val sexrdd: List[String] = List("man","woman")
  for (i <- 100000 to 300000){
    lstuser.append((
      i
      ,"mapjoin"+i+"name"
      ,sexrdd(math.round(Math.random()).toInt)
    ))
  }
  val newlstuser: RDD[(Int, String, String)] = sc.makeRDD(lstuser)
  newlstuser.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\userdata.txt")
```

##### 普通的sparkcorejoin

```scala
//普通的join
val sessiondata: RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\data.txt")
val userdata: RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\userdata.txt")
val session1: RDD[(String, (String, String, String))] = sessiondata.mapPartitions(lines => {
  lines.map(line => {
    val arr: Array[String] = line.replaceAll("\\(|\\)", "").split(",")
    (arr(0), (arr(0), arr(2), arr(3)))
  })
})
session1
val session2: RDD[(String, (String, String))] = sessiondata.mapPartitions(lines => {
  lines.map(line => {
    val arr: Array[String] = line.replaceAll("\\(|\\)", "").split(",")
    (arr(1), (arr(0), arr(2)))
  })
})
session2
val time1: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
println("普通join开始时间"+"   "+time1)
session1.join(session2).foreach(x => x)
val time2: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
println("普通join结束时间"+"   "+time2)
println("普通join间隔时间" + "  " + ((tranTimeToLong(time2)-tranTimeToLong(time1))))
```

##### map端的combine

```scala
//将数据读成scala的普通文件非rdd
val userdata2: Array[String] = scala.io.Source.fromFile("C:\\Users\\Administrator\\Desktop\\userdata.txt\\part-00000").g
//mapjoin
val data1: RDD[(String, (String, String, String))] = sessiondata.mapPartitions(lines => {
  lines.map(line => {
    val arr: Array[String] = line.replaceAll("\\(|\\)", "").split(",")
    (arr(0), (arr(0), arr(2), arr(3)))
  })
})
data1
val mapdata: Map[String, (String, String)] = userdata2.map(lines => {
  val strings: Array[String] = lines.replaceAll("\\(|\\)", "").split(",")
  (strings(0), (strings(1), (strings(2))))
})
  .toMap
mapdata
val time3: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
println("mapjoin开始时间"+"   "+time3)
//将map小数据广播
val bromapdata: Broadcast[Map[String, (String, String)]] = sc.broadcast(mapdata)
//做map端join，也就是以data1大表为基础，然后map将broadbase值取出做连接
data1.map{case(userid , info) => {
  val mapresult: io.Serializable = bromapdata.value.getOrElse(userid , "")
  (userid,(mapresult , info))
}}
val time4: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
println("mapjoin开始时间"+"   "+time4)
println("mapjoin开间隔时间" + "  " + ((tranTimeToLong(time4)-tranTimeToLong(time3))))
```

sparksqlJoin

```scala
val sqlsmall: DataFrame = spark.read
  .option("inferschema", true)
  .option("delimiter", ",")
  .csv("C:\\Users\\Administrator\\Desktop\\userdata.txt")
sqlsmall
val sqlsmall2: DataFrame = sqlsmall.select(
  regexp_replace($"_c0", "\\(|\\)", "").as("userid")
  , $"_c1"
  , $"_c2"
)
sqlsmall2
val sqlsbig: DataFrame = spark.read
  .option("inferschema", true)
  .option("delimiter", ",")
  .csv("C:\\Users\\Administrator\\Desktop\\data.txt")
sqlsbig
val sqlsbig2: DataFrame = sqlsbig.select(
  $"_c0"
  , $"_c1".as("userid")
  , $"_c2"
  , $"_c3"
)
sqlsbig2
val time1: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
println("sparksql join开始时间"+"   "+time1)
//join操作
sqlsbig2.join(sqlsmall2 , "userid")
val time2: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
println("sparksql join结束时间"+"   "+time2)
println("sparksql join间隔时间" + "  " + ((tranTimeToLong(time2)-tranTimeToLong(time1))))
```



## 股票波峰波谷

```
假设有股票数据(csv文件)如下：股票代码(sid)、日期(time)、成交价（price）（表名t1）
sh600794,2019-09-02 09:00:10,25.51
sh603066,2019-09-02 09:00:10,15.51
sh600794,2019-09-02 09:00:20,25.72
sh603066,2019-09-02 09:00:20,15.72
sh600794,2019-09-02 09:00:30,25.83
sh603066,2019-09-02 09:00:30,15.83
sh600794,2019-09-02 09:00:40,25.94
sh603066,2019-09-02 09:00:40,15.94
sh600794,2019-09-02 09:00:50,26.00
sh603066,2019-09-02 09:00:50,16.00
sh600794,2019-09-02 09:10:00,25.98
sh603066,2019-09-02 09:10:00,25.98
... ...
sh600794,2019-09-02 15:00:00,25.50
sh603066,2019-09-02 15:00:00,16.00
用Spark-SQL实现：找每支股票每日<font color=red>所有的波峰、波谷值</font>（提供两种语法SQL + DSL 
```

- sql

  ```sql
  data1.createOrReplaceTempView("exam")
      spark.sql(
        """
          |select
          |*
          |from
          |(
          |select
          |id,
          |timee,
          |price,
          |lag_price,
          |lead_price,
          |case when price > lag_price and price > lead_price then price end max_price,
          |case when price < lag_price and price < lead_price then price end min_price
          |from
          |(
          |select
          |_c0 id ,
          |_c1 timee ,
          |_c2 price ,
          |lag(_c2 , 1) over(partition by _c0 order by _c1) lag_price,
          |lead(_c2 , 1) over(partition by _c0 order by _c1) lead_price
          |from exam
          |)t1
          |)t2
          |where max_price is not null or min_price is not null
        """
        .stripMargin).show
  ```

- DSL

```SCALA
data1.select(
      $"_c0".as("id"),
      $"_c1".as("timee"),
      $"_c2".as("price"),
      lag("_c2" , 1).over(Window.partitionBy("_c0").orderBy("_c1")).as("lag_price"),
      lead("_c2" , 1).over(Window.partitionBy("_c0").orderBy("_c1")).as("lead_price")
    ).select(
      $"id",
      $"timee",
      $"price",
      when($"price">$"lag_price" and $"price">$"lead_price" ,$"price" ).as("max_price"),
      when($"price"<$"lag_price" and $"price"<$"lead_price",$"price" ).as("min_price")
    )
      .filter($"max_price".isNotNull or $"min_price".isNotNull)
      .show()
```



## 京东白条

1 用户积分：关注1分    点赞2分   用户评价2分    购物5分
2 用户等级【普通(新用户)、银牌(用户积分[1000-3000)、金牌[3000-10000)、钻石(10000+)】

示例数据【ods_test.ods_user_action_log（数据在hive中）】如下：

用户行为日志【关注01    点赞02   用户评价03    购物05】

这里假设分区字段为【bdp_day】计算时间范围为本周内【20191125-20191130】

| 用户user             | 行为action | 目标对象action_target | 行为发生时间ct |
| -------------------- | ---------- | --------------------- | -------------- |
| A                    | 02         | 评价商品001           | 20190911261010 |
| A                    | 05         | 商品001               | 20190911271010 |
| A                    | 01         | 商品001               | 20190911251010 |
| C                    | 03         | 商品001               | 20190911281111 |
| C                    | 05         | 商品001               | 20190911291212 |
| B                    | 01         | 商品001               | 20190911291313 |
| E                    | 05         | 商品002               | 20190911301010 |
| ...(此处省略X行记录) |            |                       |                |



需求结果为【用户等级表】：dm_test.user_level（将数据写入hive）

| 用户user             | 积分uscore | 等级ulevel |
| -------------------- | ---------- | ---------- |
| A                    | 6900       | 金牌       |
| B                    | 1005       | 银牌       |
| C                    | 10         | 普通       |
| D                    | 0          | 普通       |
| E                    | 1000       | 银牌       |
| ...(此处省略X行记录) |            |            |

- **DSL做法**

  ```scala
  val result: DataFrame = data.filter(
        from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd") >date_add(next_day(from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd"), "MO"), -7)
          &&
          from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd") < date_add(next_day(from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd"), "MO"), -1)
      )
        .select(
          $"user",
          when($"action" === 1, 1).when($"action" === 2, 2)
            .when($"action" === 3, 2).when($"action" === 5, 5).as("integer")
        )
        .groupBy("user")
        .agg(sum("integer").as("integer"))
        .select($"user",
          $"integer",
          when($"integer" < 1000, "普通用户")
            .when($"integer" >= 1000 && $"integer" <= 3000, "银牌")
            .when($"integer" > 3000 && $"integer" <= 10000, "金牌")
            .when($"integer" > 10000, "钻石").as("result")
        )
  ```

- 自定义udf做法

  ```scala
  //自定义UDF得到不同行为不同积分积分
      val udfInteger: UserDefinedFunction = udf((in: Int) =>
        in match {
          case 1 => 1
          case 2 => 2
          case 3 => 2
          case 5 => 5
        }
      )
  
      //得到不同总结分对应的等级
      val udfRank: UserDefinedFunction = udf((in: Int) =>
        in match {
          case in if in < 1000 => "普通用户"
          case in if in >= 1000 && in <= 3000 => "银牌"
          case in if in > 3000 && in <= 10000 => "金牌"
          case in if in > 10000 => "钻石"
        }
      )
  
      data.filter(
        from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd") >date_add(next_day(from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd"), "MO"), -7)
          &&
          from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd") < date_add(next_day(from_unixtime(unix_timestamp($"ct".cast("String"), "yyyyMMddHHmmss"), "yyyy-MM-dd"), "MO"), -1)
      )
        .select(
          $"user",
          udfInteger($"action").as("integer")
        )
        .groupBy("user")
        .agg(sum($"integer").as("integer"))
        .withColumn("rank" , udfRank($"integer"))
        .show()
  ```

  