package spark

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    //spark-core
    val conf: SparkConf = new SparkConf().setAppName("rdd run").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    //sspark-sql
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("sql")
      .getOrCreate()
    spark
    import spark.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window
    spark.sparkContext.setLogLevel("error")

    //spark-sql读取文件
    val datasql: DataFrame = spark.read
      .option("header", true)
      .option("inferschema", true)
      .option("delimiter", " ")
      .csv("")

    //spark-sql  hive
    val sparksql = SparkSession
      .builder()
      .appName("CreateDataset")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val data: DataFrame = spark.table("mydb.xxxx")


    //spark-sql读取DB数据
    val properties = new Properties()
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    properties.setProperty("user", "root")
    properties.setProperty("password", "wang1997")

    spark.read.jdbc(
      "jdbc:mysql://slave4:3306/spark",
      "student",
      properties
    )
      .show()


    //spark-streaming
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStream")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("E:\\project\\bigdata\\scalamv\\data\\checkpoint")


  }
}
