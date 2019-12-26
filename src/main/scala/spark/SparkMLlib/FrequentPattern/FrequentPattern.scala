package spark.SparkMLlib.FrequentPattern

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeSet

object FrequentPattern {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("k-means -gogogo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val lines: RDD[String] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\ML\\dataset_group.csv")
    val data: RDD[(String, TreeSet[String])] = lines.map(line => {
      val arr: Array[String] = line.split(",")
      (arr(1), arr(2))
      //购物篮中不能有重复数据
    }).aggregateByKey(TreeSet[String]())(
      (ts, elem) => {
        val newTs: TreeSet[String] = ts + elem
        newTs
      }
      ,
      (ts1, ts2) => {
        ts1 ++ ts2
      }
    )

    //数据格式要求一个泛型，转为Array给到训练
    val trainData: RDD[Array[String]] = data.map(_._2.toArray).cache()

    //配置参数
    val minSupport = 0.35
    val numPartitions = 10
    //建立模型
    val model: FPGrowthModel[String] = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)
      .run(trainData)
    println(model.freqItemsets.collect().toList)
  }
}
