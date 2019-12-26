package spark.SparkMLlib.K_Means

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class IrisData(point : linalg.Vector , label : String)

  object KmeansDemo {
    def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setMaster("local").setAppName("k-means -gogogo")
      val sc = new SparkContext(conf)
      sc.setLogLevel("error")

      val lines: RDD[String] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\iris.csv")
      val data: RDD[IrisData] = lines.map(line => {
        val fields: Array[String] = line.split(",")
        //去头特征00
        val features: Array[Double] = fields.tail.init.map(_.toDouble)
        //封装为向量,封装训练测试
        Vectors.dense(features.map(_.toInt.toDouble))
        IrisData(Vectors.dense(features), fields(5))
      })
      //    data.foreach(println)

      //提取训练数据和目标字段
      //    val (train , lable): (RDD[linalg.Vector] , String) = data.map(x => (x.point , x.label))
      val train: RDD[linalg.Vector] = data.map(_.point)
      val lable: RDD[String] = data.map(_.label)

      //使用训练数据训练数据
      val k : Int = 3
      //允许的最大迭代次数。
      val maxIterations = 1000
      val model: KMeansModel = KMeans.train(train , k , maxIterations)

      //预测模型，集群中心点
      model.clusterCenters.foreach(println)
      //做真实值和预测结果展示
      data.map{case irisD => {
        val predicted: Int = model.predict(irisD.point)
        (irisD.label , predicted)
      }}.sortByKey().foreach(println)
      //    model.predict(train).zip(lable).sortBy(_._2).foreach(println)

      //手肘法判断最佳K值，K值集合
      val k_Lst = Array(2,3,4,5,6,7,10,15,20,25,30,45,50,55,60,80,90,100)
      k_Lst.foreach(k => {
        //根据K值判断模型
        val model: KMeansModel = KMeans.train(train , k , maxIterations)
        //得到SSE
        val cost: Double = model.computeCost(train)
        println(s"k:$k -> cost:$cost")
      })
    }
}