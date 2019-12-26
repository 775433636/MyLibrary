package spark.SparkMLlib.K_Means

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object GaussianMixture {
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

    val model: GaussianMixtureModel = new GaussianMixture().setK(k).run(train)
  }
}
