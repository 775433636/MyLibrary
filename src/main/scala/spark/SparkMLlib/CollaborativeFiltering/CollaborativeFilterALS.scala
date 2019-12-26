package spark.SparkMLlib.CollaborativeFiltering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object CollaborativeFilterALS {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("k-means -gogogo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val lines: RDD[String] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\ML\\u.data")
    val data: RDD[Rating] = lines.map(line => {
      val arr: Array[String] = line.split("\\s+")
      Rating(arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    })

    //分数据
    val Array(trainData, testData): Array[RDD[Rating]] = data.randomSplit(Array(0.9, 0.001))

    /**
      * def train(
      * ratings: RDD[Rating],
      * rank: Int,
      * iterations: Int,
      * lambda: Double,
      * blocks: Int,
      * seed: Long
      * ): MatrixFactorizationModel = {
      */
    val rank = 50
    val iterations = 10
    val lambda = 0.01
    val blocks = 10
    val seed  = 222

//    val model: MatrixFactorizationModel = ALS.train(
//      trainData,
//      rank,
//      iterations,
//      lambda
//    )
//    model.save(sc , "E:\\project\\bigdata\\sparkmv\\data\\model")

    val model: MatrixFactorizationModel = MatrixFactorizationModel.load(sc , "E:\\project\\bigdata\\sparkmv\\data\\model")

    println(model.predict(253, 465))
    println(model.recommendProducts(253, 465).take(5).map(rt => (rt.rating.toInt , rt.product)).toList)

    /**
      * 5.096900266549087
      * List((6,526), (6,648), (6,470), (6,265), (6,468))
      *
      * 4.945592407289294
      * List((6,633), (6,67), (6,313), (6,416), (5,205))
      *
      *4.524192541428983
      * List((6,51), (6,520), (6,525), (6,194), (5,657))
      *
      * 4.524192541428983
      * List((6,51), (6,520), (6,525), (6,194), (5,657))
      */
  }
}
