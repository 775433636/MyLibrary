package spark.SparkMLlib.Regression

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD

object LinearRegression {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("line -gogogo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val lines: RDD[String] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\ML\\CPU.csv")
    val data: RDD[LabeledPoint] = lines.map(line => {
      val tr_Data: Array[Double] = line.split(",").tail.init.map(_.toDouble)
      //输入数据为LabeledPoint[]
      LabeledPoint(line.last.toDouble, Vectors.dense(tr_Data))
    })

//    data.foreach(println)
    val Array(train , test) = data.randomSplit(Array(0.7,0.3))

    //参数
    val numIterations = 100
    val stepSize = 0.001
//    val miniBatchFraction = 1.0
    //模型
    val model: LinearRegressionModel = LinearRegressionWithSGD.train(train , numIterations , stepSize)
    println(model.intercept)
    println(model.weights)
    test.foreach(lp => {
      val predicted: Double = model.predict(lp.features)
      println(s"true=${lp.label} ; predicted=$predicted")
    })
  }
}
