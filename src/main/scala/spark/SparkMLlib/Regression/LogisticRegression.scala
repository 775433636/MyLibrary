package spark.SparkMLlib.Regression

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object LogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("k-means -gogogo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val lines: RDD[String] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\ML\\diabetes.csv")
    val data: RDD[LabeledPoint] = lines.map(line => {
      val arr: Array[String] = line.split(",")
      val tr_data: Array[Double] = arr.tail.init.map(_.toDouble)
      val lable: String = arr.last
      val lableR: Int = lable match {
        case "tested_positive" => 0
        case "tested_negative" => 1
        case _ => 2
      }
      LabeledPoint(lableR, Vectors.dense(tr_data))
    }).filter(_.label<=1)

    //分训练测试s
    val Array(train , test): Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.7,0.3))

    //使用ML的lineRegression
    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(train)

    //得到预测值
    val prodictedAndTrue: RDD[(Double, Double)] = test.map(lp => {
      val predicted: Double = model.predict(lp.features)
      (lp.label, predicted)
    })

    //建立混淆矩阵
    val metrics = new MulticlassMetrics(prodictedAndTrue)
    println("***********************")
    println(metrics.accuracy)
    println(metrics.recall)
    println(metrics.fMeasure)
    println(metrics.confusionMatrix)
  }
}
