package spark.SparkMLlib.DecisionTree

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("k-means -gogogo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val lines: RDD[String] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\ML\\covtype.data")
    //得到输入数据LabeledPoint
    val data: RDD[LabeledPoint] = lines.map(line => {
      val arr: Array[Double] = line.split(",").map(_.toDouble)
      //机器学习识别的类别是从0开始的
      val lable: Double = arr.last - 1
      val pointVt: linalg.Vector = Vectors.dense(arr.init)
      //封装数据
      LabeledPoint(lable, pointVt)
    })

    //参数
    val numClasses = 7 //类别数
    val categoricalFeaturesInfo: Map[Int, Int] = Map[Int , Int]()
    val impurity = "gini" //information gain的算法
    val maxDepth = 20  //树的最大深度
    val maxBins = 500 //最大箱数

    //训练模型
    val model: DecisionTreeModel = DecisionTree.trainClassifier(data , numClasses , categoricalFeaturesInfo , impurity , maxDepth , maxBins)

    //模型预测
    //结果值和预测值
    val prodictedAndTrue: RDD[(Double, Double)] = data.map(lp => {
      //预测值
      val predicted: Double = model.predict(lp.features)
      // 返回值( 实际值 , 预测值)
//      println(s"predicted:$predicted  -->>  true:${lp.label}")
      (predicted, lp.label)
    })

    val metrics = new MulticlassMetrics(prodictedAndTrue)
    println("accuracy  "+metrics.accuracy)
    println("recall  "+metrics.recall)
    println("fMeasure  "+metrics.fMeasure)
    metrics.confusionMatrix
    println(metrics.confusionMatrix)
  }
}
