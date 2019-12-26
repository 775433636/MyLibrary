package spark.SparkMLlib.DecisionTree

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

object DecisionRegressionTree {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("k-means -gogogo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    val lines: RDD[String] = sc.textFile("E:\\project\\bigdata\\sparkmv\\data\\ML\\CPU.csv")
    val data: RDD[LabeledPoint] = lines.map(line => {
      val tr_Data: Array[Double] = line.split(",").tail.init.map(_.toDouble)
      //输入数据为LabeledPoint[]
      LabeledPoint(line.last.toDouble, Vectors.dense(tr_Data))
    })

    //参数
    import scala.collection.JavaConversions._
    val categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer] = Map[java.lang.Integer, java.lang.Integer]()
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32

    //训练回归树模型
    val model: DecisionTreeModel = DecisionTree.trainRegressor(data, categoricalFeaturesInfo,impurity , maxDepth , maxBins)
    //预测值
    val prodictedValue: RDD[(Double, Double)] = data.map(lp => {
      val prodicted: Double = model.predict(lp.features)
      (prodicted, lp.label)
    })
    prodictedValue.foreach(println)
    //模型检验，平均误差，得到总误差数和总预测记录数
    val (sum_error , count) = prodictedValue.map { case (pro, lable) => {
      (math.abs(pro - lable), 1)
    }
    }.reduce((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })
    println(sum_error/count)
  }
}
