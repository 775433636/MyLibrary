package spark.SparkMLlib.DecisionTree

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object RandomForests {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("word")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._
    import org.apache.spark.sql.Row

    //读取数据
    val df: DataFrame = spark.read
      .option("header", true)
      .csv("E:\\project\\bigdata\\sparkmv\\data\\ML\\diabetesHeader.csv")

    //转换数据结构类型
    val dfR: DataFrame = df.map(row => {
      (
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble,
        row.getString(4).toDouble,
        row.getString(5).toDouble,
        row.getString(6).toDouble,
        row.getString(7).toDouble,
        row.getString(8).toDouble,
        row.getString(9) match {
          case "tested_positive" => 0.0
          case "tested_negative" => 1.0
        }
      )
    }).toDF("preg", "plas", "pres", "skin", "insu", "mass", "pedi", "age" , "class")

    //
    val labelIndexer : StringIndexerModel = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("indexedLabel")
      .fit(dfR)

    //建立特征转换器并构建特征进行包装
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("preg", "plas", "pres", "skin", "insu", "mass", "pedi", "age"))
      .setOutputCol("features")
    val data: DataFrame = assembler.transform(dfR)

    val Array(train , test): Array[Dataset[Row]] = data.randomSplit(Array(0.8 , 0.2))

    val rfc: RandomForestClassifier = new RandomForestClassifier()
      .setLabelCol("class")
      .setFeaturesCol("features")
      .setNumTrees(10)

    val model: RandomForestClassificationModel = rfc.fit(train)
    val prodicetions: DataFrame = model.transform(test)
    val predictedAndTrue: DataFrame = prodicetions.select("class" , "prediction")
    predictedAndTrue.show()

    val mm: RDD[(Double, Double)] = predictedAndTrue.rdd.map(row => {
      (row.getDouble(1), row.getDouble(0))
    })

    //看模型正确率和混淆矩阵
    val metrics = new MulticlassMetrics(mm)
    println(metrics.accuracy)
    println(metrics.confusionMatrix)
  }
}
