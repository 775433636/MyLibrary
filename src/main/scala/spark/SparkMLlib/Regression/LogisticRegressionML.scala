package spark.SparkMLlib.Regression

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object LogisticRegressionML {
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

    //建立特征转换器并构建特征进行包装
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("preg", "plas", "pres", "skin", "insu", "mass", "pedi", "age"))
      .setOutputCol("features")
    val data: DataFrame = assembler.transform(dfR)

    val Array(train , test): Array[Dataset[Row]] = data.randomSplit(Array(0.8 , 0.2))

    val lgr: LogisticRegression = new LogisticRegression()
      //参数一配全为1
      .setMaxIter(1000)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)
      .setTol(1E-10)
      .setFeaturesCol("features")
      .setLabelCol("class")

    val model: LogisticRegressionModel = lgr.fit(train)
    val predictedAndTrue: DataFrame = model.transform(test).select( "class" , "prediction")

    val mm: RDD[(Double, Double)] = predictedAndTrue.rdd.map(row => {
      (row.getDouble(1), row.getDouble(0))
    })

//    println(model.coefficientMatrix)
//    println(model.interceptVector)

    //看模型正确率和混淆矩阵
    val metrics = new MulticlassMetrics(mm)
    println(metrics.accuracy)
    println(metrics.confusionMatrix)
  }
}
