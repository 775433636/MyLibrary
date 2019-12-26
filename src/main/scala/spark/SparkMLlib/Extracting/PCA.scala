package spark.SparkMLlib.Extracting

import org.apache.spark.ml.feature.{PCA, PCAModel, VectorAssembler}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.sql.{DataFrame, SparkSession}

object PCA {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("word")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._
    import org.apache.spark.sql.Row

    val df: DataFrame = spark.read
      .option("header", true)
      .csv("E:\\project\\bigdata\\sparkmv\\data\\ML\\CPUHeader.csv")

    //转化类型
    val dfR: DataFrame = df.map(row => {
      (
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble,
        row.getString(4).toDouble,
        row.getString(5).toDouble,
        row.getString(6).toDouble,
        row.getString(7).toDouble
      )
    }).toDF("MYCT","MMIN","MMAX","CACH","CHMIN","CHMAX","PERF")

    //特征转换器
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("MYCT", "MMIN", "MMAX", "CACH", "CHMIN", "CHMAX"))
      .setOutputCol("features")
    //将特征进行包装
    val data: DataFrame = assembler.transform(dfR).cache()

    val pca: PCAModel = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(data)


    pca.transform(data).select("pcaFeatures")
      .rdd
      .coalesce(1)
      .saveAsTextFile("C:\\Users\\Administrator\\Desktop\\resutllll")

  }
}
