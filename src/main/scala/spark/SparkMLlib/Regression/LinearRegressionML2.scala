package spark.SparkMLlib.Regression

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object LinearRegressionML2 {
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
    val data: DataFrame = df.map(row => {
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
    val dataSet: DataFrame = assembler.transform(data)

    //数据采样
    val Array(train , test): Array[Dataset[Row]] = dataSet.randomSplit(Array(0.8 , 0.2))

    //构建模型参数
    val lr: LinearRegression = new LinearRegression("myLinearGOGOGO")
      .setFeaturesCol("features")
      .setLabelCol("PERF")

    //建模
    val model: LinearRegressionModel = lr.fit(train)
    model.transform(test).show()

  }
}
