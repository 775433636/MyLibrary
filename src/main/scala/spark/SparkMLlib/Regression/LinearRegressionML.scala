package spark.SparkMLlib.Regression

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, OneHotEncoder, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 使用ML Pipeline构建机器学习工作流
  */
object LinearRegressionML {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("word")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._
    import org.apache.spark.sql.Row

    //sc的souce得到数据集并且转为double然后用Row封装
    val parsedRDD: RDD[Row] = spark.sparkContext.textFile("E:\\project\\bigdata\\sparkmv\\data\\ML\\CPU.csv")
      .map(_.split(",").tail.map(_.toDouble)).map(x =>
      Row(x(0) , x(1) , x(2) , x(3) , x(4) , x(5) , x(6))
    )

    val struct: StructType = StructType(
      StructField("f0", DoubleType, nullable = false) ::
        StructField("f1", DoubleType, nullable = false) ::
        StructField("f2", DoubleType, nullable = false) ::
        StructField("f3", DoubleType, nullable = false) ::
        StructField("f4", DoubleType, nullable = false) ::
        StructField("f5", DoubleType, nullable = false) ::
        StructField("label", DoubleType, nullable = false) :: Nil)

    //将数据封装为df
    val df: DataFrame = spark.createDataFrame(parsedRDD,struct).cache()

    /**
      * 步骤2 * StringIndexer将标签的字符串列编码为标签索引的列。
      * 索引在[0，numLabels）中，按标签频率排序。
      * 这可以帮助检测原始数据中的标签并自动为其提供索引。
      * 可以通过现有的火花机学习算法轻松地对其进行处理。
      */
    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)

    /**
      * 步骤3 定义一个VectorAssembler转换器，
      * 以将源要素数据转换为向量这在原始输入数据包含非要素列时非常有用，
      * 并且这样的输入数据文件通常包含诸如“ ID”之类的列，“日期”等
      */
    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("f0", "f1", "f2", "f3", "f4", "f5"))
      .setOutputCol("featureVector")

    /**
      * 步骤4 创建LinearRegression实例并设置输入参数。
      */
    val lrRegression: LinearRegression = new LinearRegression()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("featureVector")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    /**
      * 步骤5 将索引的类标签转换回原始标签，
      * 以便在我们需要将预测结果显示或保存到文件中时可以轻松理解。
      */
    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    /**
      * 步骤6 分段切分数据为训练集和测试集
      */
    val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))

    /**
      * 步骤7 创建由4个PipelineStage对象构造的ML管道。
      * 然后调用fit方法对训练数据执行已定义的操作。
      */
    val pipeline: Pipeline = new Pipeline().setStages(Array(labelIndexer,vectorAssembler,lrRegression,labelConverter))
    val model: PipelineModel = pipeline.fit(trainingData)

    /**
      * 步骤8 对测试数据进行预测。此转换方法将返回结果DataFrame ，
      * 并将新的预测列附加到先前的DataFrame上。
      */
    val predictionResultDF: DataFrame = model.transform(trainingData)

    /**
      * 得到模型，可以进行一些操作
      */
    val regression: LinearRegression = model.stages(2).asInstanceOf[LinearRegression]

    predictionResultDF.show()

    predictionResultDF.select("f0","f1","f2","f3","f4","f5","label","predictedLabel").show(20)
  }
}
