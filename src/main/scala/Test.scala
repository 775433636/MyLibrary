import java.util.Properties

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    //sspark-sql
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("sql")
      .getOrCreate()
    spark
    import spark.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window
    spark.sparkContext.setLogLevel("error")

    spark.read.csv("C:\\Users\\Administrator\\Desktop\\12道sql.docx")
      .show()

    val properties = new Properties()
    properties.setProperty("driver" , "com.mysql.jdbc.Driver")
    properties.setProperty("user", "root")
    properties.setProperty("password", "wang1997")

    spark.read.jdbc(
      "jdbc:mysql://slave4:3306/spark",
      "student",
      properties
    )
      .show()
  }
}
