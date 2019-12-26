package utils

import com.typesafe.config.ConfigFactory
import utils.Date.DateUtils

object ConfigHolder {
  // 加载数据文件
  private val config = ConfigFactory.load()
  //  App Info
  lazy val sparkAppName: String = config.getString("spark.appname")

  // Spark parameters
  lazy val sparkMaster: String = config.getString("spark.master")

  lazy val sparkParameters: List[(String, String)] = List(
    ("spark.worker.timeout", config.getString("spark.worker.timeout")),
    ("spark.cores.max", config.getString("spark.cores.max")),
    ("spark.rpc.askTimeout", config.getString("spark.rpc.askTimeout")),
    ("spark.network.timeout", config.getString("spark.network.timeout")),
    ("spark.task.maxFailures", config.getString("spark.task.maxFailures")),
    ("spark.speculation", config.getString("spark.speculation")),
    ("spark.driver.allowMultipleContexts", config.getString("spark.driver.allowMultipleContexts")),
    ("spark.serializer", config.getString("spark.serializer")),
    ("spark.buffer.pageSize", config.getString("spark.buffer.pageSize")),
    ("spark.sql.shuffle.partitions", config.getString("spark.sql.shuffle.partitions"))

  )

  // kudu parameters
  lazy val kuduMaster: String = config.getString("kudu.master")

  // input dataset
  lazy val adDataPath: String = config.getString("addata.path")
  lazy val ipsDataPath: String = config.getString("ipdata.geo.path")
  lazy val qqFilePath: String = config.getString("qqwrydat.path")
  lazy val qqinstallDir: String = config.getString("installDir.path")

  // output dataset
  private lazy val delimiter = "_"
  private lazy val odsPrefix: String = config.getString("ods.prefix")
  private lazy val adInfoTableName: String = config.getString("ad.data.tablename")
  lazy val ADMainTableName = s"$odsPrefix$delimiter$adInfoTableName$delimiter${DateUtils.getToday()}"

  // report
  lazy val Report1RegionStatTableName: String = config.getString("report.region.stat.tablename")
  lazy val ReportRegionTableName: String = config.getString("report.region.tablename")
  lazy val ReportAppTableName: String = config.getString("report.app.tablename")
  lazy val ReportDeviceTableName: String = config.getString("report.device.tablename")
  lazy val ReportNetworkTableName: String = config.getString("report.network.tablename")
  lazy val ReportIspTableName: String = config.getString("report.isp.tablename")
  lazy val ReportChannelTableName: String = config.getString("report.channel.tablename")

  //keys
  lazy val ReportIspKeys:String = config.getString("report.isp.keys")

  // GaoDe API
  private lazy val gaoDeKey: String = config.getString("gaoDe.app.key")
  private lazy val gaoDeTempUrl: String = config.getString("gaoDe.url")
  lazy val gaoDeUrl: String = s"$gaoDeTempUrl&key=$gaoDeKey"

  // GeoHash
  lazy val keyLength: Int = config.getInt("geohash.key.length")

  // 商圈库
  lazy val tradingAreaTableName: String = config.getString("trading.area.tablename")


  // tags
  private lazy val tagDelimiter: String = config.getString("tag.delimiter")
  lazy val adPrefix: String = config.getString("adtype.prefix") + tagDelimiter
  lazy val channelPrefix: String = config.getString("channel.prefix") + tagDelimiter
  lazy val appnamePrefix: String = config.getString("appname.prefix") + tagDelimiter
  lazy val sexPrefix: String = config.getString("sex.prefix") + tagDelimiter
  lazy val provincePrefix: String = config.getString("province.prefix") + tagDelimiter
  lazy val cityPrefix: String = config.getString("city.prefix") + tagDelimiter
  lazy val clientPrefix: String = config.getString("client.prefix") + tagDelimiter
  lazy val networkPrefix: String = config.getString("network.prefix") + tagDelimiter
  lazy val ispPrefix: String = config.getString("isp.prefix") + tagDelimiter
  lazy val keywordPrefix: String = config.getString("keyword.prefix") + tagDelimiter
  lazy val agePrefix: String = config.getString("age.prefix") + tagDelimiter
  lazy val tradingareaPrefix: String = config.getString("tradingarea.prefix") + tagDelimiter

  // 标识字段
  lazy val idFields: String = config.getString("non.empty.field")
  lazy val filterSQL: String = idFields
    .split(",")
    .map(field => s"$field is not null ")
    .mkString(" or ")
  lazy val appNameDic: String = config.getString("appname.dic.path")
  lazy val deviceDic: String = config.getString("device.dic.path")
  lazy val tagsTableNamePrefix: String = config.getString("tags.table.name.prefix") + delimiter
  lazy val tagCoeff: Double = config.getDouble("tag.coeff")
  lazy val tagsTableName :String=config.getString("tags.table.name")



  // 加载 elasticsearch 相关参数
  lazy val ESSparkParameters = List(
    ("cluster.name", config.getString("es.cluster.name")),
    ("es.index.auto.create", config.getString("es.index.auto.create")),
    ("es.nodes", config.getString("es.Nodes")),
    ("es.port", config.getString("es.port")),
    ("es.index.reads.missing.as.empty", config.getString("es.index.reads.missing.as.empty")),
    ("es.nodes.discovery", config.getString("es.nodes.discovery")),
    ("es.nodes.wan.only", config.getString("es.nodes.wan.only")),
    ("es.http.timeout", config.getString("es.http.timeout"))
  )

  // 获取redis
  lazy val redisHost: String = config.getString("redis.host")
  lazy val redisPort: Int = config.getInt("redis.port")


  def main(args: Array[String]): Unit = {
    println(ConfigHolder.sparkParameters)
    println(ConfigHolder.qqinstallDir)
    println(filterSQL)
  }
}