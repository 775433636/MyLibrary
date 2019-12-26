package utils.Date

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

/**
  * Created by yxz on 2019/12/14.
  * Java1.8  线程安全时间日期类型
  */
object DateTimeUtils {

  // 获取当前日期时间字符串 格式: yyyy-MM-dd HH:mm:ss
  def getDate: String = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    dtf.format(
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(Instant.now.toEpochMilli),
        ZoneId.systemDefault
      )
    )
  }

  // 根据时间戳获取日期时间字符串 格式: yyyy-MM-dd HH:mm:ss
  def getDate(timestamp: Long): String = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    dtf.format(
      LocalDateTime.ofInstant(
        Instant.ofEpochMilli(timestamp),
        ZoneId.systemDefault
      )
    )
  }

  // 获取当前时间戳
  def getTimeStamp: Long = {
    Instant.now.toEpochMilli
  }

  // 根据日期时间字符串(yyyy-MM-dd HH:mm:ss)获取时间戳
  def getTimeStamp(date: String): Long = {
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val parse: LocalDateTime = LocalDateTime.parse(date, dtf)
    LocalDateTime.from(parse).atZone(ZoneId.systemDefault).toInstant.toEpochMilli
  }

  // 获取前一天的日期 格式: yyyy-MM-dd
  def getYesterday: String = {
    LocalDate.now.minusDays(1).toString
  }

  def main(args: Array[String]): Unit = {

    val str ="-1"
    val time = str.split("\\+")(0).replaceAll("T"," ")

    println(getTimeStamp(time))
  }
}
