package utils.Date

import java.util.{Calendar, Date}

import org.apache.commons.lang.time.FastDateFormat

object DateUtils {
  //获得今天的日期
  def getToday(): String ={
    val date = new Date
    FastDateFormat.getInstance("yyyyMMdd").format(date)
  }

  //获得昨天的日期
  def getYesterday(): String ={
    //获得日历
    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH , -1)
    //日期格式化
    FastDateFormat.getInstance("yyyyMMdd").format(calendar)
  }

  def main(args: Array[String]): Unit = {
    println(getYesterday)
  }
}
