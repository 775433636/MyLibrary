package spark.sparkStreaming.myworking

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Put, Table}

/**
  *将业务数据存到HBase
  * value :
  *   1. 总金额
  *   2. 平均值
  *   3. 滑动总金额
  * rowkey :
  *   1. 事件 + 时间戳
  * colFamily：
  *   primary_need
  * colName:
  *   sumMoney5
  *   avgMoney5
  *   sumMoneyWin10
  */
object StockHbaseUtils {
  //基本属性
  private val hbaseTableName = "myhb:stockResult"
  private val colFamily = "primary_need"

  //建表
  def createTable(name: String): Unit = {
    val tableName: TableName = TableName.valueOf(name)
    val descriptor = new HTableDescriptor(tableName)
    val columnDescriptor = new HColumnDescriptor(colFamily)
    columnDescriptor.setInMemory(true)
    columnDescriptor.setVersions(1, 10)
    columnDescriptor.setTimeToLive(24 * 60 * 60)
    descriptor.addFamily(columnDescriptor)
    Utils.getAdmin.createTable(descriptor)
  }

  //存数据
  def saveDataToHbase(time : String , event : String , value : String): Unit ={
    //获取rowkey
    val rowkey: String = event+"-"+time
    //获取连接和实例table
    val connection: Connection = Utils.getConnection
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
    val admin: Admin = Utils.getAdmin
    //实例化put
    val put = new Put(rowkey.getBytes())
    put.addColumn(colFamily.getBytes , event.getBytes , value.getBytes)
    //上传
    table.put(put)
//    connection.close()
//    table.close()
  }

  def main(args: Array[String]): Unit = {
//    createTable(hbaseTableName)
  }

}
