package spark.sparkStreaming.offset.save2hbase

import kafka.common.TopicAndPartition
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.mutable

/**
  * 需求：要求保留历史offset
  * 实现功能：
  *   1.保存
  *   2.取值
  *   3.校验
  * 设计：
  *   1.rowkey：前缀+topic+groupid+timestamp
  *   2.colFamily ： 固定 offsets
  *   3.col ： partition
  *   4.value：offset
  */
object SaveOffsetToHBase {
  private val hbaseTableName = "myhb:kafka_offsets"
  private val prefix = "myOffset"
  private val colFamily = "offsets"

  //建表
  def createTable(name: String): Unit = {
    val tableName: TableName = TableName.valueOf(name)
    val descriptor = new HTableDescriptor(tableName)
    val columnDescriptor = new HColumnDescriptor("offsets")
    columnDescriptor.setInMemory(true)
    columnDescriptor.setVersions(1, 10)
    columnDescriptor.setTimeToLive(24 * 60 * 60)
    descriptor.addFamily(columnDescriptor)
    Utils.getAdmin.createTable(descriptor)
  }

  //获取rowkey的方法
  def getRowKey(topic : String , groupid : String = "" ,timestamp : Time): String ={
    s"$prefix:${topic}:${groupid}:${timestamp.milliseconds}"
  }

  //保存
  def saveOffsetToHbase(ranges :OffsetRange , topics :Set[String] , groupid :String = "" , time : Time ): Unit ={
    val admin: Admin = Utils.getAdmin
    val connection: Connection = Utils.getConnection
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
    //遍历topics组进行获取rowkey，创建put，添加列值
    topics.map{topic => {
      val rowKey: String = getRowKey(topic , groupid , time)
      val put: Put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(
        Bytes.toBytes(colFamily),
        Bytes.toBytes(ranges.partition.toString),
        Bytes.toBytes(ranges.untilOffset.toString)
      )
      table.put(put)
    }}
    admin.close()
    //不能关闭呢
//    connection.close()
  }

  //获取
  def getOffsetFromHbase(topics : Set[String] , groupid : String = ""): Map[TopicAndPartition, Long] ={
    val admin: Admin = Utils.getAdmin
    val connection: Connection = Utils.getConnection
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
    val scan: Scan = new Scan()
    var scanner: ResultScanner = null
    var resMap: mutable.Map[TopicAndPartition, Long] = mutable.Map[TopicAndPartition, Long]()

    //myOffset:topica::1573908197000   存储的rowkey
    topics.map(topic => {
      //獲得开始和结束的rowkey
      val startRow =  s"$prefix:$topic:$groupid:${String.valueOf(System.currentTimeMillis())}"
      val stopRow =  s"$prefix:$topic:$groupid:0"
      //取最后的值
      scanner = table.getScanner(scan.setStartRow(startRow.getBytes()).setStopRow(stopRow.getBytes()).setReversed(true))
      val resultScanner: Result = scanner.next()
      if (resultScanner != null){
        resultScanner.rawCells().foreach(cell => {
          val partition: String = Bytes.toString(CellUtil.cloneQualifier(cell))
          val offset: String = new String(CellUtil.cloneValue(cell) , "UTF-8")
//          println(offset)
//          println(partition)
          val tp: TopicAndPartition = TopicAndPartition(topic , partition.toInt)
//          println(tp)
          resMap += (tp -> offset.toLong)
        })
      }
      resMap
    })
//    scanner.close()
//    connection.close()
    resMap.toMap
  }

  def main(args: Array[String]): Unit = {
    //建表
//        createTable("myhb:kafka_offsets")
    println(getOffsetFromHbase(Set("topica")))

  }


}
