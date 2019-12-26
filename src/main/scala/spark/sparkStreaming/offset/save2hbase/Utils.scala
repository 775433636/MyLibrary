package spark.sparkStreaming.offset.save2hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}

object Utils {
var connection: Connection = null

    //获取配置对象
    val hbaseConf: Configuration = HBaseConfiguration.create()
    //设置zookeeper的配置信息
    hbaseConf.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181")

    connection = ConnectionFactory.createConnection(hbaseConf)

  def getConnection: Connection ={
    connection
  }

  def getAdmin: Admin ={
    val admin: Admin = connection.getAdmin
    admin
  }
}

