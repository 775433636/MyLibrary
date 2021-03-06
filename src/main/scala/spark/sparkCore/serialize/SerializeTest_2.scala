package spark.sparkCore.serialize

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用序列化的方式
  */
object SerializeTest_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(Array("xiaoli", "xiaofang", "xiaolin"))

    // 该对象在Driver端创建
    val rules = new Rules
    println("Driver端的哈希值：" + rules.toString)

    // map方法中的函数是在Executor的某个Task中执行的
    val res = lines.map(x => {
      // 获取task的hostname，也就是判断该task是在哪个节点执行的
      val hostname = InetAddress.getLocalHost.getHostName
      // 获取当前线程名称
      val threadName = Thread.currentThread().getName
      // rules是在Executor中使用的
      (hostname, threadName, rules.rulesMap.getOrElse(x, 0), rules.toString)
    })

    println(res.collect.toBuffer)

    sc.stop()
  }
}
