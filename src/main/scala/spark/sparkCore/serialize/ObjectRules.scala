package spark.sparkCore.serialize

/**
  * 第三种方式
  */
object ObjectRules extends Serializable{
  val rulesMap = Map("xiaoli" -> 25, "xiaofang" -> 27)
}

/**
  * 第四种方式
  */
//object ObjectRules {
//  val rulesMap = Map("xiaoli" -> 25, "xiaofang" -> 27)
//  println("hostname的名称为：" + InetAddress.getLocalHost.getHostName)
//}