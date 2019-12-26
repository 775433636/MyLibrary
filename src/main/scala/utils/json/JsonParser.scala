package utils.json

import scala.util.parsing.json.JSON

object JsonParser {

  /**
    * 将json转成map
    *
    * @param json json字串
    */
  def parseJsonToMap(json: String): Map[String, String] = {
    var paramMap = scala.collection.immutable.Map[String, String]()
    val parseJson = JSON.parseFull(json)
    parseJson match {
      case Some(map: Map[String, String]) => paramMap = map
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
    paramMap
  }
}