package dev.vergil

import org.apache.log4j._
import org.apache.spark.SparkContext

object TotalSpentByCustomer {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val clientId = fields(0).toInt
    val totalSpent = fields(2).toFloat
    (clientId, totalSpent)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")
    val lines = sc.textFile("in/customer-orders.csv")
    val parsedLine = lines.map(parseLine)
    val totalByCustomer = parsedLine.reduceByKey((x,y) => x+y)
    val results = totalByCustomer.collect()
    results.sortBy(_._2).reverse.foreach(println)
  }





}
