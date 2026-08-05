package SparkAppDevelopmentAndDeployment

import org.apache.spark.sql.SparkSession

import scala.io.StdIn

/*
 1. Spark Submit
    sbt clean package && spark-submit --class SparkAppDevelopmentAndDeployment.SparkDevelopmentDay17 --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
 */

object SparkDevelopmentDay17 {

  val spark: SparkSession = SparkSession.builder().appName("Dataset").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val df = spark.range(2, 100, 2)
    val dfPart = df.repartition(4)

    val dfJoin = dfPart.join(dfPart, "id")

    println(dfJoin.collect().mkString("Array(", ", ", ")"))

    var input = "dummy"
    while (input.nonEmpty) input = StdIn.readLine()

  }

}
