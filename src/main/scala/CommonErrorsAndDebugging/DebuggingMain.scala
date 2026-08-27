package CommonErrorsAndDebugging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

import scala.io.StdIn

/*
  sbt clean package && spark-submit --class CommonErrorsAndDebugging.DebuggingMain --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
 */

object DebuggingMain {

  val spark: SparkSession = SparkSession.builder().appName("Dataset").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val storeDf = spark.read.format("json").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\store_locations.json")
    storeDf.show(5)

    val storeDfPartitioned = storeDf.repartition(5)
    val storeShuffled =  storeDfPartitioned.orderBy(desc("state"))
    storeShuffled.show()



    var input = "dummy"
    while (input.nonEmpty) input = StdIn.readLine()
  }

}
