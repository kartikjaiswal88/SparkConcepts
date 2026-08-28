package SparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.io.StdIn

/*
  spark-submit --class SparkStreaming.SparkStreamingJsonToStreaming --master local[*] target/scala-2.13/sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
 */

object SparkStreamingJsonToStreaming {

  val spark: SparkSession = SparkSession.builder().appName("SparkStreamingD2").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val activityData = spark.read.json("src/main/resources/activity_streaming_files")

    val activitySchema = activityData.schema

    println("Printing the schema..")
    println(activitySchema)

    val activityDataStream = spark.readStream.schema(activitySchema).option("maxFilesPerTrigger", 1)
      .json("src/main/resources/activity_streaming_files")

    val activityCountStream = activityDataStream.groupBy("gt").count()

    val activityQuery = activityCountStream.writeStream.queryName("activityCountStreamQuery")
      .format("console").outputMode("complete").start()

    activityQuery.awaitTermination()


    var input = "dummy"
    while (input.nonEmpty) input = StdIn.readLine()
  }

}
