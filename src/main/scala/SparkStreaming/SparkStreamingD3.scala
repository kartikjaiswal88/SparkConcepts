package SparkStreaming

import org.apache.spark.sql.SparkSession

import scala.io.StdIn

/*
  sbt clean package && spark-submit --class SparkStreaming.SparkStreamingD3 --master local[*] target/scala-2.13/sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
 */

object SparkStreamingD3 {


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

    val activityCheckPointQuery = activityCountStream.writeStream.queryName("activityCountStreamCheckpointQuery")
      .format("console").outputMode("complete").option("checkpointLocation", "src/main/resources/checkpoint")
      .option("truncate", value = false).start()

    activityCheckPointQuery.awaitTermination()


    var input = "dummy"
    while (input.nonEmpty) input = StdIn.readLine()
  }

}
