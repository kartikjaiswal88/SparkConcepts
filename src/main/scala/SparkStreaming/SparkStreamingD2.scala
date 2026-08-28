package SparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/*
  spark-submit --class SparkStreaming.SparkStreamingD2 --master local[*] target/scala-2.13/sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
 */

object SparkStreamingD2 {

  val spark: SparkSession = SparkSession.builder().appName("SparkStreamingD2").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .start()

    query.awaitTermination()

  }


}
