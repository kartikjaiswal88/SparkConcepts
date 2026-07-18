package DataframePracticals9_15

import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.ListBuffer
import java.util.Comparator
import scala.io.StdIn

/*
   1. Spark Submit
      sbt clean package && spark-submit --class DataframePracticals9_15.Dataframe14MultipleFormats --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar

 */

object Dataframe14MultipleFormats {

  val localSpark: SparkSession = SparkSession.builder().appName("DfMultiFormatsConverter").master("local[*]").getOrCreate()
  localSpark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val filesToDelete = ListBuffer[Path]()

    try {
      // CSV to Json Converter
      println("Writing csv file to Json format")
      val carDf = localSpark.read.format("csv").option("header", "true").option("inferSchema", "true").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_practice_1000_records.csv")
      carDf.write.format("json").mode("overwrite").
        json("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_practice_1000_records_json")
      filesToDelete += Paths.get("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_practice_1000_records_json")


      // JSON to Parquet Converter
      println("Writing Json file to Parquet format")
      val jsonCarDf = localSpark.read.format("json").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_prediction.json")
      jsonCarDf.write.format("parquet").save("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_parquet")
      filesToDelete += Paths.get("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_parquet")


      // Parquet to ORC
      println("Writing Parquet file to Orc format")
      val parquetCarDf = localSpark.read.format("json").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price.parquet")
      parquetCarDf.write.format("orc").save("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_orc")
      filesToDelete += Paths.get("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_orc")

      // ORC to CSV
      //    println("Writing ORC file to CSV format")
      //    val orcCarDf =  localSpark.read.format("orc").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_orc.orc")
      //    parquetCarDf.write.format("csv").save("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_csv")
      //    filesToDelete += Paths.get("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_csv")


      // Repartitioning the data
      println("Repartitioning the data into five partitions")
      val carDfRepartioned = localSpark.read.format("csv").option("header", "true").option("inferSchema", "true").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_practice_1000_records.csv")
      val carDf5 = carDfRepartioned.repartition(5)

      // Partitioning the data on the basis of particular key
      carDf5.write.format("json").mode("overwrite").partitionBy("fueltype").
        json("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_repartitioned")
      filesToDelete += Paths.get("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_repartitioned")

      // BucketBy
      carDf5.write.format("json").mode("overwrite").bucketBy(4, "carName").
        json("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_bucket")
      filesToDelete += Paths.get("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_bucket")


    }
    finally {
      var input = "x"
      while (input.nonEmpty) input = StdIn.readLine()

      filesToDelete.foreach { path =>
        if (Files.exists(path)) {
          if (Files.isDirectory(path)) {
            Files.walk(path)
              .sorted(Comparator.reverseOrder()) // Delete children first
              .forEach(Files.delete)

            println(s"Directory deleted: $path")
          } else {
            Files.delete(path)
            println(s"File deleted: $path")
          }
        } else {
          println(s"Not found: $path")
        }
      }
    }

  }


}
