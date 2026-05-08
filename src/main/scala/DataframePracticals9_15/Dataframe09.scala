package DataframePracticals9_15

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

/*
  1. Spark Submit command to Run the code
     sbt clean package && spark-submit --class DataframePracticals9_15.Dataframe09 --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
  2. To print schema of df: df.schema or df.printSchema()
  3. Defining manual Schema
      val manualSchema = StructType(Seq(StructField("city", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("zip_code", LongType, nullable = true)))
  4. To see particular column: df.col("city")
 */


object Dataframe09 {
  val localSpark = SparkSession.builder()
    .appName("DataframeDay1_09")
    .master("local[*]")
    .getOrCreate()

  localSpark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val storeDf = localSpark.read.format("json").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\store_locations.json")

    // Defining manual Schema
    val manualSchema = StructType(Seq(StructField("city", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("zip_code", LongType, nullable = true)))

    val storeDfManualSchema = localSpark.read
      .format("json")
      .schema(manualSchema)
      .load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\store_locations.json")

    println("Printing the schema..........")
    println(storeDf.schema)

    println("Printing the printSchema")
    storeDf.printSchema()

    println("Printing the Dataframe...with Spark Schema")
    storeDf.show()

    println("Printing the Dataframe...with Manual Schema")
    storeDfManualSchema.show()

    println("Printing all the columns..")
    println(storeDf.columns.mkString("Array(", ", ", ")"))

    println("Printing only particular column")
    val cityDf = storeDfManualSchema.select(col("city"))
    cityDf.show()

    println("Printing the first row")
    println(storeDf.first())


  }

}
