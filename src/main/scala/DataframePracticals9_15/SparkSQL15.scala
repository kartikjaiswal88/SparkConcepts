package DataframePracticals9_15

import org.apache.spark.sql.SparkSession
import scala.io.StdIn

/*
  1. Spark Submit
      sbt clean package && spark-submit --class DataframePracticals9_15.SparkSQL15 --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
 */

object SparkSQL15 {

  val spark: SparkSession = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    println("Printing all the databases of SPARK SQL")
    spark.sql("SHOW DATABASES").show()

    println("Creating the student Database")
    spark.sql("CREATE DATABASE STUDDB")
    spark.sql("SHOW DATABASES").show()

    println("Current Database")
    spark.sql("use studdb")
    spark.sql("SELECT current_database()").show()

    println("Dropping studdb database")
    spark.sql("DROP DATABASE studdb")
    spark.sql("SHOW DATABASES").show()
    spark.sql("DROP DATABASE IF EXISTS studdb").show()


    // Creating table in Hive
    println("Creating the table")
    spark.sql("CREATE DATABASE storeDb")
    spark.sql("use storeDb")
    spark.sql(
      """ CREATE TABLE IF NOT EXISTS store_locations (city STRING, state STRING, zip_code STRING)
        |   USING JSON OPTIONS (path "C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\store_locations.json") """.stripMargin)

    spark.sql("SHOW TABLES").show()

    // Quering the table
    spark.sql(""" SELECT * FROM store_locations """).show()

    // Creating the Managed Table
    println("Creating the managed table")
    spark.sql("DROP TABLE IF EXISTS store_locations_m")
    spark.sql("CREATE TABLE IF NOT EXISTS store_locations_m as SELECT * FROM store_locations")
    spark.sql("DROP TABLE IF EXISTS store_locations_n")
    spark.sql("CREATE TABLE IF NOT EXISTS store_locations_n USING PARQUET as SELECT * FROM store_locations")
    spark.sql("SHOW TABLES").show()

    // Creating the table using partition by
    println("Creating the managed table using partition by")
    spark.sql("DROP TABLE IF EXISTS partition_store_locations")
    spark.sql(""" CREATE TABLE IF NOT EXISTS partition_store_locations USING PARQUET PARTITIONED BY (city) as SELECT city, state, zip_code FROM store_locations LIMIT 10""")
    spark.sql(""" SHOW PARTITIONS partition_store_locations """).show()
    spark.sql("SHOW TABLES").show()

    // Refreshing the metadata of spark for incoming data that is added later of external table
    println("Refreshing the spark metadata for table partition_store_locations")
    spark.sql("REFRESH TABLE partition_store_locations").show()

    // Refreshing the metadata of both Spark and Hive for incoming data that is added later of external table
    println("Refreshing the spark and hive metadata for table partition_store_locations")
    spark.sql("MSCK REPAIR TABLE partition_store_locations").show()

    // Cache and Uncache table for frequent queries to optimize the performance
    println("CACHING AND UNCACHING THE TABLE")
    spark.sql("CACHE TABLE store_locations")
    spark.sql("UNCACHE TABLE store_locations")


    println("Creating the VIEWS")
    spark.sql(""" CREATE OR REPLACE VIEW store_locations_v as SELECT * FROM store_locations WHERE city = "Woodland"  """)
    spark.sql("SELECT * FROM store_locations_v").show()

    println("Creating the temp view")
    spark.sql(""" CREATE OR REPLACE TEMP VIEW store_locations_vt as SELECT * FROM store_locations WHERE city = "Woodland"  """)

    println("Creating the Global temp view")
    spark.sql(""" CREATE OR REPLACE GLOBAL TEMP VIEW store_locations_vgt as SELECT * FROM store_locations WHERE city = "Woodland"  """)
    spark.sql("SELECT * FROM GLOBAL_TEMP.store_locations_vgt").show()
    println("Printing all the VIEWS")
    spark.sql("SHOW VIEWS").show()


    var input = "dummy"
    while (input.nonEmpty) input = StdIn.readLine()
  }


}
