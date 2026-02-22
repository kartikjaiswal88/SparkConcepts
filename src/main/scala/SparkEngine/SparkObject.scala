package SparkEngine

import org.apache.spark.sql.SparkSession

object SparkObject {
  val localSpark = SparkSession.builder()
    .appName("Spark Application")
    .master("local[*]") // Use all available cores
    .getOrCreate()

  SparkObject.localSpark.sparkContext.setLogLevel("ERROR")

}
