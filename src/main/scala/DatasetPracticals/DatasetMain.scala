package DatasetPracticals

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.io.StdIn

/*
 1. Spark Submit
    sbt clean package && spark-submit --class DatasetPracticals.DatasetMain --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar
 2. Datasets(Spark 2.0 onwards)
    a. Schema is checked at compile time.
    b. Only available in Scala and Java(compiled) [Not available in Python and R(interpreted)]
 3. Why to use?
    It provide Type Safety.
 4. Additional task to validate data against schema, comparatively slower than Dataframe


 */

case class Store(city: String, state: String, zip_code: Long)

case class State(state: String, stateName: String)

object DatasetMain {

  val spark: SparkSession = SparkSession.builder().appName("Dataset").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    val storeDf = spark.read.format("json").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\store_locations.json")
    storeDf.show(5)

    // Creating the Dataset
    println("Converting the dataframe to dataset")
    val storeDs = storeDf.as[Store]
    println("Schema of store dataset")
    println(storeDs.printSchema)

    // Creating dataset from Sequence
    val stateDs = Seq(State("CA", "California")).toDS()
    stateDs.show()

    // Transformations
    storeDs.groupBy(col("city")).count()
    storeDs.groupByKey(x => x.city).count()

    var input = "dummy"
    while (input.nonEmpty) input = StdIn.readLine()

  }

}
