package DataframePracticals9_15

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{
  array, col, current_date, current_time, date_add, date_sub,
  explode, expr, get_json_object, json_tuple, lit,
  split, struct, to_date, to_timestamp, udf
}

import scala.io.StdIn

/*
 1. Spark Submit
      sbt clean package && spark-submit --class DataframePracticals9_15.Dataframes12 --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar

 */

object Dataframes12 {
  val localSpark: SparkSession = SparkSession.builder().appName("DataframeDay12").master("local[*]").getOrCreate()
  localSpark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val carDf = localSpark.read.format("csv").option("inferschema", "true").option("header", "true")
      .load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_practice_1000_records.csv")

    println("Printing the schema of Car Dataframe")
    carDf.printSchema()
    carDf.show(5)


    // Creating and Querying the view from Car Dataframe
    carDf.createOrReplaceTempView("carDfView")
    localSpark.sql("select * from carDfView").show()


    // Creating a dataframe contains structured data
    val structDf = carDf.select(struct("carName", "carBody").alias("structColumn"))
    println("Structured data's dataframe...")
    structDf.show()

    // Creating the view from dataframe having struct datatype
    structDf.createOrReplaceTempView("structView")

    println("Printing the schema of structDf")
    structDf.printSchema()
    println("Column of structColumn using dataframe and view")
    structDf.select("structColumn.carName").show()
    structDf.select("structColumn.*").show()
    localSpark.sql("select structColumn.carName from structView").show()


    // Creating the dataframe with arrayColumn
    println("Dataframe with array column")
    val arrayDf = carDf.select(functions.split(col("carName"), " ").alias("arrayColumn"))
    arrayDf.show()

    // Creating the view with arrayDf
    println("Creating a view from arrayDf")
    arrayDf.createOrReplaceTempView("arrayDfView")

    // Accessing the first element of arrayColumn
    println("Accessing the first element of arrayColumn")
    arrayDf.select(expr("arrayColumn[0]")).show(5)
    localSpark.sql("select arrayColumn[0] from arrayDfView").show(5)

    // Size of Array
    println("Size of Array")
    arrayDf.select(functions.size(expr("arrayColumn"))).show()
    localSpark.sql("select size(arrayColumn) from arrayDfView").show()

    // Creating the array column using two columns by array method
    println("Creating the array column using two columns by array method")
    val newArrayDf = carDf.select(array(col("carName"), col("carBody")))
    newArrayDf.show(5)


    // Exploring the explode
    val newCarDf = carDf.select(split(col("carName"), " ").alias("newColumnArray"), col("carBody"))
    newCarDf.createOrReplaceTempView("newCarDfView")
    newCarDf.show(5)
    localSpark.sql("select * from newCarDfView limit 5").show()


    val skillDf = localSpark.read.format("json").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\skills.json")
    println("Schema of skillDf")
    skillDf.printSchema()
    skillDf.show()

    skillDf.createOrReplaceTempView("skillDfView")

    println("Exploding the skillDf")
    skillDf.select(explode(col("skills")), col("name")).show()
    localSpark.sql("select explode(skills), name from skillDfView").show()


    // Exploring Json Data
    val countryDf = localSpark.range(1).selectExpr("""'{"India":{"Tier1":["Mumbai","Banglore","Delhi"]}}' as jsonColumn""")
    countryDf.show()
    countryDf.collect()

    // Accessing the data of India key
    println("Accessing the value of key India using json_tuple(extracted only single level of nesting")
    countryDf.select(json_tuple(col("jsonColumn"), "India") as "extractedIndia").show()

    // Accessing nexted element of json column using get_json_object
    println("Accessing nexted element of json column using get_json_object")
    countryDf.select(get_json_object(col("jsonColumn"), "$.India") as "India Data").show()
    countryDf.select(get_json_object(col("jsonColumn"), "$.India.Tier1") as "India Data Tier Values").show()
    countryDf.select(get_json_object(col("jsonColumn"), "$.India.Tier1[0]") as "India Data Tier first Value").show()


    // Date and Timestamp Dataframe
    println("Date and Timestamp Dataframe")
    val dateTimeDf = carDf.select(col("carName"), col("carBody"), current_date() as "todaysDate", current_time() as "todaysTime")
    dateTimeDf.show()
    dateTimeDf.createOrReplaceTempView("dateTimeView")

    // Adding and subtracting the date
    println("Adding and subtracting the date ")
    dateTimeDf.select(date_add(col("todaysDate"), 365) as "addedDate", date_sub(col("todaysDate"), 365) as "subtractedDate").show()
    localSpark.sql("select date_add(todaysDate, 365) as addedDate , date_sub(todaysDate, 365) as subtractedDate from dateTimeView ").show()

    println("Extracting date from timestamp column")
    dateTimeDf.select(to_date(to_timestamp(col("todaysTime"))) as "extractedDate").show()


    // User Defined Functions
    val premiumDf = carDf.selectExpr("compressionratio as Premium")
    premiumDf.show
    premiumDf.createOrReplaceTempView("premiumView")

    // Defining the UDF
    def returnOnInvestment(x: Double): Double = x * 5 / 3

    // Register the UDF in Spark
    val returnOnInvestmentUDF = udf(returnOnInvestment(_: Double): Double)

    // Using the UDF
    premiumDf.select(col("premium"), returnOnInvestmentUDF(col("premium")).alias("roi")).show()

    localSpark.udf.register("returnOnInvestmentUDFSQL", returnOnInvestment(_: Double): Double)
    localSpark.sql("select premium, returnOnInvestmentUDFSQL(premium) from premiumView").show()


    var input = "dummy"
    while (input.nonEmpty) input = StdIn.readLine()


  }
}
