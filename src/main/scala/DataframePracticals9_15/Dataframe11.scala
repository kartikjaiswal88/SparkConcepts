package DataframePracticals9_15

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/*
 1. Spark Submit
      sbt clean package && spark-submit --class DataframePracticals9_15.Dataframe11 --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar

 */

object Dataframe11 {
  val localSpark: SparkSession = SparkSession.builder().appName("DataframeDay11").master("local[*]").getOrCreate()
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


    // List down the car names where car price is greater than the 20,000
    println("List down the car names where car price is greater than the 20,000")
    carDf.where("Price > 20000").select("carName", "Price").distinct().show()
    localSpark.sql("select distinct(carName), Price from carDfView where Price > 20000").show()


    // List down the average milage of each car by hightwaympg and citympg
    println("Average milage of each car through citympg and highwaympg")
    carDf.selectExpr("carName", "citympg", "highwaympg", "((citympg + highwaympg)/2) as averageMpg").show()

    println("Alternative way")
    val averageMpg = (col("citympg") + col("highwaympg")) / 2
    carDf.select(col("carName"), col("citympg"), col("highwaympg"), averageMpg.alias("AverageMilage")).show()

    println("Average milage using the view")
    localSpark.sql("select carName, citympg, highwaympg, ((citympg + highwaympg)/2) as AverageMpg from cardfview").show()


    // Average price of all the cars
    println("Average price of all the cars:")
    carDf.select(avg("price")).show()

    // Describe the column
    println("Count, Standard Deviation, Min, Max of price")
    carDf.select("price").describe().show()


    // Upper, lower and camel-case
    println("Upper , lower and camel-case")
    carDf.select(upper(col("carName")), lower(col("carName")), initcap(col("carName"))).show()
    localSpark.sql("""select upper(carName), lower(carName), initcap(carName) from cardfView """).show()


    // Trim, ltrim, rtrim
    println("Trim, ltrim, rtrim")
    carDf.select(trim(lit("       car           ")).alias("trim")).show()
    carDf.select(ltrim(lit("            car       ")).alias("ltrim")).show()
    carDf.select(rtrim(lit("          car          ")).alias("rtrim")).show()


    // Regular Expressions
    val extractedString = "(audi|bmw|ford)"
    println("Printing the extracted data.......")
    carDf.select((regexp_extract(col("carName"), extractedString, 1)).alias("extractedData"), col("carName")).show()
    localSpark.sql("""select regexp_extract(carName, '(audi|bmw|ford)', 1) as extracted, carName from cardfView""").show()


    // Contains
    println("Using contains method")
    carDf.withColumn("isAudi", col("carName").contains("audi")).select("isAudi", "carName").show()
    carDf.withColumn("isAudi", col("carName").contains("audi").or(col("carName").contains("bmw")))
      .select(col("isAudi"), col("carName")).show()


    // Replace string
    val replaceString = "(audi|bmw|ford)"
    println("Printing the replaced data.......")
    carDf.select((regexp_replace(col("carName"), replaceString, "High Class Car")).alias("replacedData"), col("carName")).show()
    localSpark.sql("""select regexp_replace(carName, '(audi|bmw|ford)', 1) as replacedData, carName from cardfView""").show()
    carDf.withColumn("replacedData", regexp_replace(col("carName"), replaceString, "Experiment")).select("carName", "replacedData").show()


    /* Schema does not match -string/int => null */
    //   if any column in a given row is a null, entire row will be dropped
    carDf.na.drop("any").count()
    carDf.na.drop("any", Seq("carName", "carBody")).count()

    // If all the column having null value, then row will be dropped
    carDf.na.drop("all").count()


    // Fill the null with dummy values
    carDf.na.fill("Replacing String Null values").count()
    carDf.na.fill(5).count()
    carDf.na.fill(5.5).count()
  }
}
