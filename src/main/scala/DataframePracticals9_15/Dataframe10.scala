package DataframePracticals9_15

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, expr, lit}


/*
   1. Spark Submit
      sbt clean package && spark-submit --class DataframePracticals9_15.Dataframe10 --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar

 */

object Dataframe10 {
  val localSpark: SparkSession = SparkSession.builder().appName("DataframeDay10").master("local[*]").getOrCreate()
  localSpark.sparkContext.setLogLevel("ERROR")


  def main(args: Array[String]): Unit = {
    val storeDf = localSpark.read.format("json").load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\store_locations.json")

    // Creating view of storeDf
    storeDf.createOrReplaceTempView("storeDfView")

    // Quering the data
    localSpark.sql("""select city from storeDfView""").show(5)

    // Creating df directly from the data
    import localSpark.implicits._
    val studentDf = Seq((1, "name A", "std 5"), (2, "name B", "std 6")).toDF("RollNo", "Name", "Std")
    studentDf.show()

    // Quering a column from Dataframe and View
    println("Printing the city column from Dataframe")
    storeDf.select("city").show()
    storeDf.select(storeDf.col("city")).show()
    storeDf.select(expr("city")).show()

    println("Printing the city column from View")
    localSpark.sql("""select city from storeDfView""").show(5)


    // Alias in Dataframe
    println("Aliasing the city column to city_name")
    storeDf.select(expr("city").alias("city_name")).show()
    storeDf.select(expr("city as city_name")).show()

    // Alias in SQL view
    localSpark.sql("select city as city_name_view from storeDfView").show()


    // Giving fixed value
    println("Giving fixed value in Dataframe")
    storeDf.select(expr("*"), lit("India").alias("Country")).show()

    println("Giving fixed value in Dataframe  using withcolumn method")
    storeDf.withColumn("Village", lit("Babron")).show()

    println("Giving fixed value in View")
    localSpark.sql("""select *, "India" as country from storeDfView""").show()

    println("Adding column with value evaluated from the expression")
    storeDf.withColumn("is_Antioch", expr("city == 'Antioch'")).show()


    // Exercise: Create a dataframe of lenght and breadth and calculate the area by adding column
    val metricsDf = Seq((2, 5), (3, 8), (9, 2), (3, 1), (5, 3)).toDF("Length", "Breadth")
    println("Printing the metrics DF")
    metricsDf.show()

    println("Printing the metrics df with area")
    metricsDf.withColumn("Area", expr("Length * Breadth")).show()


    // withColumnRenamed - alias
    println("Renaming the column using withColumnRenamed")
    storeDf.withColumnRenamed("city", "city name").show()


    val storeDfWithSpace = storeDf.withColumnRenamed("city", "city name")
    val storeDfWithSpaceView = storeDfWithSpace.createOrReplaceTempView("storeDfWithSpaceView")

    localSpark.sql("""select `city name` from storeDfWithSpaceView""").show()


    // Drop the column from Dataframe
    println("Printing the df after dropping city column")
    storeDf.drop("city").show()

    println("Printing the df after dropping city and state column")
    storeDf.drop("city", "state").show()


    // Changing the datatype - Type Casting
    storeDf.printSchema()
    println("Changint the data type of zip_code as integer")
    storeDf.withColumn("zip_code", col("zip_code").cast("int")).printSchema()
    storeDf.withColumn("zip_code_int", col("zip_code").cast("int")).printSchema()


    // WHERE and FILTER
    println("Using the Where for city column")
    storeDf.where("city == 'Woodland'").show()

    println("Using the Filter for city column")
    storeDf.filter("city == 'Woodland'").show()

    println("Using the Where in View")
    localSpark.sql("select * from storeDfView where city == 'Woodland'").show()


    // Dataframe - and
    println("Using the and in Dataframe")
    storeDf.where("""city == "Woodland"""").where("state == 'CA'").show()

    // View - and
    println("Using the and in View")
    localSpark.sql("select * from storeDfView where city = 'Woodland' and state = 'CA'").show()



    // Distinct Values and Count in Dataframe
    println("Printing the distinct values of state column")
    storeDf.select("state").distinct().show(100)

    println("Printing the count of state column")
    storeDf.select("state").count()


    // Distinct Values and Count in View
    println("Printing the distinct state using view")
    localSpark.sql("""select distinct(state) from storeDfView""").show()

    println("Printing the count of distinct state from storeDfView")
    localSpark.sql("""select count(distinct(state)) from storeDfView""").show()


    // Sampling(withReplacement, fraction, seed)
    println("Printing the sample data")
    storeDf.sample(withReplacement = true, .2, 40).show()


    // Split-ML: Train and Test dataset
    val trainTestData = storeDf.randomSplit(Array(0.7, 0.3), 30)
    println("Printing the trained data")
    trainTestData(0).show()
    println("Printing the test data")
    trainTestData(1).show()


    // Union: Append the data to a dataframe
    val storeAntioch = storeDf.filter("city == 'Antioch'")
    val storeWoodland = storeDf.filter("city == 'Woodland'")

    val storeAntLand = storeAntioch.union(storeWoodland)
    println("Printing the data by union of Antioch and Woodland")
    storeAntLand.show()


    // OrderBy
    println("Printing the dfs using orderBy")
    storeDf.orderBy("city").show(5)
    storeDf.orderBy(expr("city desc")).show(5)
    storeDf.orderBy(desc("city")).show(5)

    localSpark.sql("""select * from storeDfView order by city""").show()


    // Repartition: Complete shuffle - so heavy and increase or decrease the number of partitions
    // Coalesce: Avoid shuffle - fast and decrease the number of partitions
    println("Repartitioning the storeDf and printing the number of partitions:")
    val partitionedDf = storeDf.repartition(5)
    println(partitionedDf.rdd.getNumPartitions)
    println(storeDf.repartition(3, col("city")).rdd.getNumPartitions)

    println("Coalescing the partitionedDf and printing the number of partitions:")
    println(partitionedDf.coalesce(2).rdd.getNumPartitions)


    println("Program is running...")
    println("Press ENTER to stop")

    scala.io.StdIn.readLine()

    println("Program stopped")

  }


}




















