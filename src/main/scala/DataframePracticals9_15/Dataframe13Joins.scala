package DataframePracticals9_15

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, countDistinct, col}

import scala.io.StdIn


/*
 1. Spark Submit
      sbt clean package && spark-submit --class DataframePracticals9_15.Dataframe13Joins --master local[*] target\scala-2.13\sparkconcepts_2.13-0.1.0-SNAPSHOT.jar

 */

object Dataframe13Joins {

  val localSpark: SparkSession = SparkSession.builder().appName("DataframeDay11").master("local[*]").getOrCreate()

  import localSpark.implicits._

  localSpark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    // Creating the student and subject DF
    println("Creating the student and subject DF")
    val studentDf = Seq((1, "nameA"), (2, "nameB"), (3, "nameC"), (4, "nameD"), (5, "nameE")).toDF("rollNo", "name")
    val subjectDf = Seq((4, "Maths"), (5, "Science"), (5, "Geography"), (6, "History"), (7, "EVS"), (8, "Maths")).toDF("rollNo", "Subject")


    // Creating the View of Student and subject DF
    println("Creating the View of Student and subject DF")
    studentDf.createOrReplaceTempView("studentView")
    subjectDf.createOrReplaceTempView("subjectView")


    println("Student Data")
    studentDf.show()
    println("Subject Data")
    subjectDf.show()
    localSpark.sql("select * from studentView").show()


    // Inner Join
    println("Innner join of studentDf and subjectDf")
    val joinOn = studentDf.col("rollNo") === subjectDf.col("rollNo")
    studentDf.join(subjectDf, joinOn).show() // default is inner join


    println("Innner join of studentView and subjectView")
    localSpark.sql("select * from studentView as st join subjectView as sb on st.rollNo = sb.rollNo").show()

    println("LeftOuter join of studentDf and subjectDf")
    studentDf.join(subjectDf, joinOn, "left_outer").show()
    localSpark.sql("select * from studentView as st left outer join subjectView as sb on st.rollNo = sb.rollNo").show()

    println("RightOuter join of studentDf and subjectDf")
    studentDf.join(subjectDf, joinOn, "right_outer").show()
    localSpark.sql("select * from studentView as st right outer join subjectView as sb on st.rollNo = sb.rollNo").show()

    println("Full Outer join of studentDf and subjectDf")
    studentDf.join(subjectDf, joinOn, "outer").show()
    localSpark.sql("select * from studentView as st full outer join subjectView as sb on st.rollNo = sb.rollNo").show()

    println("Left Semi join of studentDf and subjectDf")
    studentDf.join(subjectDf, joinOn, "left_semi").show()
    localSpark.sql("select * from studentView as st left semi join subjectView as sb on st.rollNo = sb.rollNo").show()

    println("Left Anti join of studentDf and subjectDf")
    studentDf.join(subjectDf, joinOn, "left_anti").show()
    localSpark.sql("select * from studentView as st left anti join subjectView as sb on st.rollNo = sb.rollNo").show()

    println("Cross join of studentDf and subjectDf")
    studentDf.crossJoin(subjectDf).show()
    localSpark.sql("select * from studentView cross join subjectView ").show()

    println("Natural join of studentDf and subjectDf")
    println("Dataframe apis doesn't suppport the natural join")
    localSpark.sql("select * from studentView natural join subjectView ").show()
    println("Natural join automatically joins the views on rollNo(common column of both views)")


    println("Physical plan of inner join")
    studentDf.join(subjectDf, joinOn, "inner").explain()


    // Some more transformations and actions
    println("Some more transformations and actions")
    val carDf = localSpark.read.format("csv").option("inferschema", "true").option("header", "true")
      .load("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\scala\\DataframePracticals9_15\\car_price_practice_1000_records.csv")
    // Creating and Querying the view from Car Dataframe
    carDf.createOrReplaceTempView("carDfView")
    localSpark.sql("select * from carDfView").show()

    println("Count of Data")
    carDf.count()

    println("Showing the data")
    carDf.show(5)

    println("Collecting the data")
    println(carDf.collect().mkString("Array(", ", ", ")"))

    println("Count of distinct car names")
    carDf.select(countDistinct("carName")).show()
    localSpark.sql("""select count(distinct(carName)) from carDfView""").show()

    println("Approx Count of distinct car names")
    carDf.select(approx_count_distinct(("carName"), 0.39)).show()
    localSpark.sql("""select approx_count_distinct(carName, 0.39) from carDfView""").show()

    println("Maximum and Minimum of Horse Power")
    carDf.select(functions.max("horsepower"), functions.min("horsepower"), functions.sum("horsepower"), functions.avg("horsepower")).show()
    localSpark.sql("""select max(horsepower), min(horsepower), sum(horsepower), avg(horsepower) from carDfView""").show()

    println("Count of each carbody group")
    localSpark.sql("""select carbody, count(*) from carDfView group by carbody""").show()
    carDf.groupBy(col("carBody")).count().show()


    var input = "x"
    while (input.nonEmpty) input = StdIn.readLine()


  }


}
  