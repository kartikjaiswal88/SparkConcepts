package RDD_Practicals

import SparkEngine.SparkObject
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.DISK_ONLY

import scala.io.StdIn

object RDD2 {
  val paragraphs = Array(
    "Scala is a modern programming language that combines object-oriented and functional programming. It is concise, elegant, and powerful, making it ideal for building high-performance applications on the JVM.",
    "Apache Spark is a distributed computing engine that allows you to process large-scale data quickly and efficiently. It supports Scala, Java, Python, and R for writing big data applications.",
    "Functional programming promotes immutability and pure functions, which help in writing robust, predictable, and testable code. Scala makes functional programming more accessible with its expressive syntax.",
    "In data engineering, tools like Apache Spark, Kafka, and Hive are crucial for building scalable ETL pipelines. Scala is often used due to its seamless integration with Spark APIs.",
    "The JVM provides a robust and mature runtime environment. Scala and Java both compile to JVM bytecode, allowing you to use Java libraries within Scala projects without any extra configuration."
  )

  def main(args: Array[String]): Unit = {

    val paragraphRDD = SparkObject.localSpark.sparkContext.parallelize(paragraphs, 2)

    val rddOfParagraphWords = paragraphRDD.flatMap(para => para.split(" "))
    println("RDD with flatMap:")
    println(rddOfParagraphWords.collect().mkString(" "))


    // Sort
    val sortedRDD = rddOfParagraphWords.sortBy(word => word.length)
    println("Sorted RDD:")
    println(sortedRDD.collect().mkString(" "))


    // Random Split
    val trainTestRDD = rddOfParagraphWords.randomSplit(Array[Double](0.7, 0.3))
    println("TRAIN DATA:" + trainTestRDD(0).collect().mkString(" "))
    println("TEST DATA:" + trainTestRDD(1).collect().mkString(" "))

    actions(paragraphRDD)


  }


  def actions(paragraphRDD: RDD[String]) = {
    println("Executing the actions.................")

    // collect: returns data from executor to the driver
    println(paragraphRDD.collect().mkString(" "))

    // reduce: It takes next element and add/subtract to the result
    val rangeRDD = SparkObject.localSpark.sparkContext.parallelize(1 to 10)
    val sumTillTen = rangeRDD.reduce(_ + _)
    println("Printing the sum till 10:" + sumTillTen)


    // Program to find the smallest word in RDD
    val mappedRDD = paragraphRDD.map(word => word.length)
    println("Printing the mapped RDD:")
    println(mappedRDD.collect().mkString(", "))

    val flatmappedRDD = paragraphRDD.flatMap(para => para.split(" "))
    println("Printing the flatmapped RDD:")
    println(flatmappedRDD.collect().mkString(" "))

    val flatmappedRDDWithWordLength = flatmappedRDD.map(word => (word, word.length))
    println("Printing each word with it's length:")
    println(flatmappedRDDWithWordLength.collect().mkString(" "))

    val smallestWord = flatmappedRDDWithWordLength.reduce((leftWord, rightWord) => if (leftWord._2 < rightWord._2) leftWord else rightWord)
    println(s"Smallest word is:${smallestWord._1} of length:${smallestWord._2}")
    // We can achieve same by coding a function
    println(s"Printing the smallest word:${flatmappedRDD.reduce(smallest)}")


    // count: Gives the no. of elements in RDD
    println("No. of elements in flatmappedRDD is:" + flatmappedRDD.count())

    // countApprox: Gives the range of no. of elements in RDD in particular milli seconds(default 95% confidence)
    println("No. of elements in flatmappedRDD in approx timeout is::" + flatmappedRDD.countApprox(1, .9))

    // countByValue: Gives the count of no. of particular element in RDD
    println("Count. of elements in flatmappedRDD is:" + flatmappedRDD.countByValue())

    // countByValueApprox: Gives the count of range of no. of elements in RDD in particular milli seconds(default 95% confidence)
    println("Count. of elements in flatmappedRDD in approx timeout is::" + flatmappedRDD.countByValueApprox(100, .9))

    // first: Gives the first element in RDD
    println("First element in flatmappedRDD is:" + flatmappedRDD.first())

    val rangeTillTen = SparkObject.localSpark.sparkContext.parallelize(1 to 10)

    // Max: Gives the maximum element in RDD
    println("Max element in flatmappedRDD is:" + rangeTillTen.max())

    // Min: Gives the maximum element in RDD
    println("Min element in flatmappedRDD is:" + rangeTillTen.min())

    // Take: Give the first n elements
    val sampleRDD = SparkObject.localSpark.sparkContext.parallelize(List(3, 4, 33, 22, 66, 33, 22, 74, 335, 552, 6, 3))
    println("Taking first 5 elements:" + sampleRDD.take(5).mkString(" "))

    // TakeOrdered: Give first sorted n numbers
    println("Taking ordered 5 elements:" + sampleRDD.takeOrdered(5).mkString(" "))

    // Top: Give first n highest numbers
    println("Top first n numbers are:" + sampleRDD.top(5).mkString(" "))

    // TakeSample: Give n elements with replacement parameter(element can come more times than actual occurence)
    //             with seed(to get same elements)
    println("Result of takeSample:" + sampleRDD.takeSample(true, 5, 9).mkString(" "))

    // SaveAsTextFile: Save rdd to a text file
    //  println("Saving RDD as text file:" + sampleRDD.saveAsTextFile("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\Learning Projects\\LearningMain\\src\\main\\resources\\RDD"))


    // Cache: Keep the executed transformation result in memory only
    cache()

    // Persist: Keep the executed transformation result in memory, disk, or memory and dist as per the parameter (default is memory)
    persist()


    var continue = true
    while (continue) {
      val input = StdIn.readLine("Enter something (press 'q' to quit): ")
      if (input.trim.toLowerCase == "q") {
        continue = false
      } else {
        println(s"You entered: $input")
      }
    }
  }

  def smallest(leftWord: String, rightWord: String) = {
    if (leftWord.length > rightWord.length) rightWord
    else leftWord
  }

  def cache() = {
    val rddOfParagraph = SparkObject.localSpark.sparkContext.parallelize(paragraphs)

    val rddOfWords = rddOfParagraph.flatMap(word => word.split(" "))

    rddOfWords.persist()

    val firstFiveWords = rddOfWords.take(5)
    val firstFiveHighestWords = rddOfWords.top(5)

    println("First five words:")
    println(firstFiveWords.mkString(" "))
    println("First five Highest words:")
    println(firstFiveHighestWords.mkString(" "))

  }

  def persist() = {
    val rddOfParagraph = SparkObject.localSpark.sparkContext.parallelize(paragraphs)

    val rddOfWords = rddOfParagraph.flatMap(word => word.split(" "))

    rddOfWords.persist(DISK_ONLY)

    val firstFiveWords = rddOfWords.take(5)
    val firstFiveHighestWords = rddOfWords.top(5)

    println("First five words:")
    println(firstFiveWords.mkString(" "))
    println("First five Highest words:")
    println(firstFiveHighestWords.mkString(" "))

  }


}
