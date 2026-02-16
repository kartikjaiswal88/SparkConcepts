package RDD_Practicals

import SparkEngine.SparkObject

import java.io.{File, PrintWriter}
import scala.io.StdIn
import scala.util.Random

object RDD3 {
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

    //Checkpoint: Copy the content into the disk
    //    SparkObject.localSpark.sparkContext.setCheckpointDir("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\Learning Projects\\LearningMain\\src\\main\\resources\\CheckpointDir")
    //    rddOfParagraphWords.checkpoint()
    rddOfParagraphWords.collect()


    //    Pipe: Execute the linux command(won't run on windows environment)
    //    println("Executing linux command:", rddOfParagraphWords.pipe("wc -l").collect())

    // mapPartitions: Does the transformation on the partition level
    println(paragraphRDD.mapPartitions(part => Iterator[Int](1)).sum())


    //mapPartitionWithIndex: Does the transformation on partition level and return index also with iterator
    val mappedPartitionIndexRdd = rddOfParagraphWords.mapPartitionsWithIndex(indexFun)
    println(mappedPartitionIndexRdd.collect().mkString(""))


    // forEachPartions:Does the transformation on partition level and return the iterator
    //  val result = rddOfParagraphWords.foreachPartition(forFunc)


    // glom: Writes content of RDD into an Array partition level
    println("Printing the content of RDD as Array" + rddOfParagraphWords.glom().collect().mkString(" "))


    // Key-Pair RDD
    // map: Map every element of the RDD
    println("Key-Value RDD by Map:" + rddOfParagraphWords.map(word => (word, word.length)).collect().mkString(" ,"))


    // KeyBy: Map every element of the RDD with defined key
    println("Key-Value RDD by KeyBy:" + rddOfParagraphWords.keyBy(word => word.length).collect().mkString(" ,"))


    // mapValues: Modifies the value of key-value RDD
    val keyValueRDD = rddOfParagraphWords.keyBy(word => word.length)
    println("Modifying the value of Key-value RDD with mapValues:" + keyValueRDD.mapValues(value => value.toUpperCase).collect().mkString(", "))


    // flatMapValues: Modifies the value of key-value RDD and separates each element of RDD i.e. flattens
    println("Modifying the value of Key-value RDD with flatMapValues:" + keyValueRDD.flatMapValues(value => value.toUpperCase).collect().mkString(", "))


    // Keys: Gives all the keys of Key-Value RDD
    println("All the keys of RDD using Keys:" + keyValueRDD.keys.collect().mkString(" ,"))

    // Values: Gives all the Values of Key-Value RDD
    println("All the values of RDD using Values:" + keyValueRDD.values.collect().mkString(" ,"))

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

  def indexFun(i: Int, wordInIterator: Iterator[String]) = {
    wordInIterator.toList.map(word => s"Partition $i => $word").iterator
  }

  def forFunc(wordInIterator: Iterator[String]): Unit = {
    var randomFileName = new Random().nextInt()
    var printWriter = new PrintWriter(new File(s"C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\Learning Projects\\LearningMain\\src\\main\\resources\\$randomFileName.txt"))

    while (wordInIterator.hasNext) {
      printWriter.write(wordInIterator.next())
    }

    printWriter.close()
  }
}
