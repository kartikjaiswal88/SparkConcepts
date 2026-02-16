package RDD_Practicals


import SparkEngine.SparkObject


object RDD1 {
  SparkObject.localSpark.sparkContext.setLogLevel("ERROR")


  def main(args: Array[String]): Unit = {
    val cars = Array("a", "b", "c") // Single Machine Data Structure
    println(cars.mkString(", "))

    val carsRDD = SparkObject.localSpark.sparkContext.parallelize(cars, 2) // RDD with two partitions
    println("RDD contents: " + carsRDD.collect().mkString(", ")) // Distributed Data Structure


    /** ****************************************  Transformations  ******************************************* */
    val fruits = Array("Mango", "Apple", "Grapes", "Banana", "Mango")
    val fruitsRDD = SparkObject.localSpark.sparkContext.parallelize(fruits, 2)

    // distinct
    val distinctFruitsRDD = fruitsRDD.distinct()
    println("Distinct RDD: " + distinctFruitsRDD.collect().mkString(", "))


    // filter
    // val filterFruitsRDD = fruitsRDD.filter(fruit => fruit.startsWith("A"))
    val filterFruitsRDD = fruitsRDD.filter(_.startsWith("A"))
    println("Filtered RDD: " + filterFruitsRDD.collect().mkString(", "))

    val numbersRDD = SparkObject.localSpark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    println("Even numbers:" + numbersRDD.filter(_ % 2 == 0).collect().mkString(", ")) // Best Practice would be to define the function
    println("Odd numbers:" + numbersRDD.filter(_ % 2 != 0).collect().mkString(", "))

    def evenFilter(x: Int) = x % 2 == 0

    def oddFilter(x: Int) = x % 2 != 0

    println("Even numbers:" + numbersRDD.filter(evenFilter).collect().mkString(", "))
    println("Odd numbers:" + numbersRDD.filter(oddFilter).collect().mkString(", "))


    // map
    val mapFruitRDD = fruitsRDD.map(fruit => (fruit, fruit.startsWith("M")))
    println("Mapped RDD:" + mapFruitRDD.collect().mkString(", "))
    println("RDD with True Value:" + mapFruitRDD.filter(fruit => fruit._2 == true).collect().mkString(", "))

    val fruitWithLength = fruitsRDD.map(fruit => (fruit, fruit.length))
    println("Fruits with Length" + fruitWithLength.collect().mkString(", "))


    // flatmap
    val numberRDD = Array("1,2,3", "4,5,6", "7,8,9")
    println("NumberArray with Map: " + numberRDD.map(x => x.split(",")).mkString(", "))
    println("NumberArray with FlatMap: " + numberRDD.flatMap(x => x.split(",")).mkString(", "))

    val paragraphs = Array(
      "Scala is a modern programming language that combines object-oriented and functional programming. It is concise, elegant, and powerful, making it ideal for building high-performance applications on the JVM.",
      "Apache Spark is a distributed computing engine that allows you to process large-scale data quickly and efficiently. It supports Scala, Java, Python, and R for writing big data applications.",
      "Functional programming promotes immutability and pure functions, which help in writing robust, predictable, and testable code. Scala makes functional programming more accessible with its expressive syntax.",
      "In data engineering, tools like Apache Spark, Kafka, and Hive are crucial for building scalable ETL pipelines. Scala is often used due to its seamless integration with Spark APIs.",
      "The JVM provides a robust and mature runtime environment. Scala and Java both compile to JVM bytecode, allowing you to use Java libraries within Scala projects without any extra configuration."
    )

    val paragraphRDD = SparkObject.localSpark.sparkContext.parallelize(paragraphs, 2)

    val rddOfParagraphWords = paragraphRDD.flatMap(para => para.split(" "))
    println("RDD with flatMap:")
    println(rddOfParagraphWords.collect().mkString(" "))

    val pairedRDD = rddOfParagraphWords.map(element => (element, 1))
    println("Mapped RDD:")
    println(pairedRDD.collect().mkString(""))

    // Thus the number of times occured
    println(pairedRDD.reduceByKey(_ + _).collect().mkString(" "))


  }
}
