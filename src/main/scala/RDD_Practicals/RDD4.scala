package RDD_Practicals

object RDD4 {
  val paragraphs = Array(
    "Scala is a modern programming language that combines object-oriented and functional programming. It is concise, elegant, and powerful, making it ideal for building high-performance applications on the JVM.",
    "Apache Spark is a distributed computing engine that allows you to process large-scale data quickly and efficiently. It supports Scala, Java, Python, and R for writing big data applications.",
    "Functional programming promotes immutability and pure functions, which help in writing robust, predictable, and testable code. Scala makes functional programming more accessible with its expressive syntax.",
    "In data engineering, tools like Apache Spark, Kafka, and Hive are crucial for building scalable ETL pipelines. Scala is often used due to its seamless integration with Spark APIs.",
    "The JVM provides a robust and mature runtime environment. Scala and Java both compile to JVM bytecode, allowing you to use Java libraries within Scala projects without any extra configuration."
  )

  def main(args: Array[String]): Unit = {
    val rddOfParagraph = SparkEngine.SparkObject.localSpark.sparkContext.parallelize(paragraphs, 2)

    val rddOfWords = rddOfParagraph.flatMap(word => word.split(" "))
    val keyValueRDD = rddOfWords.map(word => (word, word.length))
    println("Key-Value RDD using map:" + keyValueRDD.collect().mkString(", "))

    // lookup: Give the value of particular key from RDD
    println("Value of like is:" + keyValueRDD.lookup("like").mkString(" "))
    println("Value of is is:" + keyValueRDD.lookup("is").mkString(" "))


    val charRDD = rddOfWords.flatMap(word => word.toLowerCase.toSeq)
    println("Characters of RDD is:" + charRDD.collect().mkString(" "))
    val keyValueCharRDD = charRDD.map(char => (char, 1))
    println("Key value RDD of Characters:" + keyValueCharRDD.collect().mkString(" "))


    // countByKey:Gives the count of each key
    println("Count of every key is:" + keyValueCharRDD.countByKey())

    // countByKeyApprox: Gives the count of each key in particular time with confidence(default time is 100 ms and conficdence 0.95)
    println("Count of every key By countByApprox:" + keyValueCharRDD.countByKeyApprox(10, .6))


    // groupByKey: Gives the key and iterator to each element
    //    val groupedRdd = keyValueCharRDD.groupByKey().map(keyWithIterator => (keyWithIterator._1, keyWithIterator._2.reduce(additionFunction)))
    val groupedRdd = keyValueCharRDD.groupByKey().map(keyWithIterator => (keyWithIterator._1, keyWithIterator._2.reduce(_ + _)))
    println(s"GroupByKey value:${groupedRdd.collect().mkString("Array(", ", ", ")")}")

    // reduceByKey: Gives the key and value after reducing it
    println(s"Count of each key using reduceByKey:${keyValueCharRDD.reduceByKey(additionFunction).collect().mkString(", ")}")


    val numbers = SparkEngine.SparkObject.localSpark.sparkContext.parallelize(1 to 20, 4)

    // Aggregate: Aggregate the values of RDD(first at element wise and then partition wise)
    println(s"Aggregating the values of RDD using aggregate:${numbers.aggregate(0)(_ + _, _ + _)}")

    // TreeAggregate: Aggregate the values of RDD, first at element wise and then partition wise where partition wise
    //                aggregation result is passed to another executor to avoid OOM
    println(s"Aggregating the values of RDD using treeAggregate:${numbers.treeAggregate(0)(_ + _, _ + _, 3)}")

    // AggregateByKey: Aggregate the values of RDD on the basis of key
    println(s"Aggregating the values of RDD  using aggregate:${numbers.aggregate(0)(_ + _, _ + _)}")

    // Cogroup: It joins two RDDs and returns the key with two iterators of RDD
    println(s"Co grouping two RDDs using cogroup:${keyValueCharRDD.cogroup(keyValueCharRDD).collect().mkString(", ")}")

    // Joins: It joins two RDDs and returns the key with values of both the RDDs
    println(s"Joining two RDDs using joins:${keyValueCharRDD.join(keyValueCharRDD).collect().mkString(", ")}")

    // LeftOuterJoin: It joins two RDDs and returns the key with values of both the RDDs as left join
    println(s"Joining two RDDs using LeftOuterJoin:${keyValueCharRDD.leftOuterJoin(keyValueCharRDD).collect().mkString(", ")}")

    // FullOuterJoin: It joins two RDDs and returns the key with values of both the RDDs as full join
    println(s"Joining two RDDs using FullOuterJoin:${keyValueCharRDD.fullOuterJoin(keyValueCharRDD).collect().mkString(", ")}")

    // RightOuterJoin: It joins two RDDs and returns the key with values of both the RDDs as right join
    println(s"Joining two RDDs using joins:${keyValueCharRDD.rightOuterJoin(keyValueCharRDD).collect().mkString(", ")}")


  }

  def additionFunction(x: Int, y: Int): Int = x + y

}
