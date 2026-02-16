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
    println("Count of every key is:"+keyValueCharRDD.countByKey())

    // countByKeyApprox: Gives the count of each key in particular time with confidence(default time is 100 ms and conficdence 0.95)
    println("Count of every key By countByApprox:"+keyValueCharRDD.countByKeyApprox(10, .6))
  }

}
