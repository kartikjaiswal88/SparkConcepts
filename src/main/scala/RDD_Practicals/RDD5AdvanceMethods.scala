package RDD_Practicals

import org.apache.spark.{HashPartitioner, Partitioner}

import java.util.Random

object RDD5AdvanceMethods {
  /*
   1. Zip: To make pair RDD
           Both RDD's should have same number of partitions.
           Both RDD's should have same number of elements.
   2. Coalesce: Only reduce number of partitions.
                Coalesce avoids reshuffling of data.
   3. Repartition: Reduce or increase the number of partitions
                   Repartition causes shuffling of data.
   4. Custom Partitions: There are two types of partitioner which are HashPartitioner and Partitioner
      a. Read data as dataframe
      b. Convert the dataframe into RDD
      c. Create Keyed RDD and apply partitioner
      d. Create dataframe from RDD



   */


  val spark = SparkEngine.SparkObject.localSpark

  def main(args: Array[String]): Unit = {

    // Zip
    val car = Array("Audi", "BMW", "Mercedes", "Suzuki",
      "Toyota", "Honda", "Hyundai", "Kia",
      "Ford", "Chevrolet", "Nissan", "Volkswagen",
      "Skoda", "Renault", "Tata", "Mahindra"
    )
    val carRdd = spark.sparkContext.parallelize(car, 4)
    println(s"Car RDD:${carRdd.collect().mkString(", ")}")

    val cost = Array(200000, 3000000, 264440, 600000,
      450000, 800000, 1200000, 950000,
      670000, 720000, 1500000, 2100000,
      1750000, 980000, 430000, 560000
    )
    val costRdd = spark.sparkContext.parallelize(cost, 4)
    println(s"Cost RDD:${costRdd.collect().mkString(", ")}")

    val zipRdd = carRdd.zip(costRdd)
    println(s"Pair RDD of carRdd and costRdd using zip is:${zipRdd.collect.mkString(", ")}")


    // Coalesce
    println(s"Number of partitions in carRdd is:${carRdd.getNumPartitions}")
    println("Reducing the number of partitions to 2 using coalesce")
    val coalesceCarRdd = carRdd.coalesce(2)
    println(s"Number of partitions in carRdd after coalesce is:${coalesceCarRdd.getNumPartitions}")

    // Repartition
    println(s"Number of partitions in carRdd is:${carRdd.getNumPartitions}")
    println("Increasing the number of partitions to 8 using Repartition")
    val repartitionsCarRdd = carRdd.repartition(8)
    println(s"Number of partitions in carRdd after repartition  is:${repartitionsCarRdd.getNumPartitions}")


    // Custom Partitions
    val flightDf = spark.read.option("header", "true").option("inferschema", "true").csv("C:\\Users\\karti\\OneDrive\\Desktop\\DataEngineering\\SparkLearning\\sparkconcepts\\src\\main\\resources\\flight_data.csv")
    flightDf.show(5)

    val rdd = flightDf.repartition(8).rdd
    val keyedRDD = rdd.keyBy(r => r(7))

    //    println("Printing the Key-Value RDD")
    //    println(keyedRDD.collect().mkString(", "))

    // HashPartitioners
    val hashPartitionedRDD = keyedRDD.partitionBy(new HashPartitioner(8)).glom()
    println("Printing the hashPartitioned RDD:")
    hashPartitionedRDD.collect().zipWithIndex.foreach { case (partitionData, partitionIndex) =>
      println(s"Partition $partitionIndex: ${partitionData.mkString(", ")}")
    }

    // Partitioners
    val customPartitionedRDD = keyedRDD.partitionBy(new customPart())
    println("Printing the customPartitioned RDD:")
    hashPartitionedRDD.collect().zipWithIndex.foreach { case (partitionData, partitionIndex) =>
      println(s"Partition $partitionIndex: ${partitionData.map(_._1).mkString(", ")}")
    }

  }


  class customPart extends Partitioner {
    override def numPartitions: Int = 4

    override def getPartition(key: Any): Int = {
      val country = key.toString
      if (country == "Australia")
        return 0
      else new Random().nextInt(2) + 1
    }

  }

}












