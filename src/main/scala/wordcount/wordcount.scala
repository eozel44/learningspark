package wordcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object wordcount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext
    val file = s"src/main/resources/wordcount/wordcount.txt"

    //base rdd
    val lines = spark.sparkContext.textFile(file, 2)

    //transformed rdd
    val words = lines.flatMap(line => line.split(" "))


    //action1
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
    counts.foreach(println)

    //action2
    val countHello = words.filter(word => word.contains("soul")).count
    println(s"word count= $countHello")

    spark.stop()

  }

}
