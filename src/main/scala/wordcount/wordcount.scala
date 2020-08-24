package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val file = s"src/main/resources/wordcount/wordcount.txt"

    //base rdd
    val lines = sc.textFile(file, 2)

    //transformed rdd
    val words = lines.flatMap(line => line.split(" "))


    //action1
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
    counts.foreach(println)

    //action2
    val countHello = words.filter(word => word.contains("soul")).count
    println(s"word count= $countHello")

    sc.stop()

  }

}
