package workshop1

import org.apache.spark.sql.SparkSession

/** *
 * Deployment modes
 * An attractive feature of Spark is its support for myriad deployment modes, enabling
 * Spark to run in different configurations and environments.
 * Because the cluster manager is agnostic to where it runs (as long as it can manage Spark’s executors and
 * fulfill resource requests), Spark can be deployed in some of the most popular environments—such as
 * Apache Hadoop YARN and Kubernetes—and can operate in different modes.
 *
 */

object workshop1 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .master("local[2]")
      .getOrCreate()

    val file = s"src/main/resources/wordcount.txt"

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
