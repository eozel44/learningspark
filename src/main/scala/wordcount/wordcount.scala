package wordcount

import org.apache.spark.sql.SparkSession

/***
 * Spark Driver
 * As the part of the Spark application responsible for instantiating a SparkSession,
 * the Spark driver has multiple roles:
 *   it communicates with the cluster manager;
 *   it requests resources (CPU, memory, etc.) from the cluster manager for Spark’s executors(JVMs);
 *   it transforms all the Spark operations into DAG computations, schedules them,and distributes their execution as tasks across the Spark executors.
 *   Once the resources are allocated, it communicates directly with the executors.
 *
 */

/***
 * SparkSession
 *
 * In Spark 2.0, the SparkSession became a unified conduit to all Spark operations and data.
 * Not only did it subsume previous entry points to Spark like the SparkContext, SQLContext, HiveContext,
 * SparkConf, and StreamingContext, but it also made working with Spark simpler and easier.
 *
 * Through this one conduit, you can create JVM runtime parameters, define Data‐Frames and Datasets,
 * read from data sources, access catalog metadata, and issue Spark SQL queries.
 * SparkSession provides a single unified entry point to all of Spark’s functionality.
 *
 */

/***
 * Cluster Manager
 * The cluster manager is responsible for managing and allocating resources for
 * the cluster of nodes on which your Spark application runs.
 * Currently, Spark supports four cluster managers: the built-in standalone cluster manager, Apache Hadoop
 * YARN, Apache Mesos, and Kubernetes.
 *
 */

/***
 * Spark executor
 * A Spark executor runs on each worker node in the cluster.
 * The executors communicate with the driver program and are responsible for executing tasks on the workers.
 * In most deployments modes, only a single executor runs per node.
 *
 */

/***
 * Deployment modes
 * An attractive feature of Spark is its support for myriad deployment modes, enabling
 * Spark to run in different configurations and environments.
 * Because the cluster manager is agnostic to where it runs (as long as it can manage Spark’s executors and
 * fulfill resource requests), Spark can be deployed in some of the most popular environments—such as
 * Apache Hadoop YARN and Kubernetes—and can operate in different modes.
 *
 */

object wordcount {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .master("local[2]")
      .getOrCreate()

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
