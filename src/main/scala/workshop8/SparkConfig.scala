package workshop8

import org.apache.spark.sql.SparkSession

object SparkConfig {

  def printConfigs(session: SparkSession) = {
    // get conf
    val mconf = session.conf.getAll
    // print them
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }

  /***
   * bin/spark-submit --conf spark.sql.shuffle.partitions=5 --conf "spark.executor.memory=2g" --class workshop2.workshop2 /home/eren/bigdata/cases/workshop/learningspark/target/scala-2.12/learningspark_2.12-0.1.jar
   *
   */

  def main(args: Array[String]): Unit = {
    // create a session
    val spark = SparkSession.builder
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .master("local[*]")
      .appName("SparkConfig")
      .getOrCreate()

    printConfigs(spark)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    spark.conf.set("spark.sql.shuffle.partitions",  spark.sparkContext.defaultParallelism)
    printConfigs(spark)
    spark.stop()
  }

}
