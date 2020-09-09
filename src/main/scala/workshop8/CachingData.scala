package workshop8

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


/***
 *
 * When to Cache and Persist
 * Common use cases for caching are scenarios where you will want to access a large
 * data set repeatedly for queries or transformations. Some examples include:
 * • DataFrames commonly used during iterative machine learning training
 * • DataFrames accessed commonly for doing frequent transformations during ETL or building data pipelines
 *
 * When Not to Cache and Persist
 * Not all use cases dictate the need to cache. Some scenarios that may not warrant cach‐
 * ing your DataFrames include:
 * • DataFrames that are too big to fit in memory
 * • An inexpensive transformation on a DataFrame not requiring frequent use,regardless of size
 *
 */

object CachingData {

  def printConfig(session: SparkSession, key:String) = {
    // get conf
    val v = session.conf.getOption(key)
    println(s"${key} -> ${v}\n")
  }

  def timer[A](blockOfCode: => A) = {
    val startTime = System.nanoTime
    val result = blockOfCode
    val endTime = System.nanoTime
    val delta = endTime - startTime
    (result, delta/1000000d)

  }

  def main(args: Array[String]): Unit = {

    // create a session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("CachingData")
      .getOrCreate()

    import spark.implicits._

    printConfig(spark, "\"spark.sql.join.preferSortMergeJoin\"")
    val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    // cache DataFrame
    df.cache()
    val (res, tm) = timer(df.count())
    println(s"***** Count=${res} and time=${tm}")
    println("***** Get the second time around")
    val (res2, tm2) = timer(df.count())
    println(s"***** Count=${res2} and time=${tm2}")
    // Persist on Disk
    //df.persist(StorageLevel.MEMORY_ONLY)
    df.persist(StorageLevel.DISK_ONLY)
    val (res3, tm3) = timer(df.count())
    println(s"***** Count=${res3} and time=${tm3}")
    // create an temporary SQL view
    df.createOrReplaceTempView("dfTable")
    spark.sql("cache table dfTable")
    val (res4, tm4) = timer(spark.sql("select count(*) from dfTable").show())
    println(s"***** Count=${res4} and time=${tm4}")

    // uncomment to view the SparkUI otherwise the program terminates and shutdowsn the UI
    // Thread.sleep(200000000)

    // unpersist
    df.unpersist()
    //


  }

}
