package workshop2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc}

/** *
 * Transformations, Actions, and Lazy Evaluation
 *
 * Spark operations on distributed data can be classified into two types: transformations and actions.
 * Transformations, as the name suggests, transform a Spark DataFrame into a new DataFrame without altering the original data,
 * giving it the property of immutability.
 *
 * All transformations are evaluated lazily. That is, their results are not computed immediately,
 * but they are recorded or remembered as a lineage. A recorded lineage allows Spark,
 * at a later time in its execution plan, to rearrange certain transformations, coalesce them,
 * or optimize transformations into stages for more efficient execution.
 * Lazy evaluation is Spark’s strategy for delaying execution until an action is invoked or data
 * is “touched” (read from or written to disk).
 *
 *
 * While lazy evaluation allows Spark to optimize your queries by peeking into your chained transformations,
 * lineage and data immutability provide fault tolerance.
 * Because Spark records each transformation in its lineage and the DataFrames are immutable between transformations,
 * it can reproduce its original state by simply replaying the recorded lineage, giving it resiliency in the event of failures.
 *
 *
 * Transformations Actions
 * orderBy()       show()
 * groupBy()       take()
 * filter()        count()
 * select()        collect()
 * join()          save()
 *
 *
 * Transformations can be classified as having either narrow dependencies or wide
 * dependencies. Any transformation where a single output partition can be computed
 * from a single input partition is a narrow transformation.
 * For example, filter() and contains() represent narrow transformations because
 * they can operate on a single partition and produce the resulting output partition
 * without any exchange of data.
 *
 * However, groupBy() or orderBy() instruct Spark to perform wide transformations,
 * where data from other partitions is read in, combined, and written to disk. Since each
 * partition will have its own count of the word that contains the “Spark” word in its row
 * of data, a count (groupBy()) will force a shuffle of data from each of the executor’s
 * partitions across the cluster. In this transformation, orderBy() requires output from
 * other partitions to compute the final aggregation.
 *
 */


object workshop2 {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("mnmount")
      .master("local[2]")
      .getOrCreate()

    // get the mnm data set file name
    val mnmFile = s"/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/mnmcount.csv"
    // read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mnmFile)
    // display DataFrame
    mnmDF.show(5, false)
    // aggregate count of all colors and groupBy state and color
    // orderBy descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy(desc("Total"))
    // show all the resulting aggregation for all the dates and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // find the aggregate count for California by filtering
    val caCountMnNDF = mnmDF.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count")
        .alias("Total"))
      .orderBy(desc("Total"))
    // show the resulting aggregation for California
    caCountMnNDF.show(10)

    spark.stop()
  }

}
