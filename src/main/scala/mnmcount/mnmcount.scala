package mnmcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/***
 * Spark Jobs
 * The driver converts your Spark application into one or more Spark jobs. It then transforms each job into a DAG.
 * This, in essence, is Spark’s execution plan, where each node within a DAG could be a single or multiple Spark stages.
 *
 * A Job is a sequence of Stages, triggered by an Action such as; count(), foreachRdd(), collect(), read() or write().
 *
 */

/***
 * Spark Stages
 * As part of the DAG nodes, stages are created based on what operations can be performed serially or in parallel.
 * Not all Spark operations can happen in a single stage, so they may be divided into multiple stages.
 * Often stages are delineated on the operator’s computation boundaries, where they dictate data transfer among Spark executors.
 *
 * A Stage is a sequence of Tasks that can all be run together, in parallel, without a shuffle.
 */

/***
 * Spark Tasks
 * Each stage is comprised of Spark tasks (a unit of execution), which are then federated across each Spark executor;
 * each task maps to a single core and works on a single partition of data.
 * As such, an executor with 16 cores can have 16 or more tasks working on 16 or more partitions in parallel,
 * making the execution of Spark’s tasks exceedingly parallel!
 *
 * A Task is a single operation (.map or .filter) applied to a single Partition.
 * Each Task is executed as a single thread in an Executor!
 */

/***
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


object mnmcount {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("mnmount")
      .master("local[2]")
      .getOrCreate()

    // get the mnm data set file name
    val mnmFile =s"/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/mnmcount.csv"
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
