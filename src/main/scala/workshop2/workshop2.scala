package workshop2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc}

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

/** Underlying Engine
 *
 * Phase 1: Analysis
 * The Spark SQL engine begins by generating an abstract syntax tree (AST) for the SQL or DataFrame query.
 * In this initial phase, any columns or table names will be resolved by consulting an internal Catalog,
 * a programmatic interface to Spark SQL that holds a list of names of columns, data types, functions, tables, databases, etc.
 * Once they’ve all been successfully resolved, the query proceeds to the next phase.
 *
 * Phase 2: Logical optimization
 * This phase comprises two internal stages. Applying a standard rule based optimization approach,
 * the Catalyst optimizer will first construct a set of multiple plans and then, using its cost-based optimizer (CBO),
 * assign costs to each plan. These plans are laid out as operator trees; they may include,
 * for example, the process of constant folding, predicate pushdown, projection pruning, Boolean expression simplification, etc.
 * This logical plan is the input into the physical plan.
 *
 * Phase 3: Physical planning
 * In this phase, Spark SQL generates an optimal physical plan for the selected logical plan,
 * using physical operators that match those available in the Spark execution engine.
 *
 * Phase 4: Code generation
 * The final phase of query optimization involves generating efficient Java bytecode to
 * run on each machine. Because Spark SQL can operate on data sets loaded in memory,
 * Spark can use state-of-the-art compiler technology for code generation to speed up
 * execution. In other words, it acts as a compiler. Project Tungsten, which facilitates
 * whole-stage code generation, plays a role here.
 * Just what is whole-stage code generation? It’s a physical query optimization phase that
 * collapses the whole query into a single function, getting rid of virtual function calls
 * and employing CPU registers for intermediate data. The second-generation Tungsten
 * engine, introduced in Spark 2.0, uses this approach to generate compact RDD code
 * for final execution. This streamlined strategy significantly improves CPU efficiency
 * and performance.
 *
 * */
