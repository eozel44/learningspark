package workshop8

import org.apache.spark.sql.SparkSession

/***
 *
 * Shuffle Sort Merge Join
 * The sort-merge algorithm is an efficient way to merge two large data sets over a common key that is sortable,
 * unique, and can be assigned to or stored in the same partition—that is,
 * two data sets with a common hashable key that end up being on the same partition.
 * From Spark’s perspective, this means that all rows within each data set
 * with the same key are hashed on the same partition on the same executor.
 * Obviously, this means data has to be colocated or exchanged between executors.
 *
 * When to use a shuffle sort merge join
 * • When each key within two large data sets can be sorted and hashed to the same
 *   partition by Spark
 * • When you want to perform only equi-joins to combine two data sets based on
 *   matching sorted keys
 * • When you want to prevent Exchange and Sort operations to save large shuffles
 *   across the network
 *
 * Broadcast Hash Join
 * Also known as a map-side-only join, the broadcast hash join is employed when two data sets,
 * one small (fitting in the driver’s and executor’s memory) and another large
 * enough to ideally be spared from movement, need to be joined over certain
 * conditions or columns. Using a Spark broadcast variable, the smaller data set is broadcasted by the driver to all Spark executors,
 * and subsequently joined with the larger data set on each executor. This strategy avoids the large exchange.
 *
 * // In Scala
 * import org.apache.spark.sql.functions.broadcast
 * val joinedDF = biggerDF.join(broadcast(smallerDF), "key1 === key2")
 *
 */

object SortMergeJoin {

  // curried function to benchmark any code or function
  def benchmark(name: String)(f: => Unit) {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SortMergeJoin")
      .config("spark.sql.codegen.wholeStage", true)
      .config("spark.sql.join.preferSortMergeJoin", true)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.defaultSizeInBytes", 100000)
      .config("spark.sql.shuffle.partitions", 16)
      .master("local[*]")
      .getOrCreate ()

    import spark.implicits._

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)

    // initialize states and items purchased
    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")
    // create dataframes
    val usersDF = (0 to 100000).map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")
    val ordersDF = (0 to 100000).map(r => (r, r, rnd.nextInt(100000), 10 * r* 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    usersDF.show(10)
    ordersDF.show(10)

    // do a Join
    val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")
    usersOrdersDF.show(10, false)
    usersOrdersDF.cache()
    usersOrdersDF.explain()
    // usersOrdersDF.explain("formated")
    //uncoment to view the SparkUI otherwise the program terminates and shutdowsn the UI
    //Thread.sleep(200000000)
    spark.stop

  }

}
