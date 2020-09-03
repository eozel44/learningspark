package workshop4

import org.apache.spark.sql.SparkSession

/***
 * Dataset
 * Conceptually, you can think of a DataFrame in Scala as an alias for a collection of generic objects, Dataset[Row],
 * where a Row is a generic untyped JVM object that may hold different types of fields.
 * A Dataset, by contrast, is a collection of strongly typed JVM objects in Scala or a class in Java.
 * A Dataset is;
 *      a strongly typed collection of domain-specific objects that can be transformed in parallel
 *      using functional or relational operations. Each Dataset [in Scala] also has an untyped view called a DataFrame,
 *      which is a Dataset of Row.
 *
 */
/***
 * DataFrames Versus Datasets
 *
 *  If you want to tell Spark what to do, not how to do it, use DataFrames or Datasets.
 *  If you want rich semantics, high-level abstractions, and DSL operators, use DataFrames or Datasets.
 *  If you want strict compile-time type safety and don’t mind creating
 *     multiple case classes for a specific Dataset[T], use Datasets.
 *  If your processing demands high-level expressions, filters, maps, aggregations,computing averages or sums,
 *    SQL queries, columnar access, or use of relational operators on semi-structured data, use DataFrames or Datasets.
 *  If your processing dictates relational transformations similar to SQL-like queries, use DataFrames.
 *  If you want to take advantage of and benefit from Tungsten’s efficient serialization with Encoders, use Datasets.
 *  If you want unification, code optimization, and simplification of APIs across Spark components, use DataFrames.
 *  If you are an R user, use DataFrames.
 *  If you are a Python user, use DataFrames and drop down to RDDs if you need more control.
 *  If you want space and speed efficiency, use DataFrames.
 *
 */

/***
 * When to Use RDD ?
 *
 *  Are using a third-party package that’s written using RDDs
 *  Can forgo the code optimization, efficient space utilization,
 *    and performance benefits available with DataFrames and Datasets
 *  Want to precisely instruct Spark how to do a query
 *
 */

/***
 * Dataset Encoders
 * Encoders convert data in off-heap memory from Spark’s internal Tungsten format to JVM Java objects.
 * In other words, they serialize and deserialize Dataset objects from Spark’s internal format to JVM objects,
 * including primitive data types. For example, an Encoder[T] will convert from Spark’s internal Tungsten format to Dataset[T].
 *
 * Spark has built-in support for automatically generating encoders for primitive types(e.g., string, integer, long),
 * Scala case classes, and JavaBeans. Compared to Java and Kryo serialization and deserialization,
 * Spark encoders are significantly faster.
 *
 */

/***
 * Memory management dataset & dataframes
 *
 * • Spark 1.0 used RDD-based Java objects for memory storage, serialization, and
 *   deserialization, which was expensive in terms of resources and slow. Also, storage
 *   was allocated on the Java heap, so you were at the mercy of the JVM’s garbage
 *   collection (GC) for large data sets.
 *
 * • Spark 1.x introduced Project Tungsten. One of its prominent features was a new
 *   internal row-based format to lay out Datasets and DataFrames in off-heap memory,
 *   using offsets and pointers. Spark uses an efficient mechanism called encoders
 *   to serialize and deserialize between the JVM and its internal Tungsten format.
 *   Allocating memory off-heap means that Spark is less encumbered by GC.
 *
 * • Spark 2.x introduced the second-generation Tungsten engine, featuring whole-
 *   stage code generation and vectorized column-based memory layout. Built on
 *   ideas and techniques from modern compilers, this new version also capitalized
 *   on modern CPU and cache architectures for fast parallel data access with the
 *   “single instruction, multiple data” (SIMD) approach.
 *
 *
 * Costs of Using Datasets
 * In “DataFrames Versus Datasets”, we outlined some of the benefits of using Datasets—but these benefits come at a cost.
 * As noted when Datasets are passed to higher-order functions such as filter(), map(), or flatMap()
 * that take lambdas and functional arguments, there is a cost associated
 * with deserializing from Spark’s internal Tungsten format into the JVM object.
 * Compared to other serializers used before encoders were introduced in Spark, this
 * cost is minor and tolerable. However, over larger data sets and many queries, this cost
 * accrues and can affect performance.
 *
 * One strategy to mitigate excessive serialization and deserialization is to use DSL expressions in your queries and
 * avoid excessive use of lambdas as anonymous functions as arguments to higher-order functions.
 * Because lambdas are anonymous and opaque to the Catalyst optimizer until runtime,
 * when you use them it cannot efficiently discern what you’re doing (you’re not telling Spark what to do) and
 * thus cannot optimize your queries.
 *
 * The second strategy is to chain your queries together in such a way that serialization
 * and deserialization is minimized. Chaining queries together is a common practice in
 * Spark. (means that use only dsl or anonymous in query chain)
 *
 */


case class DeviceIoTData (battery_level: Long, c02_level: Long,
                          cca2: String, cca3: String, cn: String, device_id: Long,
                          device_name: String, humidity: Long, ip: String, latitude: Double,
                          lcd: String, longitude: Double, scale:String, temp: Long,
                          timestamp: Long)

object workshop4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("workshop4")
      .master("local[2]")
      .getOrCreate()

    //get the path to the JSON file
    val jsonFile = s"/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/iot_devices.json"

    import spark.implicits._

    val ds = spark.read
      .json(jsonFile)
      .as[DeviceIoTData]

    ds.show(5, false)
    println(s"rows count: ${ds.count}")

    // filter out all devices whose temperature exceed 25 degrees and generate
    // another Dataset with three fields that of interest and then display
    // the mapped Dataset
    val dsTemp = ds.filter(d => d.temp > 25).map(d => (d.temp, d.device_name, d.cca3))
    dsTemp.show

    // Apply higher-level Dataset API methods such as groupBy() and avg().
    // Filter temperatures > 25, along with their corresponding
    // devices' humidity, compute averages, groupBy cca3 country codes and display

   //code ??



    spark.stop()




  }



}
