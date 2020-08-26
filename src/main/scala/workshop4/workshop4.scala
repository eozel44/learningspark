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

case class DeviceIoTData (battery_level: Long, c02_level: Long,
                          cca2: String, cca3: String, cn: String, device_id: Long,
                          device_name: String, humidity: Long, ip: String, latitude: Double,
                          lcd: String, longitude: Double, scale:String, temp: Long,
                          timestamp: Long)

object workshop4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("workshop3")
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
