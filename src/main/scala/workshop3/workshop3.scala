package workshop3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, expr}

/***
 *
 * RDD;
 *   Dependencies
 *   Partitions (with some locality information)
 *   Compute function: Partition => Iterator[T]
 *
 * All three are integral to the simple RDD programming API model upon which all
 * higher-level functionality is constructed.
 * First, a list of dependencies that instructs Spark how an RDD is constructed with its inputs is required.
 * When necessary to reproduce results, Spark can recreate an RDD from these dependencies and replicate operations on it.
 * This characteristic gives RDDs resiliency.
 *
 * Second, partitions provide Spark the ability to split the work to parallelize computation on partitions across executors. In some cases—for example, reading from
 * HDFS—Spark will use locality information to send work to executors close to the data.
 * That way less data is transmitted over the network.
 *
 * And finally, an RDD has a compute function that produces an Iterator[T] for the
 * data that will be stored in the RDD.
 *
 * DataFrame
 *
 * Spark DataFrames are like distributed in-memory tables with named columns and schemas,
 * where each column has a specific data type: integer, string, array, map, real, date, timestamp, etc.
 * To a human’s eye, a Spark DataFrame is like a table.  structured table!!!
 *
 * A schema in Spark defines the column names and associated data types for a DataFrame.
 * Most often, schemas come into play when you are reading structured data from an external data source
 *
 *      You relieve Spark from the onus of inferring data types.
 *      You prevent Spark from creating a separate job just to read a large portion of
 *          your file to ascertain the schema, which for a large data file can be expensive and time-consuming.
 *      You can detect errors early if data doesn’t match the schema.
 *
 *
 *
 */
object workshop3 {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("workshop3")
      .master("local[2]")
      .getOrCreate()

    //get the path to the JSON file
    val jsonFile = s"/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/blogs.json"
    //define our schema as before
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    //Create a DataFrame by reading from the JSON file a predefined Schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    //show the DataFrame schema as output
    blogsDF.show(truncate = false)
    // print the schemas
    print(blogsDF.printSchema)
    print(blogsDF.schema)
    // Show columns and expressions
    blogsDF.select(expr("Hits") * 2).show(2)
    blogsDF.select(col("Hits") * 2).show(2)
    blogsDF.select(expr("Hits * 2")).show(2)
    // show heavy hitters
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    spark.stop()

  }
}