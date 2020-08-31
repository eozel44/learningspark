package workshop6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.avro.functions._

/***
 * Parquet
 * We’ll start our exploration of data sources with Parquet, because it’s the default data source in Spark.
 * Supported and widely used by many big data processing frameworks and platforms,
 * Parquet is an open source columnar file format that offers many I/O optimizations
 * such as compression, which saves storage space and allows for quick access to data columns.
 *
 * JSON
 * JavaScript Object Notation (JSON) is also a popular data format.
 * It came to prominence as an easy-to-read and easy-to-parse format compared to XML.
 * It has two representational formats: single-line mode and multiline mode. Both modes are
 * supported in Spark.
 *
 * CSV
 * As widely used as plain text files, this common text file format captures each datum
 * or field delimited by a comma; each line with comma-separated fields represents a
 * record. Even though a comma is the default separator, you may use other delimiters
 * to separate fields in cases where commas are part of your data. Popular spreadsheets
 * can generate CSV files, so it’s a popular format among data and business analysts.
 *
 * Avro
 * Introduced in Spark 2.4 as a built-in data source, the Avro format is used, for exam‐
 * ple, by Apache Kafka for message serializing and deserializing. It offers many bene‐
 * fits, including direct mapping to JSON, speed and efficiency, and bindings available
 * for many programming languages.
 *
 * ORC
 * As an additional optimized columnar file format, Spark 2.x supports a vectorized ORC reader.
 * Two Spark configurations dictate which ORC implementation to use.
 * When spark.sql.orc.impl is set to native and spark.sql.orc.enableVectorizedReader is set to true,
 * Spark uses the vectorized ORC reader. A vectorized reader reads blocks of rows (often 1,024 per block)
 * instead of one row at a time, streamlining operations and reducing CPU usage for intensive operations like
 * scans, filters, aggregations, and joins.
 * For Hive ORC SerDe (serialization and deserialization) tables created with the SQL
 * command USING HIVE OPTIONS (fileFormat 'ORC'), the vectorized reader is used
 * when the Spark configuration parameter spark.sql.hive.convertMetastoreOrc is set to true.
 *
 */

object workshop6 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("workshop5")
      .master("local[2]")
      .getOrCreate()



    // parquet file
    //Reading a Parquet file into a DataFrame
    val parquetFile = s"/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/summary-data/parquet/2010-summary.parquet/"
    val df = spark.read.format("parquet").load(parquetFile)


    val df_parquet = df.filter(col("DEST_COUNTRY_NAME") === "United States")

    //dataframe to spark sql table
    df.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl")


    // write dataframe to a parquet file
    df_parquet.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/summary-data/parquet/df_parquet")



    //Reading an Avro file into a Spark SQL table
    val tempTable =
      s"""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING parquet
         |OPTIONS (path "/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/summary-data/parquet/2010-summary.parquet/" )""".stripMargin

    spark.sql(tempTable)
    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show(10)

    //avro

    //Reading an Avro file into a DataFrame

    /*  spark shell
        ./bin/spark-shell --packages org.apache.spark:spark-avro_2.12:3.0.0
    */

    //Reading an Avro file into a DataFrame
    val dfAvro = spark.read.format("avro").load("/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/summary-data/avro/*")
    dfAvro.show(false)

    //Reading an Avro file into a Spark SQL table
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW episode_tbl
              USING avro
              OPTIONS (
              path "/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/summary-data/avro/*"
    )""")
    spark.sql("SELECT * FROM episode_tbl").show(false)

    // Writing DataFrames to Avro files
    dfAvro.write
      .format("avro")
      .mode("overwrite")
      .save("/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/summary-data/avro/df_avro")


    /* orc code ?? */




    spark.stop()

  }

}
