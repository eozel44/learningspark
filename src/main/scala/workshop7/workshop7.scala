package workshop7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



/***
 * User-Defined Functions
 * While Apache Spark has a plethora of built-in functions, the flexibility of Spark
 * allows for data engineers and data scientists to define their own functions too.
 * These are known as user-defined functions (UDFs).
 *
 *
 * common Dataframe and spark sql operations ;
 *
 *      union
 *      join
 *      modification
 *
 */

object workshop7 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("workshop7")
      .master("local[2]")
      .getOrCreate()


    val cubed =(s:Long)=>{
      s*s*s
    }

    //register udf
    spark.udf.register("cubed",cubed)
    //create temp vive
    spark.range(1,9).createOrReplaceTempView("udf_test")
    //sql with udf
    spark.sql("select id,cubed(id) from udf_test").show()




    // Set file paths
    val delaysPath = "/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/departuredelays.csv"
    val airportsPath = "/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/airport-codes-na.txt"

    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")

    // Obtain departure Delays data set
    val delays = spark.read
      .option("header","true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Create temporary small table
    val foo = delays.filter(
      expr("""origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0"""))
    foo.createOrReplaceTempView("foo")


    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo").show()

    // Union two tables
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()

    //in sql
    spark.sql("""
                  SELECT *
                  FROM bar
                  WHERE origin = 'SEA'
                  AND destination = 'SFO'
                  AND date LIKE '01010%'
                  AND delay > 0
    """).show()

    //Join
    foo.join(airports, col("IATA") === col("origin")
    ).select("City", "State", "date", "delay", "distance", "destination").show()

    //In SQL
    spark.sql("""
          SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
          FROM foo f
          JOIN airports_na a
          ON a.IATA = f.origin
          """).show()

    //modification

    //add column
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )
    foo2.show()

    // drop column
    val foo3 = foo2.drop("delay")
    foo3.show()

    //renaming column
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    //pivot

    val pivot = spark.sql("""SELECT * FROM (
      SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
        FROM departureDelays WHERE origin = 'SEA'
    )
    PIVOT (
      CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
        FOR month IN (1 JAN, 2 FEB)
    )
    ORDER BY destination""")

    pivot.show(10)





    spark.stop()


  }

}
