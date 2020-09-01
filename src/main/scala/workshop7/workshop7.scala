package workshop7

import org.apache.spark.sql.SparkSession

/***
 * User-Defined Functions
 * While Apache Spark has a plethora of built-in functions, the flexibility of Spark
 * allows for data engineers and data scientists to define their own functions too.
 * These are known as user-defined functions (UDFs).
 *
 */
/
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

    spark.sql("select id,cubed(id) from udf_test").show()

    spark.stop()


  }

}
