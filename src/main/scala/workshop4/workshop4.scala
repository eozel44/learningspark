package workshop4

import org.apache.spark.sql.SparkSession

/***
 * Dataset
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
