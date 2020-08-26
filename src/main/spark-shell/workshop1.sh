val file =s"/home/eren/bigdata/cases/workshop/learningspark/src/main/resources/wordcount.txt"
val strings = spark.read.text(file)
strings.show(10)
strings.take(10).foreach(println)


//transformation & actions
import org.apache.spark.sql.functions._
val strings = spark.read.text(file)
val filtered = strings.filter(col("value").contains("soul"))
filtered.count()


