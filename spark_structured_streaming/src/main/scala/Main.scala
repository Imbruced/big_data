import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

object Main extends App {

  val schema_lat = StructType(Seq(
    StructField("lat", DoubleType, false)
  ))

  val schema_lon = StructType(Seq(
    StructField("lon", DoubleType, false)
  ))


  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Test App")
    .getOrCreate()


  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .load
    .select(col("key").cast("string"),
      col("value").cast("string"),
      col("topic"),
      col("partition"),
      col("offset"))
    .withColumn("lat", from_json(col("value"), schema_lat))
    .withColumn("lon", from_json(col("value"), schema_lon))
    .select("lon.lon", "lat.lat", "key")


  val consoleOutput = df.writeStream
    .outputMode("append")
    .format("console")
    .start()
  consoleOutput.awaitTermination()



}
