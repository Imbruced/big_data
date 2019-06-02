import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

object NextBike extends App{
  val schema = StructType(Seq(
    StructField("lat", DoubleType, false),
    StructField("lng", DoubleType, false),
    StructField("bike", StringType, false),
    StructField("name", StringType, false),
    StructField("address", StringType, false),
    StructField("spot", StringType, false),
    StructField("number", StringType, false),
    StructField("bikes", StringType, false),
    StructField("booked_bikes", StringType, false),
    StructField("bike_racks", StringType, false),
    StructField("free_racks", StringType, false),
    StructField("special_racks", StringType, false),
    StructField("free_special_racks", StringType, false),
    StructField("maintenance", StringType, false),
    StructField("terminal_type", StringType, false),
    StructField("place_type", StringType, false)
  ))

  val fields = schema.fields.map(
    x => "j_data" + "." + x.name
  ).toSeq

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Test App")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "next_bike")
    .load
    .select(col("key").cast("string"),
      col("value").cast("string"),
      col("topic"),
      col("partition"),
      col("offset"))
    .withColumn("j_data", from_json(col("value"), schema))
    .select("key", fields:_*)

  val consoleOutput = df.writeStream
    .outputMode("append")
    .format("console")
    .start()
  consoleOutput.awaitTermination()
}
