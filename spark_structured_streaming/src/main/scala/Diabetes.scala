import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import java.util.Properties


object Diabetes extends App{

  val props = new Properties()
  props.put("dbName", "diabetes")
  props.put("user", "pkocinski001")
  props.put("password", "kojack01")
  props.put("tableName", "public.test2")

  val postgreConnector = new PostgreWrite(props)


  val numCols = List.range(0, 10).map(
    x => StructField(x.toString, DoubleType, false)
  )

  val dateCol = List(StructField("date", TimestampType, false))

  val schema = StructType(
    numCols++dateCol
  )

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
    .option("subscribe", "diabetes")
    .load
    .select(col("key").cast("string"),
      col("value").cast("string"),
      col("topic"),
      col("partition"),
      col("offset"))
    .withColumn("j_data", from_json(col("value"), schema))
    .select("key", fields:_*)
    .withWatermark("date", "30 seconds")
    .groupBy(
      window(col("date"), "1 minute", "30 seconds"),
      col("key")
    ).agg(sum("0").alias("value"))
    .drop("key")

    val consoleOutput = df.writeStream
    .foreachBatch((x, y) => postgreConnector.write(x, y))
    .start()

  consoleOutput.awaitTermination()
}
