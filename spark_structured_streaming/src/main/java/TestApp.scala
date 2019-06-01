import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher




object TestApp extends App{
  val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))
  val lines = ssc.socketTextStream("localhost", 9092)
  val words = lines.flatMap(_.split(" "))
  words.print()

  ssc.start()
//  val df = spark
//    .readStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//    .option("subscribe", "topic1")
//    .load()
}
