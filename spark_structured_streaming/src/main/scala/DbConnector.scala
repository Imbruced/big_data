import java.util.Properties
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

abstract class DbWriter(properties: Properties) {
  val dbName: String = properties.getProperty("dbName", "name")
  val hostName: String = properties.getProperty("host", "localhost")
  val mode: String = properties.getProperty("mode", "append")
  val user: String = properties.getProperty("user", "user")
  val password: String = properties.getProperty("password", "password")
  val port: String = properties.getProperty("port", "5432")
  val tableName: String = properties.getProperty("tableName", "table")

  def url: String = ???
  def sparkProp: Properties = {
    val spProp: Properties = new Properties()
    spProp.put("user", this.user)
    spProp.put("password", this.password)
    spProp
  }

  def write(batchData: Dataset[Row], batchId: Long): Unit= {
    batchData
      .withColumn("start_date", col("window.start"))
      .withColumn("end_date", col("window.end"))
      .drop("window")
      .write
      .mode(this.mode)
      .jdbc(this.url, tableName, this.sparkProp)
  }

  def read(sparkSession: SparkSession): Dataset[Row] = {
    sparkSession
      .read
      .jdbc(this.url, tableName, this.sparkProp)
  }

}

class PostgreWrite(properties: Properties) extends DbWriter(properties){
  override val dbName: String = properties.getProperty("dbName", "postgres")
  override val hostName: String = properties.getProperty("host", "localhost")
  override val mode: String = properties.getProperty("mode", "append")
  override val user: String = properties.getProperty("user", "postgres")
  override val password: String = properties.getProperty("password", "postgres")
  override val port: String = properties.getProperty("port", "5432")
  override val tableName: String = properties.getProperty("tableName", "public.table")

  override def url: String = s"jdbc:postgresql://${this.hostName}:${this.port}/${this.dbName}"

}
