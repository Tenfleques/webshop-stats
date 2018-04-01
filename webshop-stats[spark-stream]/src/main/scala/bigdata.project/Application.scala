import org.apache

object Application extends App{
  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._
}