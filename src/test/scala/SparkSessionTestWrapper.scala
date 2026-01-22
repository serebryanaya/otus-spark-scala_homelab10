import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkApp")
    .master("local")
    .getOrCreate

}