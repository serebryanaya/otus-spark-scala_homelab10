import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._

object App extends Logging with SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    val schema: StructType = StructType(Array(
      StructField("name", StructType(Array(
        StructField("common", StringType),
        StructField("official", StringType)
      ))),
      StructField("borders", ArrayType(StringType)),
      StructField("languages", MapType(StringType, StringType))
    ))

    val df = spark.read
      .schema(schema)
      .option("multiLine", value = true)
      .json("src/test/resources/countries.json")

  }
}