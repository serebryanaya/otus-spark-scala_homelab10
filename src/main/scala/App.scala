import Utils.schema
import org.apache.logging.log4j.scala.Logging

object App extends Logging with SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    val df = spark.read
      .schema(schema)
      .option("multiLine", value = true)
      .json("src/test/resources/countries.json")

  }
}