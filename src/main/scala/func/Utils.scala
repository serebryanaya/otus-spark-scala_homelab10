import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, explode}

object Utils extends SparkSessionWrapper with Logging {

  def getCountriesWith5BordersDF(df: DataFrame): DataFrame = {

    df.filter(functions.size(col("borders")) >= 5)
      .select(
        col("name.common").alias("Country"),
        functions.size(col("borders")).alias("NumBorders"),
        concat_ws(",", col("borders")).alias("BorderCountries")
      )

  }

  def getLanguageRankDF(df: DataFrame): DataFrame = {

    df.select(
        col("name.common").alias("Country"),
        explode(col("languages")).as(Seq("lang_code", "language"))
      )
      .groupBy("language")
      .agg(
        count("Country").alias("NumCountries"),
        collect_list("Country").alias("Countries")
      )
  }
}