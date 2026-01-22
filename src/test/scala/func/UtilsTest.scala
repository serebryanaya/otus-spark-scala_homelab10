import Utils.{getCountriesWith5BordersDF, getLanguageRankDF}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class UtilsTest
  extends AnyFlatSpec
    with SparkSessionTestWrapper
    with Logging
    with BeforeAndAfter
    with DataFrameComparer {

  private var testDf: DataFrame = _

  before {

    import spark.implicits._

    val tempData = Seq(
      (
        ("Russia", "Russian Federation"),
        List("AZE", "BLR", "CHN", "EST", "FIN", "GEO", "KAZ", "PRK", "LVA", "LTU", "MNG", "NOR", "POL", "UKR"),
        Map("rus" -> "Russian")
      ),
      (
        ("China", "People's Republic of China"),
        List("AFG", "BTN", "MMR", "HKG", "IND", "KAZ", "NPL", "PRK", "KGZ", "LAO", "MAC", "MNG", "PAK", "RUS", "TJK", "VNM"),
        Map("zho" -> "Chinese")
      ),
      (
        ("Monaco", "Principality of Monaco"),
        List("FRA"),
        Map("fra" -> "French")
      ),
      (
        ("Germany", "Federal Republic of Germany"),
        List("AUT", "BEL", "CZE", "DNK", "FRA", "LUX", "NLD", "POL", "CHE"),
        Map("deu" -> "German")
      ),
      (
        ("France", "French Republic"),
        List("AND", "BEL", "DEU", "ITA", "LUX", "MCO", "ESP", "CHE"),
        Map("fra" -> "French")
      ),
      (
        ("Switzerland", "Swiss Confederation"),
        List("AUT", "FRA", "DEU", "ITA", "LIE"),
        Map("deu" -> "German", "fra" -> "French", "ita" -> "Italian", "roh" -> "Romansh")
      )
    ).toDF("name_tuple", "borders", "languages")

    testDf = tempData.select(
      struct(
        col("name_tuple._1").alias("common"),
        col("name_tuple._2").alias("official")
      ).alias("name"),
      col("borders"),
      col("languages")
    )
  }

  it should "print testDf schema and show" in {
    testDf.printSchema()
    testDf.show(truncate = false)
  }

  it should "getCountriesWith5BordersDF should return DataFrame with correct fields" in {
    val result = getCountriesWith5BordersDF(testDf)
    val actualSchema = result.schema

    val expectedFieldNames = Seq("Country", "NumBorders", "BorderCountries")
    assert(actualSchema.fieldNames.forall(expectedFieldNames.contains))
  }

  it should "getLanguageRankDF should DataFrame with correct fields" in {
    val result = getLanguageRankDF(testDf)
    val actualSchema = result.schema

    val expectedFieldNames = Seq("language", "NumCountries", "Countries")
    assert(expectedFieldNames.forall(actualSchema.fieldNames.contains))
  }

  it should "getCountriesWith5BordersDF should filter correctly - only countries with 5+ borders" in {
    val result = getCountriesWith5BordersDF(testDf)
    val countries = result.collect().map(_.getString(0)).toSet

    countries should contain("Russia")
    countries should contain("China")
    countries should contain("Germany")
    countries should contain("France")
    countries should contain("Switzerland")

  }
}
