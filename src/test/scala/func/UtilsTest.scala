import Utils.{getCountriesWith5BordersDF, getLanguageRankDF, schema}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class UtilsTest
  extends AnyFlatSpec
    with SparkSessionTestWrapper
    with Logging
    with BeforeAndAfter
    with DataFrameComparer {

  private var testDf: DataFrame = _

  before {
    testDf = spark.read
      .schema(schema)
      .json("src/test/resources/countries.json")
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

  it should "getCountriesWith5BordersDF should correct for country with and without borders" in {
    val result = getCountriesWith5BordersDF(testDf)

    val australiaRows = result.filter(col("Country") === "Australia").collect()
    australiaRows.length.equals(0)

    val russianRows = result.filter(col("Country") === "Russia").collect()
    !russianRows.length.equals(0)
  }
}
