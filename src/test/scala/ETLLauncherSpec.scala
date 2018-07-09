import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.FunSuite

class ETLLauncherSpec extends FunSuite with DataFrameSuiteBase{

  import spark.implicits._

  test("Invoking populateConfig on empty Array produces IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      ETLLauncher.populateConfig(Array())
    }
  }

  val expectedData = Seq(
    Row("miguel", "hello world"),
    Row("luisa", "hello world")
  )

  val expectedSchema = List(
    StructField("name", StringType, true),
    StructField("greeting", StringType, false)
  )

  /*val expectedDF = spark.createDataFrame(
    spark.sparkContext.parallelize(expectedData),
    StructType(expectedSchema)
  )*/
}
