import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class ETLLauncherSpec extends FunSuite with DataFrameSuiteBase{

  import spark.implicits._

  test("Invoking populateConfig on empty Array produces IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      ETLLauncher.populateConfig(Array())
    }
  }
}
