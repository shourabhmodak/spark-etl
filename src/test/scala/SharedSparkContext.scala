import com.holdenkarau.spark.testing._
import org.apache.spark.sql.hive.HiveContext
import org.scalatest._

trait SharedSparkContext extends FunSuite with Matchers with DataFrameSuiteBase with BeforeAndAfterAll  {

  lazy val hc = new HiveContext(sc)

  override def beforeAll(): Unit = {
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

}
