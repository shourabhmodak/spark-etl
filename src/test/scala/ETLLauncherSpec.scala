import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import service.ETLConfig

class ETLLauncherSpec extends SharedSparkContext {
  test("ETL Configuration parameter check") {
    val stgDataDF = DataLoader.loadDataFromCSVWithSchema(hc, "src/test/resources/stgdata_base.csv", stgTableSchema)
    val targetDF = DataLoader.loadDataFromCSVWithSchema(hc, "src/test/resources/tgtdata_base.csv", tgtTableSchema)

    stgDataDF.registerTempTable("STG_CUST_LOC")
    targetDF.registerTempTable("CUSTOMER_LOCATION")

    val configValidParams = ETLConfig (
      sourceTableName     = "STG_CUST_LOC",
      targetTableName     = "CUSTOMER_LOCATION",
      sourcePartitionCol  = "SRC_DT",
      keyColumns          =  Seq("CUST_ID"))
    assert(ETLLauncher.checkConfigParams(configValidParams, hc), true)

    val configInvalidStgTable = ETLConfig (
      sourceTableName     = "INVALID_STG_TABLE_NAME",
      targetTableName     = "CUSTOMER_LOCATION",
      sourcePartitionCol  = "SRC_DT",
      keyColumns          =  Seq("CUST_ID"))
    intercept[IllegalArgumentException] {
      ETLLauncher.checkConfigParams(configInvalidStgTable, hc)
    }

    val configInvalidStgPartCol = ETLConfig (
      sourceTableName     = "STG_CUST_LOC",
      targetTableName     = "CUSTOMER_LOCATION",
      sourcePartitionCol  = "INVALID_SRC_DT",
      keyColumns          =  Seq("CUST_ID"))
    intercept[IllegalArgumentException] {
      ETLLauncher.checkConfigParams(configInvalidStgPartCol, hc)
    }
  }

  test("Input Staging table with new data") {
    val stgDataDF = DataLoader.loadDataFromCSVWithSchema(hc, "src/test/resources/stgdata_new.csv", stgTableSchema)
    val baseTargetDF = DataLoader.loadDataFromCSVWithSchema(hc, "src/test/resources/tgtdata_base.csv", tgtTableSchema)
    val expectedTargetDF = DataLoader.loadDataFromCSVWithSchema(hc, "src/test/resources/tgtdata_new.csv", tgtTableSchema)

    stgDataDF.registerTempTable("STG_CUST_LOC")
    baseTargetDF.registerTempTable("CUSTOMER_LOCATION")
    val config = ETLConfig(
      sourceTableName = "STG_CUST_LOC",
      targetTableName = "CUSTOMER_LOCATION",
      sourcePartitionCol = "SRC_DT",
      keyColumns = Seq("CUST_ID"))

    val generatedTargetDF = ETLLauncher.generateTargetData(config, hc)
    assertDataFrameEquals(expectedTargetDF, generatedTargetDF)
  }

  val stgTableSchema = StructType(
    Seq(
      StructField("CUST_ID", IntegerType, true),
      StructField("CUST_CITY", StringType, true),
      StructField("CUST_COUNTRY", StringType, true),
      StructField("SRC_DT", StringType, true)
    ))

  val tgtTableSchema = StructType(
    Seq(
      StructField("CUST_ID", IntegerType, true),
      StructField("CUST_CITY", StringType, true),
      StructField("CUST_COUNTRY", StringType, true)
    ))

}
