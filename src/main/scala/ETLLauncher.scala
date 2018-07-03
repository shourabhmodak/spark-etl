import domain.ETLConfig
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ETLLauncher {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ETL dedupe")
      .enableHiveSupport()
      .getOrCreate()

    val etlConfig = populateConfig(args)

    val sourceDF = getSourceData(etlConfig, spark)

    val expectedData = Seq(
      Row("miguel", "hello world"),
      Row("luisa", "hello world")
    )

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

  }

  /**
    *
    * @param args
    * @return
    */
  def populateConfig(args: Array[String]): ETLConfig = {
    if (args.length < 4) {
      throw new IllegalArgumentException("Missing required arguments: Expected 3, got " + args.length)
    }

    val srcTableName = args(0)
    val tgtTableName = args(1)
    val srcPartitionCol = args(2)
    val keyAttrs = args(3).split(",")

    ETLConfig(srcTableName, tgtTableName, srcPartitionCol, keyAttrs)
  }

  def checkConfigParams(etlConf: ETLConfig, spark: SparkSession) : Unit = {
    if (!spark.catalog.tableExists(etlConf.sourceTableName))
      throw new IllegalArgumentException("Invalid source table name, " + etlConf.sourceTableName)
    else if (!spark.catalog.tableExists(etlConf.targetTableName))
      throw new IllegalArgumentException("Invalid target table name, " + etlConf.targetTableName)
    else {
      val srcColumns = spark.catalog.listColumns(etlConf.sourceTableName)
      srcColumns
        .select("columnName")
        .filter("columnName = $etlConf.sourcePartitionCol")
        .count()
    }

  }

  def getSourceData(etlConf: ETLConfig, sparkSes: SparkSession) : DataFrame = {
    sparkSes.sql(
      QueryManager.getSrcLatestDataQuery(etlConf.sourceTableName, etlConf.sourcePartitionCol)
    )
  }
}
