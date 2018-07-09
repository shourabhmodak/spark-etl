import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import service.ETLConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import util.SparkUtils

object ETLLauncher {

  private val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getSparkContext
    val hiveContext = SparkUtils.getHiveContext(sparkContext)

    ETLConfig.getConfig.fold(
      error => {
        log.error(error)
        throw new RuntimeException(error)
      },
      config => {
        log.info(s"Running Spark ETL with config: $config")
        try {
          runJob(config, hiveContext)
        } catch {
          case ex: Exception =>
            log.error(s"Error in Spark ETL : ${ex.getMessage} :  ${ex.printStackTrace()}")
        }
      }
    )
  }

  def runJob(etlConfig: ETLConfig, hiveCtx: HiveContext): Unit = {
    checkConfigParams(etlConfig, hiveCtx)

    val targetDF = hiveCtx.table(etlConfig.targetTableName)
    targetDF.registerTempTable("tgtTbl")

    val isHistorical = targetDF.count() == 0

    val finalTargetDF = if (isHistorical) {
      log.info(s"No data exist in $etlConfig.targetTableName. Starting to do initial load using complete staging table data.")

      val stgDF = hiveCtx.table(etlConfig.sourceTableName).dropDuplicates()
      stgDF.registerTempTable("stgTbl")

      getQueryResults(
        QueryManager.getStgDedupedDataQuery(etlConfig.sourceTableName, etlConfig.sourcePartitionCol,
          targetDF.schema.fieldNames, etlConfig.keyColumns),
        hiveCtx)
    } else {
      log.info(s"Starting delta load using latest staging table data.")

      val stgDF = getQueryResults(
        QueryManager.getSrcLatestDataQuery(etlConfig.sourceTableName, etlConfig.sourcePartitionCol),
        hiveCtx).dropDuplicates()
      stgDF.registerTempTable("stgTbl")

      val tgtDataUnchangedDF = getQueryResults(
        QueryManager.getUnchangedTgtDataQuery(etlConfig.sourceTableName, etlConfig.targetTableName, etlConfig.keyColumns),
        hiveCtx)

      val stgNewAndModifiedDF = getQueryResults(
        QueryManager.getNewModifiedStgDataQuery(etlConfig.sourceTableName, etlConfig.targetTableName,
          targetDF.schema.fieldNames, etlConfig.keyColumns),
        hiveCtx)

      tgtDataUnchangedDF.unionAll(stgNewAndModifiedDF)
    }

    finalTargetDF.write.format("orc").mode("overwrite").insertInto(etlConfig.targetTableName)
  }

  def checkConfigParams(etlConf: ETLConfig, hiveCtx: HiveContext) : Unit = {
    if (!hiveCtx.tableNames.contains(etlConf.sourceTableName))
      throw new IllegalArgumentException("Invalid staging table name, " + etlConf.sourceTableName)
    else if (!hiveCtx.tableNames.contains(etlConf.targetTableName))
      throw new IllegalArgumentException("Invalid target table name, " + etlConf.targetTableName)
    else {
      val srcColumns = hiveCtx.table(etlConf.sourceTableName).columns
      if(!srcColumns.contains(etlConf.sourcePartitionCol))
        throw new IllegalArgumentException("Invalid staging column name, " + etlConf.sourcePartitionCol)
    }
  }

  def getQueryResults(queryString: String, hiveCtx: HiveContext) : DataFrame = {
    hiveCtx.sql(queryString)
  }
}
