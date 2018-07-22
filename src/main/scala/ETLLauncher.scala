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
          val targetDF = generateTargetData(config, hiveContext)
          targetDF.write.format("orc").mode("overwrite").insertInto(config.targetTableName)
        } catch {
          case ex: Exception =>
            log.error(s"Error in Spark ETL : ${ex.getMessage} :  ${ex.printStackTrace()}")
        }
      }
    )
  }

  def generateTargetData(etlConfig: ETLConfig, hiveCtx: HiveContext): DataFrame = {
    if (checkConfigParams(etlConfig, hiveCtx)) {
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
      finalTargetDF

    } else {
      val errorMsg = "Error in Spark ETL, config parameters verification failed!"
      log.error(errorMsg)
      throw new RuntimeException(errorMsg)
    }
  }

  def checkConfigParams(etlConf: ETLConfig, hiveCtx: HiveContext) : Boolean = {
    val checksPassed = true

    if (!hiveCtx.tableNames.contains(etlConf.sourceTableName.toLowerCase))
      throw new IllegalArgumentException("Invalid staging table name, " + etlConf.sourceTableName)
    else if (!hiveCtx.tableNames.contains(etlConf.targetTableName.toLowerCase))
      throw new IllegalArgumentException("Invalid target table name, " + etlConf.targetTableName)
    else {
      val srcColumns = hiveCtx.table(etlConf.sourceTableName).columns
      if(!srcColumns.contains(etlConf.sourcePartitionCol))
        throw new IllegalArgumentException("Invalid staging column name, " + etlConf.sourcePartitionCol)
    }
    checksPassed
  }

  def getQueryResults(queryString: String, hiveCtx: HiveContext) : DataFrame = {
    println(s"Executing : $queryString")
    hiveCtx.sql(queryString)
  }
}
