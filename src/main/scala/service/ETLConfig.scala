package service

import cats.data.Xor
case class ETLConfig (sourceTableName: String,
                      targetTableName: String,
                      sourcePartitionCol:String,
                      keyColumns: Seq[String])

object ETLConfig {
  private def requiredEnvVar(key: String): String Xor String =
    Xor.fromOption(Option(System.getenv(s"SPARK_ETL_$key")), s"Required SPARK_ETL_$key environment variable is missing")

  def getConfig: String Xor ETLConfig =
    for {
      sourceTableName <- requiredEnvVar("SRC_TABLE_NAME").map(x => x)
      targetTableName <- requiredEnvVar("TGT_TABLE_NAME").map(x => x)
      sourcePartitionCol <- requiredEnvVar("SRC_PARTITION_COL").map(x => x)
      keyColumns <- requiredEnvVar("KEY_COLS").map(x => x.split(","))
    } yield ETLConfig (
      sourceTableName,
      targetTableName,
      sourcePartitionCol,
      keyColumns
    )

}

