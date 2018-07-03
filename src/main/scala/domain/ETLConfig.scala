package domain

case class ETLConfig (sourceTableName: String,
                      targetTableName: String,
                      sourcePartitionCol:String,
                      keyColumns: Seq[String])

