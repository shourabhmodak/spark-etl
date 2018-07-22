object QueryManager {

  def getSrcLatestDataQuery(stgTableName: String, stgPartCol: String) : String = {
    /*s"""
      | SELECT * FROM $stgTableName
      | WHERE $stgPartCol IN (SELECT MAX($stgPartCol) AS MAX_SRC_DT FROM $stgTableName)
    """.stripMargin*/
    s"SELECT * FROM $stgTableName AS STG, (SELECT MAX($stgPartCol) AS MAX_SRC_DT FROM $stgTableName) MDT WHERE STG.$stgPartCol = MDT.MAX_SRC_DT"
  }

  def getTgtBaseDataQuery(tgtTableName: String) : String = {
    s"SELECT * FROM $tgtTableName"
  }

  def getStgDedupedDataQuery(stgTableName: String, srcPartitionCol: String, tgtCols: Array[String],
                             keyCols: Seq[String]) : String = {
    "SELECT "
      .concat(tgtCols.map(tgtCol => s"stgo.$tgtCol").mkString(" , "))
      .concat(" FROM (SELECT stg.*, row_number() OVER (PARTITION BY ")
      .concat(keyCols.map(keyCol => s"$keyCol")
        .mkString(" , "))
      .concat(s" ORDER BY $srcPartitionCol DESC) AS rnm FROM $stgTableName as stg) stgo WHERE rnm = 1")
  }

  def getUnchangedTgtDataQuery(stgTableName: String, tgtTableName: String, keyCols: Seq[String]) : String = {
    s"SELECT tgt.* FROM $tgtTableName as tgt LEFT OUTER JOIN $stgTableName as stg ON "
      .concat(keyCols.map(keyCol => s"tgt.$keyCol = stg.$keyCol")
        .mkString(" AND "))
      .concat(" WHERE ")
      .concat(keyCols.map(keyCol => s"stg.$keyCol IS NULL")
        .mkString(" AND "))
  }

  def getNewModifiedStgDataQuery(stgTableName: String, tgtTableName: String, tgtCols: Array[String],
                                 keyCols: Seq[String]) : String = {
    "SELECT "
      .concat(tgtCols.map(tgtCol => s"stg.$tgtCol").mkString(" , "))
      .concat(s" FROM $stgTableName stg")
  }
}
