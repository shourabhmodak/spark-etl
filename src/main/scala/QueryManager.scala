object QueryManager {

  def getSrcLatestDataQuery(stgTableName: String, stgPartCol: String) : String = {
    """
      | SELECT * FROM $stgTableName
      | WHERE $srcPartCol = (SELECT MAX($stgPartCol) FROM $srcTableName)
    """.stripMargin
  }

  def getTgtBaseDataQuery(tgtTableName: String) : String = {
    "SELECT * FROM $tgtTableName"
  }

  def getStgDedupedDataQuery(stgTableName: String, srcPartitionCol: String, tgtCols: Array[String],
                             keyCols: Seq[String]) : String = {
    "SELECT "
      .concat(tgtCols.map(tgtCol => "stgo.$tgtCol").mkString(" , "))
      .concat(" FROM (SELECT stg.*, row_number() OVER (PARTITION BY ")
      .concat(keyCols.map(keyCol => "$keyCol")
        .mkString(" , "))
      .concat(" ORDER BY $srcPartitionCol DESC) AS rnm FROM $stgTableName as stg) stgo WHERE rnm = 1")
  }

  def getUnchangedTgtDataQuery(stgTableName: String, tgtTableName: String, keyCols: Seq[String]) : String = {
    "SELECT tgt.* FROM $tgtTableName as tgt LEFT OUTER JOIN $stgTableName as stg ON "
      .concat(keyCols.map(keyCol => "tgt.$keyCol = stg.$keyCol")
        .mkString(" AND "))
      .concat(" WHERE ")
      .concat(keyCols.map(keyCol => "stg.$keyCol IS NULL")
        .mkString(" AND "))
  }

  def getNewModifiedStgDataQuery(stgTableName: String, tgtTableName: String, tgtCols: Array[String],
                                 keyCols: Seq[String]) : String = {
    "SELECT "
      .concat(tgtCols.map(tgtCol => "stg.$tgtCol").mkString(" , "))
      .concat(" FROM $stgTableName")
  }
}
