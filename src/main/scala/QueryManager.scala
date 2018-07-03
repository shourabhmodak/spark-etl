object QueryManager {

  def getSrcLatestDataQuery(srcTableName: String, srcPartCol: String) : String = {
    """
      | SELECT * FROM $srcTableName
      | WHERE $srcPartCol = (SELECT MAX($srcPartCol) FROM $srcTableName)
    """.stripMargin
  }
}
