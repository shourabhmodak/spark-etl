import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

object DataLoader {

  def loadDataFromCSV(sqlContext: HiveContext, inputFile: String): DataFrame = {

    sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .load(inputFile)
  }

  def loadDataFromCSVWithSchema(sqlContext: HiveContext, inputFile: String, schema: StructType): DataFrame = {

    sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(schema)
      .load(inputFile)

  }

}
