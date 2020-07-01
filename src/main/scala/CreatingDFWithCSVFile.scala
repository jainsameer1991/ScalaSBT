import CreatingDatasetWithCSVFile.{properties, ss}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object CreatingDFWithCSVFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Creating Data Frame with CSV file")
      .getOrCreate()

    val properties = Map("header" -> "true", "inferSchema" -> "true")

    val ownSchema = StructType(
      StructField("Year", IntegerType, nullable = true) ::
        StructField("Industry_aggregation_NZSIOC", StringType, nullable = true) ::
        StructField("Industry_code_NZSIOC", StringType, nullable = true) ::
        StructField("Industry_name_NZSIOC", StringType, nullable = true) ::
        StructField("Units", StringType, nullable = true) ::
        StructField("Variable_code", StringType, nullable = true) ::
        StructField("Variable_name", StringType, nullable = true) ::
        StructField("Variable_category", StringType, nullable = true) ::
        StructField("Value", LongType, nullable = true) ::
        StructField("Industry_code_ANZSIC06", StringType, nullable = true) :: Nil
    )

    val dataFrame = spark.read
      .options(properties)
      .schema(ownSchema)
      .csv("/Users/sameer.jain/Downloads/annual-enterprise-survey-2018-financial-year-provisional-csv.csv")

    dataFrame.printSchema()
    dataFrame.show(10)

    dataFrame.createTempView("survey_table")

    // create or replace table in memory, else will throw an error that table with <name> already exists in memory
//    dataFrame.createOrReplaceTempView("survey_table")


    val filteredSurveyTable = spark.sql("select `Industry_code_NZSIOC`, `Industry_name_NZSIOC`, `Value`  from `survey_table` where `Value` < 300")
    filteredSurveyTable.show(20)

  }

}
