import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

case class IndustrySurveys(Year: Integer,
                           Industry_aggregation_NZSIOC: String,
                           Industry_code_NZSIOC: String,
                           Industry_name_NZSIOC: String,
                           Units: String,
                           Variable_code: String,
                           Variable_name: String,
                           Variable_category: String,
                           Value: Integer,
                           Industry_code_ANZSIC06: String)

object CreatingDatasetWithCSVFile extends App {
  val ss = SparkSession.builder()
    .appName("Creating Dataset")
    .master("local")
    .getOrCreate()

  val properties = Map("header" -> "true", "inferSchema" -> "true")

  import ss.implicits._

  val ownSchema: StructType = ScalaReflection.schemaFor[IndustrySurveys].dataType.asInstanceOf[StructType]

  val dataset = ss.read
    .options(properties)
    .schema(ownSchema)
    .csv("/Users/sameer.jain/Downloads/annual-enterprise-survey-2018-financial-year-provisional-csv.csv")
    .as[IndustrySurveys]

  dataset.printSchema()
  dataset.show(20)

  val filteredDataset = dataset.filter(row => row.Value < 300)
  filteredDataset.printSchema()
  filteredDataset.show(20)
}
