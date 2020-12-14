import org.apache.spark.sql.SparkSession

object SparkWriteParquet {

  def main(args: Array[String]): Unit ={

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession
      .builder()
      .appName("SparkWriteParquet")
      .config("spark.master", "local")
      .getOrCreate()

    val pathJson = "file:/C:/Users/carli/IdeaProjects/SparkParquet/src/main/resources/dataframes/people.json"
    val pathJson2 = "C:\\Users\\carli\\IdeaProjects\\SparkParquet\\src\\main\\resources\\dataframes\\people.json"

    val dfPeople = spark.read.json(pathJson)
    dfPeople.show

    val pathParquetToWrite = "people" //criara na raiz do projeto
    val pathParquetToWrite2 = "src/main/resources/people" //criara na pasta indicada

    dfPeople.select("id", "name").write.format("parquet").save(pathParquetToWrite2)
  }
}
