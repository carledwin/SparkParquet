import org.apache.spark.sql.SparkSession

object SparkReadParquet {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession
      .builder()
      .appName("SparkReadParquet")
      .config("spark.master", "local")
      .getOrCreate()

    val pathParquetToRead = "C:\\Users\\carli\\IdeaProjects\\SparkParquet\\src\\main\\resources\\people\\part-00000-222c175e-1cbf-4e44-89c3-efb9a154c8c0-c000.snappy.parquet"

    val dfPeople = spark.read.parquet(pathParquetToRead)
    dfPeople.show

  }
}
