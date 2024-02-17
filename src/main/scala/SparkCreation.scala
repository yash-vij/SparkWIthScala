import org.apache.spark.sql.SparkSession
object SparkCreation {
  def SparkInitial(appName: String): SparkSession = {
    val spark = SparkSession.builder().config("spark.master", "local").appName(appName).getOrCreate()
    return spark
  }
}
