import org.apache.spark.sql.SparkSession
import SparkCreation._
import WordCheck._
import scala.io.StdIn.readLine
object SparkScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkCreation.SparkInitial("MyApp")
    val location = "src/main/resources"
    val df = spark.read.textFile(s"$location/data.txt")
    //println("Enter the word you want to find count for : ")
    //val word = readLine()
    val wordCount = WordCheck.wordCheck(df,"and")
    print(s"Count of and is $wordCount")

  }
}
