package UDFs
import org.apache.spark.sql.functions.udf
import sparkScala.SparkCreation._
object MyFirstUDF {
  val random = udf(() => Math.random())     // udf() -> indicates it is a user defined function
}
