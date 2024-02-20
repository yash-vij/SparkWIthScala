package UDFs

import org.apache.spark.sql.functions.udf

object ParameterUDF {
  val oneParaUDF = udf((x:Int) => x+23)
  val twoParaUDF = udf((x:String,y:Int) => x.length+y)
}
