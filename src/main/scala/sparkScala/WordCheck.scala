package sparkScala

import org.apache.spark.sql.Dataset

object WordCheck {
  def wordCheck(df:Dataset[String],word:String): Long = {
     val lines = df.filter(line => line.contains(word)).count()
    lines
  }

}
