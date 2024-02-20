package sparkScala

import org.apache.spark.rdd.RDD

object WordCount {
  def WordCount(data : RDD[String]): Unit = {
    val lines = data.flatMap(f => f.split(" "))
    val words = lines.map(l =>(l,1))
    val wordsCount = words.reduceByKey(_ + _)
    wordsCount.foreach(println)

    }
}
