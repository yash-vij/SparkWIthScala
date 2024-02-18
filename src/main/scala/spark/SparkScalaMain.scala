package spark
import WordCount._
object SparkScalaMain extends App{

    val spark = SparkCreation.SparkInitial("MyApp")
    val location = "src/main/resources"
    val df = spark.read.textFile(s"$location/data.txt")
    //println("Enter the word you want to find count for : ")
    //val word = readLine()
    val wordCount = WordCheck.wordCheck(df,"and")
    println(s"Count of and is $wordCount")

    //Parallelize data set
    val array = Array(12,23,234,435,32,65,12)
    val distData = spark.sparkContext.parallelize(array,4)
    println("Sum of arrays is : "+distData.reduce((a,b)=>a+b))

    //Size of File
    val data = spark.sparkContext.textFile(s"$location/data.txt")
    val sizeOfFile = data.map(s => s.length).reduce((a,b)=>a+b)
    println("Size of file is : "+sizeOfFile)

    //Count each word
    println("Count of each word is : "+WordCount(data))




}
