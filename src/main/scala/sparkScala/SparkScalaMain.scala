package sparkScala
import WordCount._
import caseClassFiles.Person
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import UDFs.MyFirstUDF._
import UDFs.ParameterUDF.{oneParaUDF, twoParaUDF}
import org.apache.spark._

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

    //Creating a DataSet
    val caseClassDS = spark.createDataFrame(Seq(Person("Andy", 32)))
    caseClassDS.show()

    val peopleData = spark.sparkContext.textFile(s"$location/people.txt")
    val peopleDF = spark.createDataFrame(peopleData.map(_.split(",")).map(data => Person(data(0),data(1).trim.toInt)))
    peopleDF.show()

    //Programmatically Specifying the Schema

    val schemaString = "NAME_ AGE_"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName,StringType, nullable = true))
    val schema = StructType(fields)

    //convert rdd people to rows
    val rowRDD = peopleData.map(_.split(" ")).map(par => Row(par(0),par(1).trim))
    //Apply schema
    val ownSchemaDF = spark.createDataFrame(rowRDD,schema)
    ownSchemaDF.createOrReplaceTempView("People")

    val res = spark.sql("select * from People")
    println(res.show())

    //UDFs
    // Register the udf in spark first
    spark.udf.register("random",random.asNondeterministic())
    spark.sql("select random()").show()

    //UDF with 1 parameter
    println("UDF with 1 parameter : ")
    spark.udf.register("oneParaUDF",oneParaUDF)
    spark.sql("select oneParaUDF(12)").show()

    //UDF with 2 parameter
    println("UDF with 2 parameter : ")
    spark.udf.register("twoParaUDF",twoParaUDF)
    spark.sql("select twoParaUDF('Testing',1)").show()


}
