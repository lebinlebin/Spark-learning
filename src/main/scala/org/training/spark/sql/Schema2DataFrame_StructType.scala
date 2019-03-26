package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * 测试为dataFrame指定schema的各种方式
  * 1.  直接手动确定：
  *     peopleRDD.map{x =>
  *     val para = x.split(",")
  *     (para(0), para(1).trim.toInt)
  *     }.toDF("name","age")
  * 2. 通过反射确定  （利用case class 的功能）
  *     case class People(name:String, age:Int)
  *         peopleRdd.map{ x =>
  *         val para = x.split(",")
  *         People(para(0),para(1).trim.toInt)
  *     }.toDF
  * 3. 通过编程方式来确定  StructType
  *     1）准备Scheam
  *     val schema = StructType( StructField("name",StringType):: StructField("age",IntegerType)::Nil )
  *     2）准备Data   【需要Row类型】
  *     val data = peopleRdd.map{ x =>
  *     val para = x.split(",")
  *     Row(para(0),para(1).trim.toInt)
  *     }
  *     3）生成DataFrame
  *     val dataFrame = spark.createDataFrame(data, schema)
  */
object Schema2DataFrame_StructType {

    def main(args: Array[String]): Unit = {

        //创建SparkConf()并设置App名称
        val sparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")

        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        val sc = spark.sparkContext

        import spark.implicits._
        // Create an RDD
        val peopleRDD = spark.sparkContext.textFile("data/people.txt")

        // The schema is encoded in a string,应该是动态通过程序生成的
        val schemaString = "name age"

        // Generate the schema based on the string of schema   Array[StructFiled]
        val fields = schemaString.split(" ")
            .map(fieldName => StructField(fieldName, StringType, nullable = true))

        // val filed = schemaString.split(" ").map(filename=>
        // filename match{ case "name"=> StructField(filename,StringType,nullable = true);
        // case "age"=>StructField(filename, IntegerType,nullable = true)} )

        val schema = StructType(fields)

        // Convert records of the RDD (people) to Rows
        import org.apache.spark.sql._
        val rowRDD = peopleRDD
            .map(_.split(","))
            .map(attributes => Row(attributes(0), attributes(1).trim))

        // Apply the schema to the RDD
        val peopleDF = spark.createDataFrame(rowRDD, schema)

        // Creates a temporary view using the DataFrame
        peopleDF.createOrReplaceTempView("people")

        // SQL can be run over a temporary view created using DataFrames
        val results = spark.sql("SELECT name FROM people")

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        results.map(attributes => "Name: " + attributes(0)).show()
        // +-------------+
        // |        value|
        // +-------------+
        // |Name: Michael|
        // |   Name: Andy|
        // | Name: Justin|
        // +-------------+
        sc.stop()
    }


}
