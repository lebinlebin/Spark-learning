package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
object Schema2DataFrame_reflect {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {

        //创建SparkConf()并设置App名称
        val sparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")

        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        val sc = spark.sparkContext

        import spark.implicits._




        /**
          * 创建DataSet
          *Dataset是具有强类型的数据集合，需要提供对应的类型信息。
          */

        val caseClassDS = Seq(Person("Andy", 32)).toDS()
        caseClassDS.show()
        val primitiveDS = Seq(1, 2, 3).toDS()
        primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

        val path = "data/people.json"
        val peopleDS = spark.read.json(path).as[Person]
        peopleDS.show()
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+


        /**
          * 通过反射获取Scheam
          * SparkSQL能够自动将包含有case类的RDD转换成DataFrame，case类定义了table的结构，
          * case类属性通过反射变成了表的列名。Case类可以包含诸如Seqs或者Array等复杂的结构
          */


        // Create an RDD
        val peopleDF  = spark.sparkContext.textFile("data/people.txt").map(_.split(","))
            .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
            .toDF()

        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people")

        // SQL statements can be run by using the sql methods provided by Spark
        val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

        // The columns of a row in the result can be accessed by field index  ROW object
        teenagersDF.map(teenager => "Name: " + teenager(0)).show()
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+

        // or by field name
        teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+

        // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
        implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
        // Primitive types and case classes can be also defined as
        // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

        // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
        teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
        // Array(Map("name" -> "Justin", "age" -> 19))



        sc.stop()
    }


}
