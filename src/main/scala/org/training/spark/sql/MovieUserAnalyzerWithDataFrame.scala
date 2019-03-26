package org.training.spark.sql

import org.apache.spark._
import org.apache.spark.sql.{SparkSession, Row}

/**
 * 看过“Lord of the Rings, The (1978)”用户年龄和性别分布
 */
object MovieUserAnalyzerWithDataFrame {
  def main(args: Array[String]) {
    var dataPath = "data/ml-1m"
    val conf = new SparkConf()
    if(args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val spark = SparkSession
        .builder()
        .appName("MovieUserAnalyzerWithDataFrame")
        .config(conf)
        .getOrCreate()

    val sc = spark.sparkContext

    /**
     * Step 1: Create RDDs
     */
    val DATA_PATH = dataPath
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"
    val MOVIE_ID = "2116"

    val usersRdd = sc.textFile(DATA_PATH + "/users.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "/ratings.dat")

    /**
     * Step 2: Transform to DataFrame
      * 反射方式
      * 1).定义case class，作为RDD的schema
      * 2).直接通过RDD.toDF将RDD转换为DataFrame
     */
    import org.apache.spark.sql.types.{StructType,StructField,StringType}

    val userSchemaString = "userid gender age"
    val schema = StructType(userSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val userRDD = usersRdd.map(_.split("::")).map(p => Row(p(0), p(1).trim, p(2).trim))
    val userDataFrame = spark.createDataFrame(userRDD, schema)

    val ratingSchemaString = "userid movieid"
    val ratingSchema = StructType(ratingSchemaString.split(" ")
        .map(fieldName => StructField(fieldName, StringType, true)))
    val ratingRDD = ratingsRdd.map(_.split("::")).map(p => Row(p(0), p(1).trim))
    val ratingDataFrame = spark.createDataFrame(ratingRDD, ratingSchema)

    /**
     * Step 3: process & print result
     */

    // use rdd //  use dataframe
    ratingDataFrame.filter(s"movieid = ${MOVIE_ID}").//获取打分表中所有看过 ${MOVIE_ID} 的用户，评分列表
        join(userDataFrame, "userid").//从用户表中join出这部分用户，用户数据包含 用户id  age  gender
        select("gender", "age").
        groupBy("gender", "age").
        count().
        collect().foreach(println(_))

    //    spark.sql
    userDataFrame.createOrReplaceTempView("users")
    ratingDataFrame.createOrReplaceTempView("rating")
    spark.sql("SELECT gender, age, count(*) from users as u join rating  as r " +
        s"on u.userid = r.userid where movieid = ${MOVIE_ID} group by gender, age").
        collect().foreach(println(_))

    sc.stop()
  }
}