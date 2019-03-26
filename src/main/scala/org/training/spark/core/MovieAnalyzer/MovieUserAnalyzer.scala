package org.training.spark.core.MovieAnalyzer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 看过“Lord of the Rings, The (1978)”用户的年龄和性别分布
 */
object MovieUserAnalyzer {

  def main(args: Array[String]) {
    var dataPath = "data/ml-1m"
    val conf = new SparkConf().setAppName("PopularMovieAnalyzer")
    if(args.length > 0) {
      dataPath = args(0)//如果程序命令行有输入路径就用命令行输入的地址为路径
    } else {
      conf.setMaster("local[1]")
    }

    val spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
    val sc = spark.sparkContext
    /**
     * Step 1: Create RDDs
      * users.dat
      * UserID::Gender::Age::Occupation::Zip-code
      *
      * movies.dat
      * MovieID::Title::Genres
      *
      * ratings.dat
      * UserID::MovieID::Rating::Timestamp
     */
    val DATA_PATH = dataPath
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"
    val MOVIE_ID = "2116"

    val usersRdd = sc.textFile(DATA_PATH + "/users.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "/ratings.dat")
    print (usersRdd.count())
    //笔记：必须有schema才能转换为DataFrame
    //先给RDD加上schema才能转换为 DataFrame   DataFrame=RDD+Schema
    /**
      *  笔记： 要先 import  spark.implicits._  才能执行 .toDF()的函数操作
      */

    /**
     * Step 2: Extract columns from RDDs
     */
    //users: RDD[(userID, (gender, age))]   K,V
    val users = usersRdd.map(_.split("::")).map { x =>
      (x(0), (x(1), x(2)))
    }

    //rating: RDD[Array(userID, movieID, ratings, timestamp)]
    val rating = ratingsRdd.map(_.split("::"))

    //usermovie: RDD[(userID, movieID)]
    val usermovie = rating.map { x =>  //ratings.dat=>UserID::MovieID::Rating::Timestamp
      (x(0), x(1))
    }.filter(_._2.equals(MOVIE_ID))//RDD[(userID, movieID)] 第二个维度.equals(MOVIE_ID)

    /**
     * Step 3: join RDDs
     */
    //useRating: RDD[(userID, (movieID, (gender, age))]       usermovie: RDD[(userID, movieID)]
    val userRating = usermovie.join(users)//users: RDD[(userID, (gender, age))]   K,V
    //根据userID进行join   获得看过MOVIE_ID=2116 的 [(userID, (gender, age))] 用户年龄和性别

    //userRating.take(1).foreach(print)

    //movieuser: RDD[(movieID, (movieTile, (gender, age))]
    val userDistribution = userRating.map { x =>  //useRating: RDD[(userID, (movieID, (gender, age))]
      (x._2._2, 1)//age,1
    }.reduceByKey(_ + _)//以x._2._2即(gender, age)为key进行累计,获得相同age的用户数目

     userDistribution.foreach(println)
      println("=========================================")
    userDistribution.collect.foreach(println)

    sc.stop()
  }
}
