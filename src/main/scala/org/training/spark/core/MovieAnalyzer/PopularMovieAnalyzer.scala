package org.training.spark.core.MovieAnalyzer

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
 * 年龄段在“18-24”的男性年轻人，最喜欢看哪10部电影
  * - Age is chosen from the following ranges:
  *
  * *  1:  "Under 18"
  * * 18:  "18-24"
  * * 25:  "25-34"
  * * 35:  "35-44"
  * * 45:  "45-49"
  * * 50:  "50-55"
  * * 56:  "56+"
 */
object PopularMovieAnalyzer {

  def main(args: Array[String]) {
    var dataPath = "data/ml-1m"
    val conf = new SparkConf().setAppName("PopularMovieAnalyzer")
    if(args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    /**
     * Step 1: Create RDDs
     */
    val DATA_PATH = dataPath
    val USER_AGE = "18"

    val usersRdd = sc.textFile(DATA_PATH + "/users.dat")
    val moviesRdd = sc.textFile(DATA_PATH + "/movies.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "/ratings.dat")

    /**
     * Step 2: Extract columns from RDDs
     */
    //users: RDD[(userID, age)]
    val users = usersRdd.map(_.split("::")).map { x =>
      (x(0), x(2))
    }.filter(_._2.equals(USER_AGE))  //获取年龄段在18-24的用户列表

    //Array[String]  获取到本地组成一个数组  获取年龄段在18-24的用户列表
    val userlist = users.map(_._1).collect()//userID  list

    //注意：HashSet()后面要带小括号
    //broadcast
    val userSet = HashSet() ++ userlist
    val broadcastUserSet = sc.broadcast(userSet)

    /**
     * Step 3: map-side join RDDs
      * * ratings.dat
      * * UserID::MovieID::Rating::Timestamp
     */
      //topKmovies: RDD[(userID, MovieID)]
    val topKmovies = ratingsRdd.map(_.split("::")).map { x =>
      (x(0), x(1))
    }.filter { x =>
      broadcastUserSet.value.contains(x._1)//从ratingsRdd中获取在userSet中存在的userid
    }.map { x =>
      (x._2, 1)
    }.reduceByKey(_ + _)//(movieid,次数)
     .map { x =>(x._2, x._1)     //.sortBy(_._2,false).take(10)  //(movieid,次数)
    }.sortByKey(false).map { x =>
      (x._2, x._1)
    }.take(10)

    /**
     * Transfrom filmID to fileName
      * * movies.dat
      * * MovieID::Title::Genres
     */
    val movieID2Name = moviesRdd.map(_.split("::")).map { x =>
      (x(0), x(1))//(MovieID::Title)
    }.collect().toMap//(K,V)
                        //获取Title，如果没有就为null  ，观看的次数
    topKmovies.map(x => (movieID2Name.getOrElse(x._1, null), x._2))
        .foreach(println)

    println(System.currentTimeMillis())

    sc.stop()
  }
}
