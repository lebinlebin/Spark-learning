package org.training.spark.sql

/**
  * 开窗函数，求每个班级学生最高分数的学生信息(每个班级最高分数不止一个)
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Score(name: String, clazz: Int, score: Int)

object OverFunction extends App {

  val sparkConf = new SparkConf().setAppName("over").setMaster("local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._
  println("//***************  原始的班级表  ****************//")
  val scoreDF = spark.sparkContext.makeRDD(Array( Score("a", 1, 80),
                                                  Score("b", 1, 78),
                                                  Score("c", 1, 95),
                                                  Score("d", 2, 74),
                                                  Score("e", 2, 92),
                                                  Score("f", 3, 99),
                                                  Score("g", 3, 99),
                                                  Score("h", 3, 45),
                                                  Score("i", 3, 55),
                                                  Score("j", 3, 78))).toDF("name","class","score")
  scoreDF.createOrReplaceTempView("score")
  scoreDF.show()

  println("//***************  求每个班最高成绩学生的信息  ***************/")
  println("    /*******  开窗函数的表  ********/")
  spark.sql("select name,class,score, rank() over(partition by class order by score desc) rank from score").show()

  println("    /*******  计算结果的表  *******")
  spark.sql("select * from " +
    "( select name,class,score,rank() over(partition by class order by score desc) rank from score) " +
    "as t " +
    "where t.rank=1").show()

  //spark.sql("select name,class,score,row_number() over(partition by class order by score desc) rank from score").show()

  println("/**************  求每个班最高成绩学生的信息（groupBY）  ***************/")

  spark.sql("select class, max(score) max from score group by class").show()

  spark.sql("select a.name, b.class, b.max from score a, " +
    "(select class, max(score) max from score group by class) as b " +
    "where a.score = b.max  and a.class = b.class").show()


  println("rank（）跳跃排序，有两个第二名时后边跟着的是第四名\n" +
    "dense_rank() 连续排序，有两个第二名时仍然跟着第三名\n" +
    "over() 开窗函数：\n" +
    "       在使用聚合函数后，会将多行变成一行，而开窗函数是将一行变成多行；\n" +
    "       并且在使用聚合函数后，如果要显示其他的列必须将列加入到group by中，\n" +
    "       而使用开窗函数后，可以不使用group by，直接将所有信息显示出来。\n" +
    "        开窗函数适用于在每一行的最后一列添加聚合函数的结果。\n" +
    "常用开窗函数：\n" +
    "   1.为每条数据显示聚合信息.(聚合函数() over())\n" +
    "   2.为每条数据提供分组的聚合函数结果(聚合函数() over(partition by 字段) as 别名) \n" +
    "         --按照字段分组，分组后进行计算\n" +
    "   3.与排名函数一起使用(row number() over(order by 字段) as 别名)\n" +
    "常用分析函数：（最常用的应该是1.2.3 的排序）\n" +
    "   1、row_number() over(partition by ... order by ...)\n" +
    "   2、rank() over(partition by ... order by ...)\n" +
    "   3、dense_rank() over(partition by ... order by ...)\n" +
    "   4、count() over(partition by ... order by ...)\n" +
    "   5、max() over(partition by ... order by ...)\n" +
    "   6、min() over(partition by ... order by ...)\n" +
    "   7、sum() over(partition by ... order by ...)\n" +
    "   8、avg() over(partition by ... order by ...)\n" +
    "   9、first_value() over(partition by ... order by ...)\n" +
    "   10、last_value() over(partition by ... order by ...)\n" +
    "   11、lag() over(partition by ... order by ...)\n" +
    "   12、lead() over(partition by ... order by ...)\n" +
    "lag 和lead 可以 获取结果集中，按一定排序所排列的当前行的上下相邻若干offset 的某个行的某个列(不用结果集的自关联）；\n" +
    "lag ，lead 分别是向前，向后；\n" +
    "lag 和lead 有三个参数，第一个参数是列名，第二个参数是偏移的offset，第三个参数是 超出记录窗口时的默认值")

  spark.stop()
}
