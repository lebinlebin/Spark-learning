package org.training.spark.core.spark_core

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * * 格式 ：timestamp province city userid adid
  * * 某个时间点 某个省份 某个城市 某个用户 某个广告
  * 1516609143867 6 7 64 16
  * 1516609143869 9 4 75 18
  * * 用户ID范围: 0 - 99
  * * 省份、城市 ID相同 ： 0 - 9
  * * adid: 0 - 19
  *
  * 需求：统计每一个省份点击TOP3的广告ID
  *
  * 需求：统计每一个省份每一个小时的TOP3广告的ID
  */
object ad_statistic {
    case class Click(time:Long,
                     province:Int,
                     city:Int,
                     userid:Int,
                     adid:Int)
    def main(args: Array[String]): Unit = {

        //sparkConf
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("practice")
        val sc = new SparkContext(sparkConf)

        //载入数据
        val click:RDD[String] =
            sc.textFile("data/agent.log")
        //    val click:RDD[String] = sc.textFile("..\\data\\agent.log")


        val clickRDD: RDD[Click] = click.map{ item =>
            //      blanklines.add(1)
            val param = item.split(" ")
            Click(param(0).toLong, param(1).toInt, param(2).toInt, param(3).toInt, param(4).toInt)
        }

        //    blanklines.value

        clickRDD.cache()

        //统计每一个省份点击TOP3的广告ID
        val proAndAd2CountRDD:RDD[(String,Int)] = clickRDD.map(click => (click.province + "_" + click.adid, 1))

        val proAndAd2CountsRDD:RDD[(String,Int)] = proAndAd2CountRDD.reduceByKey(_+_)

        val pro2AdCountsRDD = proAndAd2CountsRDD.map{ item =>
            val para = item._1.split("_")
            (para(0).toInt, (para(1).toInt, item._2))//( province,(adid,clickcount)  )
        }


        val pro2AdsRDD:RDD[(Int,Iterable[(Int,Int)])] = pro2AdCountsRDD.groupByKey()
        val result = pro2AdsRDD.mapValues{items =>
            items.toList.sortWith(_._2 > _._2).take(3)
        }

        println(result.collect())

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

        // pro-hour-adid
        val proAndTimeAndAd2CountRDD:RDD[(String,Int)] = clickRDD.map{click =>
            (click.province + "_" + simpleDateFormat.format(new Date(click.time)) + "_" + click.adid, 1)
        }

        val result2 = proAndTimeAndAd2CountRDD.reduceByKey(_+_).map{item =>
            val para = item._1.split("_")
            (para(0) + "_" + para(1), (para(2).toInt, item._2))
        }.groupByKey().mapValues{items =>
            items.toList.sortWith(_._2 > _._2).take(3)
        }.map{ item =>
            val para = item._1.split("_")
            (para(0), (para(1), item._2))
        }.groupByKey()

        print(result2.collect())

        sc.stop()
    }
}
