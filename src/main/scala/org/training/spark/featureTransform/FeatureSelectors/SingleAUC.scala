package org.training.spark.featureTransform.FeatureSelectors

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object SingleAUC {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder()
            .appName("SingleAUC")
            .getOrCreate()

        val sc = spark.sparkContext
        val inputpath = "/home/hdp_lbg_ectech/resultdata/strategy/ads/escRealtimeSamples2/20190516"
        val outputpath = "/home/hdp_lbg_ectech/resultdata/ctr/ershouche/singleAUC/20190518realtimefeatures/"
//        val outputpath = "data2/"

        val items1 = Array("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20",
            "21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40",
            "41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60",
            "61","62","63","64","65","66","67","68","69","70","71","72","73","76","77","76","77","78","79","80",
            "81","82","83","84","85","86","87"
        )
        val items2 = Array("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20",
            "21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40",
            "41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60",
            "61","62","63","64","65","66","67","68","69","70","71","72","73","76","77","76","77","78","79","80",
            "81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100",
            "101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120",
            "121","122","123","124","125","126","127","128","129","130","131","132","133","134","135","136","137","138","139","140",
            "141","142","143","144","145","146","147","148","149","150","151"
        )
        val data = sc.textFile(inputpath)
            .map(_.split("\t")) //是一个数组
        val roclist = new ListBuffer[String]()
        val test = data.map(_.drop(1))
        val test2 = data.map(_.drop(1)(0)) //取到数组的第一个,实际的数据字符串
//        val pw = new PrintWriter("data/output")
        for (i <- items2) {
            val item = i.toInt
            val feature_valuecnt_all = test.map(fields => (fields(item), 1)).reduceByKey((x, y) => x + y)
            val feature_valuecnt_pos = test.map(fields => (fields(item), fields(0).toInt))
                .filter(_._2 == 1).reduceByKey((x, y) => x + y)
            val combine_valuecnt = feature_valuecnt_all.leftOuterJoin(feature_valuecnt_pos)
                .map(x =>
                    (x._1, (x._2._1.toInt, x._2._2 match {
                        case Some(n) => n
                        case None => 0
                    }))
                )
            val pos_sample_pro = combine_valuecnt.map(x => (x._1, x._2._2 / (x._2._1).toFloat))
            val all_sample_pro = test.map(fields => (fields(item), fields(0)))
                .leftOuterJoin(pos_sample_pro)
                .map(x => (x._2._2 match {
                    case Some(n) => n.toDouble
                    case None => 0.0
                }, x._2._1.toDouble))
            val metrics = {
                new BinaryClassificationMetrics(all_sample_pro)
            }
            val roc = i.toString + ":" + metrics.areaUnderROC.toString
//            print(roc)
            roclist.append(roc)
        }
        val rdd = sc.makeRDD(roclist)
        rdd.coalesce(1).saveAsTextFile(outputpath)
    }
}

