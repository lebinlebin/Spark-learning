package org.training.spark.recommondByCtrModel.AUC

import java.io.PrintWriter

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class RecommonderSingleAUC {

}

object RecommonderSingleAUC {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        val inputpath = "/home/hdp_lbg_ectech/resultdata/strategy/realtime_feature/offline_megedlog/ESCfeatures20190314/"
//        val outputpath = "/home/hdp_lbg_ectech/resultdata/ctr/ershouche/singleAUC"
        val outputpath = "data_ctr_model/data2/"

        val items = Array("2","3")
        //按照\t做分割切分，得到的是label和features字符串
        //1	Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id889*hitop_id166:1;User.Item*Item,hitop_id46*hitop_id166:1;User.Item*Item,hitop_id985*hitop_id166:1;User.phone*Item,device_name1591*hitop_id166:1;User.pay*Item.price,pay_ability0*9290:1
        val data = sc.textFile("data_ctr_model/escsampleAUCtest")
            .map(_.split("\t")) //是一个数组

        val roclist = new ListBuffer[String]()

        //data为    1 \t  Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id889*hitop_id166:1;User.Item*Item,hitop_id46*hitop_id166:1;User.Item*Item,hitop_id985*hitop_id166:1;User.phone*Item,device_name1591*hitop_id166:1;User.pay*Item.price,pay_ability0*9290:1
        //        val res =
        //统计所有的特征  先把label切掉
        val test = data.map(_.drop(1))
        //drop(1) 之后还是一个数组格式，因为data本来就是一个数组格式，数组格式不能直接打印，需要指定下标才能打印。数组这里其实就只有一个元素
        //                test.take(1).foreach(println(_))//数组  一条数据就一个样本=>其实就是一条样本  [Ljava.lang.String;@5a00eb1e
        val test2 = data.map(_.drop(1)(0)) //取到数组的第一个,实际的数据字符串
        //                println("###########################################################")
        //                test2.take(100).foreach(println(_))
        val pw = new PrintWriter("data_ctr_model/output")
        for (i <- items) {
            val item = i.toInt

            val feature_valuecnt_all = test.map(fields => (fields(item), 1)).reduceByKey((x, y) => x + y)
            //            feature_valuecnt_all.take(10).foreach(println(_))
            val feature_valuecnt_pos = test.map(fields => (fields(item), fields(0).toInt))
                .filter(_._2 == 1).reduceByKey((x, y) => x + y)

            //            feature_valuecnt_pos.take(10).foreach(println(_))

            val combine_valuecnt = feature_valuecnt_all.leftOuterJoin(feature_valuecnt_pos)
                .map(x =>
                    (x._1, (x._2._1.toInt, x._2._2 match {
                        case Some(n) => n
                        case None => 0
                    }))
                )

            //            combine_valuecnt.take(10).foreach(println(_))

            /*
      # 特征名:取值  样本数量     为正例的样本数量
      #   (1:2,       (153,       23))
      #   (1:3,       (154,       54))
      #   (1:5,       (52,        12))
      #   (1:7,       (55,        15))
             */
            val pos_sample_pro = combine_valuecnt.map(x => (x._1, x._2._2 / (x._2._1).toFloat))
            //          pos_sample_pro.take(10).foreach(println(_))
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
            print(roc)
            roclist.append(roc)
        }
        val rdd = sc.makeRDD(roclist)
        //            os.system('hadoop fs -rmr ' + outputpath)
        rdd.coalesce(1).saveAsTextFile(outputpath)
    }
}

