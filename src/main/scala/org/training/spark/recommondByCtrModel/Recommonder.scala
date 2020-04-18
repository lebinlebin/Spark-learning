package org.training.spark.recommondByCtrModel

import java.io.PrintWriter

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

class Recommonder {

}

object Recommonder{

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        //按照\t做分割切分，得到的是label和features字符串
        //1	Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id889*hitop_id166:1;User.Item*Item,hitop_id46*hitop_id166:1;User.Item*Item,hitop_id985*hitop_id166:1;User.phone*Item,device_name1591*hitop_id166:1;User.pay*Item.price,pay_ability0*9290:1
        val data = sc.textFile("data_ctr_model/000001_0(训练数据格式_输入到Recommonder的文件)").map(_.split("\t"))//是一个数组


        //统计所有的特征  先把label切掉


        //如果用map得到的是RDD[Array[String]],flatmap可以压平，将Array里面String释放出来，这里的map实际把:1去掉
        //去重得到特征的字典映射
        val features: RDD[String] =  data.flatMap(_.drop(1)(0).split(";")).map(_.split(":")(0)).distinct()

        //转成map为了后面得到稀疏向量非零下标用
        val dict: Map[String, Long] =  features.zipWithIndex().collectAsMap()
        //构建labelpoint,分label和vector两部分
        val traindata: RDD[LabeledPoint] = data.map(x=>{
            //得到label，逻辑回归只支持0.0和1.0这里需要转换一下
            val label = x.take(1)(0) match {
                case "-1" => 0.0
                case "1" => 1.0
            }
            // 获得当前样本的每个特征在map中的下标，这些下标的位置是非零的，值统一是1.0
            val index: Array[Int] = x.drop(1)(0).split(";").map(_.split(":")(0)).map(
                fe=>{

                    val index: Long = dict.get(fe) match {
                        case Some(n) => n
                        case None => 0
                    }
                    index.toInt
                }
            )
            //创建一个所有元素是1.0的数组，作为稀疏向量非零元素集合
            val vector = new SparseVector(dict.size,index,Array.fill(index.length)(1.0))
            //构建LabeledPoint
            new LabeledPoint(label,vector)
        })


        //模型训练，两个参数分别是迭代次数和步长
        //训练集的格式：  LabeledPoint(label,vector)
        //(1.0,(176217,[75865,80604,162029,101398,62477,150452,152208,45922,73135,3245,23069,94060,87963,25485,72752,50704,158679,133224,85569,161080,8740],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
        val model: LogisticRegressionModel = LogisticRegressionWithSGD.train(traindata,10,0.1)
        //得到权重
        val weights = model.weights.toArray
        //将原来的字典表反转，根据下标找对应的特征字符串
        val map: Map[Long, String] = dict.map(x=>{(x._2,x._1)})

        val pw = new PrintWriter("data_ctr_model/output")//输出为模型文件  "特征名称 \t  权重值"

        for(i <- 0 until weights.length){
            val feartureName = map.get(i) match {
                case Some(x) => x
                case None =>""
            }
            val result = feartureName+"\t"+weights(i)
            pw.write(result)
            pw.println()//换行
        }
        pw.flush()
        pw.close()
    }
}
