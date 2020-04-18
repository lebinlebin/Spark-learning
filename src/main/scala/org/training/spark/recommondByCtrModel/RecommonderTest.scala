package org.training.spark.recommondByCtrModel

import java.io.PrintWriter

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

class RecommonderTest {

}

object RecommonderTest{

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        //按照\t做分割切分，得到的是label和features字符串
      /**
       * 1	Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id370*hitop_id166:1;User.Item*Item,hitop_id801*hitop_id166:1;User.Item*Item,hitop_id583*hitop_id166:1;User.phone*Item,device_name1422*hitop_id166:1;User.pay*Item.price,pay_ability1*9290:1
       * 1	Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id300*hitop_id166:1;User.Item*Item,hitop_id968*hitop_id166:1;User.Item*Item,hitop_id400*hitop_id166:1;User.phone*Item,device_name3083*hitop_id166:1;User.pay*Item.price,pay_ability3*9290:1
       * 1	Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id119*hitop_id166:1;User.Item*Item,hitop_id9*hitop_id166:1;User.phone*Item,device_name7097*hitop_id166:1;User.pay*Item.price,pay_ability0*9290:1
       * 1	Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id499*hitop_id166:1;User.Item*Item,hitop_id159*hitop_id166:1;User.Item*Item,hitop_id638*hitop_id166:1;User.phone*Item,device_name3679*hitop_id166:1;User.pay*Item.price,pay_ability2*9290:1
       * 1	Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;Item.author,author72:1;Item.sversion,sversion9:1;Item.network,x:1;Item.dgner,designer108:1;Item.icount,4:1;Item.stars,1.41:1;Item.comNum,11:1;Item.font,font4:1;Item.price,9290:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,1000:1;User.Item*Item,hitop_id213*hitop_id166:1;User.Item*Item,hitop_id82*hitop_id166:1;User.Item*Item,hitop_id849*hitop_id166:1;User.phone*Item,device_name7744*hitop_id166:1;User.pay*Item.price,pay_ability1*9290:1
       * -1	Item.id,hitop_id117:1;Item.screen,screen12:1;Item.name,ch_name183:1;All,0:1;Item.author,author15:1;Item.sversion,sversion7:1;Item.network,x:1;Item.dgner,designer136:1;Item.icount,6:1;Item.stars,5.63:1;Item.comNum,11:1;Item.font,font19:1;Item.price,43406:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,5000:1;User.Item*Item,hitop_id363*hitop_id117:1;User.Item*Item,hitop_id880*hitop_id117:1;User.Item*Item,hitop_id421*hitop_id117:1;User.phone*Item,device_name9023*hitop_id117:1;User.pay*Item.price,pay_ability1*43406:1
       * -1	Item.id,hitop_id117:1;Item.screen,screen12:1;Item.name,ch_name183:1;All,0:1;Item.author,author15:1;Item.sversion,sversion7:1;Item.network,x:1;Item.dgner,designer136:1;Item.icount,6:1;Item.stars,5.63:1;Item.comNum,11:1;Item.font,font19:1;Item.price,43406:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,5000:1;User.Item*Item,hitop_id558*hitop_id117:1;User.Item*Item,hitop_id471*hitop_id117:1;User.Item*Item,hitop_id887*hitop_id117:1;User.phone*Item,device_name5848*hitop_id117:1;User.pay*Item.price,pay_ability2*43406:1
       * -1	Item.id,hitop_id117:1;Item.screen,screen12:1;Item.name,ch_name183:1;All,0:1;Item.author,author15:1;Item.sversion,sversion7:1;Item.network,x:1;Item.dgner,designer136:1;Item.icount,6:1;Item.stars,5.63:1;Item.comNum,11:1;Item.font,font19:1;Item.price,43406:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,5000:1;User.Item*Item,hitop_id3*hitop_id117:1;User.phone*Item,device_name5903*hitop_id117:1;User.pay*Item.price,pay_ability1*43406:1
       * -1	Item.id,hitop_id117:1;Item.screen,screen12:1;Item.name,ch_name183:1;All,0:1;Item.author,author15:1;Item.sversion,sversion7:1;Item.network,x:1;Item.dgner,designer136:1;Item.icount,6:1;Item.stars,5.63:1;Item.comNum,11:1;Item.font,font19:1;Item.price,43406:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,5000:1;User.Item*Item,hitop_id150*hitop_id117:1;User.Item*Item,hitop_id762*hitop_id117:1;User.Item*Item,hitop_id1*hitop_id117:1;User.phone*Item,device_name8680*hitop_id117:1;User.pay*Item.price,pay_ability0*43406:1
       * -1	Item.id,hitop_id117:1;Item.screen,screen12:1;Item.name,ch_name183:1;All,0:1;Item.author,author15:1;Item.sversion,sversion7:1;Item.network,x:1;Item.dgner,designer136:1;Item.icount,6:1;Item.stars,5.63:1;Item.comNum,11:1;Item.font,font19:1;Item.price,43406:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,5000:1;User.Item*Item,hitop_id244*hitop_id117:1;User.Item*Item,hitop_id383*hitop_id117:1;User.Item*Item,hitop_id631*hitop_id117:1;User.phone*Item,device_name3051*hitop_id117:1;User.pay*Item.price,pay_ability0*43406:1
       * -1	Item.id,hitop_id117:1;Item.screen,screen12:1;Item.name,ch_name183:1;All,0:1;Item.author,author15:1;Item.sversion,sversion7:1;Item.network,x:1;Item.dgner,designer136:1;Item.icount,6:1;Item.stars,5.63:1;Item.comNum,11:1;Item.font,font19:1;Item.price,43406:1;Item.fsize,2:1;Item.ischarge,1:1;Item.downNum,5000:1;User.Item*Item,hitop_id617*hitop_id117:1;User.Item*Item,hitop_id846*hitop_id117:1;User.Item*Item,hitop_id833*hitop_id117:1;User.phone*Item,device_name1312*hitop_id117:1;User.pay*Item.price,pay_ability1*43406:1
       */

        val data = sc.textFile("data_ctr_model/000001_0(训练数据格式_输入到Recommonder的文件)")
            .map(_.split("\t"))//是一个数组


        //统计所有的特征  先把label切掉
        val test = data.map(_.drop(1))//drop(1) 之后还是一个数组格式，因为data本来就是一个数组格式，数组格式不能直接打印，需要指定下标才能打印。数组这里其实就只有一个元素
//        test.take(1).foreach(println(_))//数组  一条数据就一个样本=>其实就是一条样本  [Ljava.lang.String;@5a00eb1e
        val test2 = data.map(_.drop(1)(0))  //取到数组的第一个,实际的数据字符串
        println("###########################################################")
        //最终代码
        data.map{x =>
            var features:Array[String] = x.drop(1)(0).split(";")
            features.map(_.split(":")(0))//取前边的特征名，抛掉最后的":1"
        }

        val featurestest: RDD[String] =  data.flatMap(_.drop(1)(0).split(";"))
        featurestest.take(5).foreach(println(_))
      /**
       * Item.id,hitop_id166:1
       * Item.screen,screen18:1
       * Item.name,ch_name220:1
       * All,0:1
       * Item.author,author72:1
       */


        //如果用map得到的是RDD[Array[String]],flatmap可以压平，将Array里面String释放出来，这里的map实际把:1去掉
        //去重得到特征的字典映射
             val features: RDD[String] =  data.flatMap(_.drop(1)(0).split(";")).map(_.split(":")(0)).distinct()
        features.take(50).foreach(println(_))
      /**
       * Item.stars,1.39
       * User.Item*Item,hitop_id93*hitop_id973
       * User.phone*Item,device_name2860*hitop_id824
       * User.Item*Item,hitop_id643*hitop_id899
       * User.phone*Item,device_name3001*hitop_id157
       * User.phone*Item,device_name4940*hitop_id204
       */

        val dicttest =  features.zipWithIndex()//给每个特征编一个号
        println("###########################################################")
        dicttest.take(10).foreach(println(_))
      /**
       * (User.Item*Item,hitop_id430*hitop_id411,0)
       * (User.Item*Item,hitop_id3*hitop_id239,1)
       * (User.Item*Item,hitop_id394*hitop_id219,2)
       * (User.Item*Item,hitop_id669*hitop_id247,3)
       * (User.phone*Item,device_name618*hitop_id88,4)
       * (User.phone*Item,device_name2349*hitop_id290,5)
       * (User.pay*Item.price,pay_ability0*7691,6)
       * (User.Item*Item,hitop_id676*hitop_id197,7)
       * (User.phone*Item,device_name2873*hitop_id958,8)
       * (User.Item*Item,hitop_id961*hitop_id303,9)
       */

        val featureIDMap: Map[String, Long] =  features.zipWithIndex().collectAsMap()//转换为map是为以后用方便
        println("转换为map")
        featureIDMap.take(10).foreach(println(_))
      /**
       * 转换为map  特征名称，特征编号
       * (User.Item*Item,hitop_id*hitop_id53,93022)
       * (User.Item*Item,hitop_id781*hitop_id388,65630)
       * (User.Item*Item,hitop_id503*hitop_id190,124611)
       * (User.Item*Item,hitop_id842*hitop_id938,168801)
       * (User.phone*Item,device_name5172*hitop_id928,8918)
       * (User.phone*Item,device_name4769*hitop_id501,29343)
       * (User.Item*Item,hitop_id978*hitop_id948,48078)
       * (User.phone*Item,device_name3483*hitop_id224,101125)
       * (User.Item*Item,hitop_id205*hitop_id276,23545)
       * (User.Item*Item,hitop_id13*hitop_id214,164655)
       */

        //转成map为了后面得到稀疏向量非零下标用
         val dict: Map[String, Long] =  features.zipWithIndex().collectAsMap()//所有有取值的特征全集


      //先对label进行处理, 得到label，逻辑回归只支持0.0和1.0这里需要转换一下
      //构建labelpoint,分label和vector两部分  mllib要求的格式
      val traindata: RDD[LabeledPoint] = data.map(  x => {
        val label = x.take(1)(0) match {//data是一个数组
                        case "-1" => 0.0
                        case "1" => 1.0
         }
        // 获得当前样本的每个特征在map中的下标，这些下标的位置是非零的，值统一是1.0
        // Item.id,hitop_id166:1;Item.screen,screen18:1;Item.name,ch_name220:1;All,0:1;
        /*val test = x.drop(1)(0).split(";").map(_.split(":")(0))
        test.take(10).foreach(println(_))*/

          val index: Array[Int] = x.drop(1)(0).split(";").map(_.split(":")(0)).map(
              fe => {  //构建稠密向量需要获取非0下标的特征
                  /*val tstt: Option[Long] =  dict.get(fe)  有可能找不到这个key*/
                  val index: Long = dict.get(fe) match {
                     case Some(n) => n  //取到后就为原来的Long型数据
                     case None => 0
                     }
                   index.toInt//这个特征在map中的位置
               }
          )
         //创建一个所有元素是1.0的数组，作为稀疏向量非零元素集合
                                                         //index要求是int类型的
          val vector = new SparseVector(dict.size,index,Array.fill(index.length)(1.0))
                                                                        //Array.fill需要两个值  一个是长度，一个是要赋予的值
                                                                         //创建一个数组指定数组长度和要赋予的值  见Test.scala
           //构建LabeledPoint
           new LabeledPoint(label,vector)
       })


        traindata.take(10).foreach(println(_))
      /**
       * label,(特征全集数量,[取值为1.0的index])
       * (1.0,(176217,[75865,80604,162029,101398,62477,150452,152208,45922,73135,3245,23069,94060,87963,25485,72752,50704,158679,133224,85569,161080,8740],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (1.0,(176217,[75865,80604,162029,101398,62477,150452,152208,45922,73135,3245,23069,94060,87963,25485,72752,50704,124050,102297,175197,169485,125674],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (1.0,(176217,[75865,80604,162029,101398,62477,150452,152208,45922,73135,3245,23069,94060,87963,25485,72752,50704,70029,169900,29915,89873,66826],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (1.0,(176217,[75865,80604,162029,101398,62477,150452,152208,45922,73135,3245,23069,94060,87963,25485,72752,50704,132186,31374,109507,8740],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (1.0,(176217,[75865,80604,162029,101398,62477,150452,152208,45922,73135,3245,23069,94060,87963,25485,72752,50704,5664,6929,168696,61919,3457],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (1.0,(176217,[75865,80604,162029,101398,62477,150452,152208,45922,73135,3245,23069,94060,87963,25485,72752,50704,108829,104331,144588,67789,125674],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (0.0,(176217,[80507,33696,30117,101398,134647,165282,152208,3382,35973,165194,23069,81247,138418,25485,72752,42609,44923,85693,83948,135693,32705],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (0.0,(176217,[80507,33696,30117,101398,134647,165282,152208,3382,35973,165194,23069,81247,138418,25485,72752,42609,103216,96549,69906,55249,50256],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (0.0,(176217,[80507,33696,30117,101398,134647,165282,152208,3382,35973,165194,23069,81247,138418,25485,72752,42609,3119,71902,32705],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       * (0.0,(176217,[80507,33696,30117,101398,134647,165282,152208,3382,35973,165194,23069,81247,138418,25485,72752,42609,108203,130700,19739,98374,137350],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
       *
       */
      //模型训练，两个参数分别是迭代次数和步长
      //Symbol LogisticRegressionWithSGD is deprecated. Use ml.classification.LogisticRegression or LogisticRegressionWithLBFGS more... (Ctrl+F1)
        val model: LogisticRegressionModel
        = LogisticRegressionWithSGD.train(traindata,10,0.1)//迭代次数   步长

        //获得我们想要的
        // "特征名称 \t 权重"  的格式
        //得到权重
        val weightstest: linalg.Vector = model.weights
        val weights = model.weights.toArray  //Vector转为Array
        //将原来的字典表反转，根据下标找对应的特征字符串
        val map: Map[Long, String] = dict.map(x=>{(x._2,x._1)})
        //根据下标到特征词典(dict)中找到特征的名称
        val pw = new PrintWriter("data_ctr_model/output")//输出为模型文件  特征及权重值

       for(i <- 0 until weights.length) {
            val feartureName = map.get(i) match {
             case Some(x) => x
             case None =>""
            }
            val result = feartureName+"\t"+weights(i)//得到  特征名称 \t  权重
            pw.write(result)
          pw.println()
       }

       pw.flush()
       pw.close()
    }
}

