package org.training.spark.recommondByCtrModel

import java.io.{FileOutputStream, PrintWriter}
import java.lang.Math.random

class DataGenerator {

}

//比例确定如果有10000条记录，有一千个用户，100个app
//默认应该是十万条记录  一万个用户   一千条记录  相应的作者应该是两百个
object DataGenerator{
    def main(args: Array[String]) {
        //    for(i<- 1 to 100){
        //      var str = new StringBuilder()
        //      for(i<-1 to (10*random()).toInt+1){
        //        str.append("hitop_id").append((random()*100000/100).toInt).append(",")
        //      }
        //
        //      println(str)
        //    }
        makelist(args(0).toInt,args(1))
        makeappdown(args(0).toInt,args(1))
        makesample(args(0).toInt,args(1))
    }
    /**
      *
    hitop_id    STRING,
    name        STRING,
    author      STRING,
    sversion    STRING,
    ischarge    SMALLINT,
    designer    STRING,
    font        STRING,
    icon_count  INT,
    stars       DOUBLE,
    price       INT,
    file_size   INT,
    comment_num INT,
    screen      STRING,
    dlnum       INT
      */
    def makelist(num:Int,path:String): Unit ={
        val pw = new PrintWriter(new FileOutputStream(path+"/applist.txt",true))
        var str = new StringBuilder()
        for(i<-1 to num){
            str.clear()
            str.append("hitop_id").append((random()*num/100).toInt).append("\t")
            str.append("name").append((random()*num/100).toInt).append("\t")
            str.append("author").append((random()*num/500).toInt).append("\t")
            str.append("sversion").append((10*random()).toInt).append("\t")
            str.append((2*random()).toInt).append("\t")
            str.append("designer").append((random()*num/500).toInt).append("\t")
            str.append("font").append((20*random()).toInt).append("\t")
            str.append((10*random()).toInt).append("\t")
            str.append(f"${10*random()}%1.2f").append("\t")
            str.append((random()*num).toInt).append("\t")
            str.append((random()*num).toInt).append("\t")
            str.append((random()*num/50).toInt).append("\t")
            str.append("screen").append((random()*num/20).toInt).append("\t")
            str.append((random()*num/50).toInt)
            pw.write(str.toString())
            pw.println()
        }
        pw.flush()
        pw.close()
    }

    def makeapplist(num: Int) :String = {
        var str = new StringBuilder()
        for(i<-1 to (10*random()).toInt+1){
            str.append("hitop_id").append((random()*num/100).toInt).append(",")
        }
        str.deleteCharAt(str.length-1)
        return str.toString()
    }

    /**
      *
    device_id           STRING,
    devid_applist       STRING,
    device_name         STRING,
    pay_ability         STRING
      */
    def makeappdown(num:Int,path:String): Unit ={
        val pw = new PrintWriter(new FileOutputStream(path+"/userdownload.txt",true))
        var str = new StringBuilder()
        for(i<-1 to num){
            str.clear()
            str.append("device_id").append((random()*num/10).toInt).append("\t")
            str.append(makeapplist(num)).append("\t")
            str.append("device_name").append((random()*num/10).toInt).append("\t")
            str.append("pay_ability").append((4*random()).toInt)
            pw.write(str.toString());
            pw.println()
        }
        pw.flush()
        pw.close()
    }
    /**
      *
    label       STRING,
    device_id   STRING,
    hitop_id    STRING,
    screen      STRING,
    en_name     STRING,
    ch_name     STRING,
    author      STRING,
    sversion    STRING,
    mnc         STRING,
    event_local_time STRING,
    interface   STRING,
    designer    STRING,
    is_safe     INT,
    icon_count  INT,
    update_time STRING,
    stars       DOUBLE,
    comment_num INT,
    font        STRING,
    price       INT,
    file_size   INT,
    ischarge    SMALLINT,
    dlnum       INT
      */
    def makesample(num:Int,path:String): Unit ={
        val pw = new PrintWriter(new FileOutputStream(path+"/sample.txt",true))
        var str = new StringBuilder()
        for(i<-1 to num){
            str.clear()
            str.append(2*(2*random()).toInt-1).append("\t")
            str.append("device_id").append((random()*num/10).toInt).append("\t")
            str.append("hitop_id").append((random()*num/100).toInt).append("\t")
            str.append("screen").append((random()*20).toInt).append("\t")
            str.append("en_name").append((random()*num/100).toInt).append("\t")
            str.append("ch_name").append((random()*num/100).toInt).append("\t")
            str.append("author").append((random()*num/500).toInt).append("\t")
            str.append("sversion").append((10*random()).toInt).append("\t")
            str.append("mnc").append((random()*num/10).toInt).append("\t")
            str.append("event_local_time").append((random()*num).toInt).append("\t")
            str.append("interface").append((random()*num).toInt).append("\t")
            str.append("designer").append((random()*num/500).toInt).append("\t")
            str.append((2*random()).toInt).append("\t")
            str.append((10*random()).toInt).append("\t")
            str.append("update_date").append((random()*num).toInt).append("\t")
            str.append(f"${10*random()}%1.2f").append("\t")
            str.append((random()*num/50).toInt).append("\t")
            str.append("font").append((20*random()).toInt).append("\t")
            str.append((random()*num).toInt).append("\t")
            str.append((random()*num).toInt).append("\t")
            str.append((2*random()).toInt).append("\t")
            str.append((random()*num/50).toInt)
            pw.write(str.toString());
            pw.println()
        }
        pw.flush()
        pw.close()
    }

}