package org.training.spark.recommondByCtrModel

import org.apache.spark.mllib.linalg.Vectors
/*
稠密向量和稀疏向量
一个向量(1.0,0.0,3.0)它有2种表示的方法
密集：[1.0,0.0,3.0]    其和一般的数组无异
稀疏：(3,[0,2],[1.0,3.0])     其表示的含义(向量大小，序号，值)   序号从0开始
下面是一个简单的例子
 */
object SparseVectorTest {
    def main(args: Array[String]) {
        val vd = Vectors.dense(2, 5, 8)
        println(vd(1))
        println(vd)

        //向量个数，序号，value
        val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 3, 5, 7))
        println(vs(2)) //序号访问
        println(vs)

        val vs2 = Vectors.sparse(4, Array(0, 2, 1, 3), Array(9, 3, 5, 7))
        println(vs2(2))
        println(vs2)
    }


}
