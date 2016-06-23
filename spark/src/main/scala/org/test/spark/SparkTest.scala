

package org.test.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkTest {
      def main(args: Array[String]) {
      val conf=new SparkConf()
      .setAppName("SparkTest")
      .setMaster("local")
      val sc=new SparkContext(conf)
      val tf=sc.textFile("file:///spark-1.6.1/README.md")
      val tokens=tf.flatMap(l=>l.split(" "))
      val c_pair=tokens.map(w=>(w,1))
      val counts=c_pair.reduceByKey((c,k)=>c+k)
      val s_counts=counts.sortBy(p=>p._2,false).saveAsTextFile("count_20160620_2")
    }
}