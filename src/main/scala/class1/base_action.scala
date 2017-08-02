package main.scala.class1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
  * Created by lsc on 2017/7/21.
  */
object base_action {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("base_make").setMaster("local")
    val sc = new SparkContext(conf)

    val rddInt:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,2,5,1))
    val rddStr:RDD[String] = sc.parallelize(Array("a","b","c","d","b","a"), 1)

    /* count操作 */
    println("======count操作======")
    println(rddInt.count())
    println("======count操作======")
    /* countByValue操作 */
    println("======countByValue操作======")
    println(rddInt.countByValue())
    println("======countByValue操作======")
    /* reduce操作 */
    println("======reduce操作======")
    println(rddInt.reduce((x ,y) => x + y))
    println("======reduce操作======")
    /* fold操作 */
    println("======fold操作======")
    println(rddInt.fold(0)((x ,y) => x + y))
    println(rddInt.fold(1)((x ,y) => x + y))
    println("======fold操作======")
    /* aggregate操作 */
    println("======aggregate操作======")
    val res:(Int,Int) = rddInt.aggregate((0,0))((x,y) => (x._1 + x._2,y),(x,y) => (x._1 + x._2,y._1 + y._2))
    println(res._1 + "," + res._2)
    val res1:(Int,Int) = rddInt.aggregate((1,1))((x,y) => (x._1 + x._2,y),(x,y) => (x._1 + x._2,y._1 + y._2))
    println(res1._1 + "," + res1._2)
    println("======aggregate操作======")
    /* foeach操作 */
    println("======foeach操作======")
    println(rddStr.foreach { x => println(x) })
    println("======foeach操作======")

    sc.stop()
  }
}
