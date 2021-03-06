package main.scala.ssq.statistical_analysis

/**
  * Created by lsc on 2017/8/1.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.Calendar

import breeze.linalg.sum
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD

object ml_regression_test_h1 {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf=new SparkConf().setAppName("regression").setMaster("local[1]")
    val sc=new SparkContext(conf)

    val records=sc.textFile("file:///D:/soft-install/workspaces/ideaprojects/sparkprojects/data/spark_test_0.1/ssq/ml/regression/data2.csv").map(_.split(",")).cache()

    val mappings=for(i<-Range(1,24))yield get_mapping(records,i)
    val cat_len=sum(mappings.map(_.size))

    //linear regression data 此部分代码最重要，主要用于产生训练数据集，按照前文所述处理类别特征和实数特征。
    val data=records.map{record=>

      val cat_vec=Array.ofDim[Double](cat_len)
      var i=0
      var step=0
      for(filed<-record.slice(1,25))
      {

        val m=mappings(i)
        val idx=m(filed)
        cat_vec(idx.toInt+step)=1.0
        i=i+1
        step=step+m.size
      }

      val features=cat_vec
      val label=record(record.size-1).toInt


      LabeledPoint(label,Vectors.dense(features))
    }

    // val categoricalFeaturesInfo = Map[Int, Int]()
    //val linear_model=DecisionTree.trainRegressor(data,categoricalFeaturesInfo,"variance",5,32)
    val linear_model=LinearRegressionWithSGD.train(data,10,0.1)
    val true_vs_predicted=data.map(p=>(p.label,linear_model.predict(p.features)))
    //输出前10个真实值与预测值
    println( true_vs_predicted.take(5).toVector.toString())
    //Vector((3.0,3.219002121341281), (14.0,3.2573873232352053), (1.0,2.7040376299928734), (3.0,3.0739772364797595), (3.0,2.7280288939401416))
  }

  def get_mapping(rdd:RDD[Array[String]], idx:Int)=
  {
    rdd.map(filed=>filed(idx)).distinct().zipWithIndex().collectAsMap()
  }

}
