package main.scala.ssq.statistical_analysis

/**
  * Created by lsc on 2017/8/1.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.util.Calendar

object base_test_h1 {
  /**
    * 判断字符串是否是纯数字组成的串，如果是，就返回对应的数值，否则返回0
    * @param str
    * @return
    */
  def strToInt(str: String): Int = {
    val regex = """([0-9]+)""".r
    val res = str match{
      case regex(num) => num
      case _ => "0"
    }
    val resInt = Integer.parseInt(res)
    resInt
  }

  /**
    * 根据日期获取星期几
    * @param dateStr
    * @return
    */
  def dayOfWeek(dateStr: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(dateStr)

    val cal = Calendar.getInstance();
    cal.setTime(date);
    var w = cal.get(Calendar.DAY_OF_WEEK) - 1;

    //星期天 默认为0
    if (w <= 0)
      w = 7
    w
  }

  def main(args: Array[String]) {
    val inputFile =  "file:///D:/Workspaces/IdeaProjects/SCALA_PROJECTS/data/spark_test_0.1/ssq/statistical_analysis/in/2016_2017_data.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
//    val wordCount1 = textFile.flatMap(line => line.split(" ")).filter(x => strToInt(x) < 100 && strToInt(x) > 0)
//    val wordCount2 = wordCount1.map(word => (word, 1)).reduceByKey((a, b) => a + b).map(pair => (pair._2, pair._1))
//    val wordCount3 = wordCount2.sortByKey(false).map(pair => (pair._2, pair._1))

//    val wordCount1 = textFile.map(line => line.split(" ").zipWithIndex.map(pair => (pair._2, pair._1)))
//    val wordCount1 = textFile.map(line => line.split(" ").zipWithIndex)
    val wordCount1 = textFile.map(line => line.split(" "))
    wordCount1.foreach(println)

  }
}
