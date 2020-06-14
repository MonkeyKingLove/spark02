package com.atguigu.spark

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @BelongsProject: spark02
  * @BelongsPackage: com.atguigu.spark
  * @Author: liangyu
  * @CreateTime: 2019-12-06 17:03
  * @Description: ${Description}
  */
object Spark54_Req_3_PageFlowAverage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark49_Req_1_Top10Category")
    val sc = new SparkContext(conf)
    //TODO:1.获取原始数据
    val datas: RDD[String] = sc.textFile("E:\\demo\\demo\\user_visit_action.txt")
    val userVisitActionRDD: RDD[UserVisitAction] = datas.map(
      line => {
        val ids = line.split("_")
        UserVisitAction(
          ids(0),
          ids(1).toLong,
          ids(2),
          ids(3).toLong,
          ids(4),
          ids(5),
          ids(6).toLong,
          ids(7).toLong,
          ids(8),
          ids(9),
          ids(10),
          ids(11),
          ids(12).toLong
        )
      }
    )
    //1.TODO:首先对源数据以sessionId分组，排序
    val rdd1: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(
      action0 => action0.session_id
    )
    //TODO:创建一个时间对象
    val date_TIme: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val rdd2: RDD[(String, List[((Long, Long), Int)])] = rdd1.mapValues(
      action1 => {
        val list1: List[UserVisitAction] = action1.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        val zip1: List[(UserVisitAction, UserVisitAction)] = list1.zip(list1.tail)
        val map1: List[((Long, String), (Long, String))] = zip1.map {
          case (obj1, obj2) => {

            ((obj1.page_id, obj1.action_time), (obj2.page_id, obj2.action_time))
          }
        }
        val map2: List[((Long, Long), Int)] = map1.map {
          case (l1, l2) => {
            ((l1._1, (date_TIme.parse(l2._2).getTime - date_TIme.parse(l1._2).getTime)), 1)
          }
        }
        map2
      }
    )
    val rdd3: RDD[(String, ((Long, Long), Int))] = rdd2.flatMapValues(
      action2 => action2
    )
    val rdd4: RDD[(String, (Long, Int))] = rdd3.map(
      action3 => {
        (action3._1 + "_" + action3._2._1._1, (action3._2._1._2, action3._2._2))
      }
    )
    val rdd5: RDD[(String, (Long, Int))] = rdd4.reduceByKey(
      (action4, action5) => {
        (action4._1 + action5._1, action4._2 + action5._2)
      }
    )
    val rdd6: RDD[(String, Iterable[(Long, Int)])] = rdd5.groupByKey()
    val rdd7: RDD[(String, Long)] = rdd6.flatMapValues(
      action6 => {
        action6.map(
          action7 => {
            action7._1/ action7._2
          }
        )
      }
    )
    //    rdd7.collect().foreach(println)
    val rdd8: RDD[(Long, (Long, Int))] = rdd3.map(
      action8 => {
        (action8._2._1._1, (action8._2._1._2, action8._2._2))
      }
    ).reduceByKey(
      (action9, action10) => {
        (action9._1 + action10._1, action9._2 + action10._2)
      }
    )
    val rdd9: RDD[(Long, Long)] = rdd8.map(
      action11 => {
        (action11._1, action11._2._1 / action11._2._2)
      }
    )
    rdd9.collect().foreach(println)



    //2.TODO：格式转换：session,actionTime

  }



}
