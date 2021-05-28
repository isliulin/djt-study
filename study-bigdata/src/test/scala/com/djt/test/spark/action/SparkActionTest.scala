package com.djt.test.spark.action

import com.alibaba.fastjson.JSONObject
import com.djt.spark.action.impl.FirstSparkAction
import com.djt.test.dto.CaseClass.Person
import com.djt.utils.RandomUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Partitioner, TaskContext}
import org.junit.Test

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
 */
class SparkActionTest extends AbsActionTest {

    @Test
    def testConfig(): Unit = {

    }

    @Test
    def testFirstAction(): Unit = {
        new FirstSparkAction(config).action()
    }

    @Test
    def testRDD(): Unit = {
        val sparkSession = getSparkSession
        val fileRDD: RDD[String] = sparkSession.sparkContext.textFile("D:\\data\\person.txt")
        val linesRDD: RDD[Array[String]] = fileRDD.map(_.split(" "))
        val rowRDD: RDD[Person] = linesRDD.map(line => Person(line(0).toInt, line(1), line(2).toInt))
        import sparkSession.implicits._
        val personDF: DataFrame = rowRDD.toDF
        personDF.show(20, truncate = false)
    }


    @Test
    def testSparkSql(): Unit = {
        val sql = "select creator from base_info.t_diction"
        val df = getSparkSession.sql(sql)

        df.foreachPartition(iter => {
            val partitionId = TaskContext.get.partitionId
            val arr = new ArrayBuffer[String]()
            while (iter.hasNext) {
                val row = iter.next()
                val creator = row.getAs[String]("creator")
                arr.append(creator)
            }
            println(partitionId + "======666======" + arr)
        })

        df.write.partitionBy("creator")
    }


    @Test
    def testRepartition(): Unit = {
        val sparkSession = getSparkSession
        val keyList = Array[String]("t_table_1", "t_table_2", "t_table_3", "t_table_4", "t_table_5")

        val dataList = new ListBuffer[(String, (Int, Int))]()
        for (_ <- 1 to 100) {
            val table = keyList(RandomUtils.getRandomNumber(0, keyList.length))
            val id = RandomUtils.getRandomNumber(0, 100)
            val amt = RandomUtils.getRandomNumber(0, 10000)
            val pos = RandomUtils.getRandomNumber(1, 5)
            dataList.append((s"$table#$id", (amt, pos)))
        }

        val rdd = sparkSession.sparkContext.parallelize(dataList, 10).cache()
        //打印原数据
        rdd.foreachPartition(iter => {
            println(s"分区号：${TaskContext.get.partitionId} 数据：${iter.toList}")
        })

        println("正经分割线=====================================================================")

        rdd.reduceByKey(new MyPartitioner(keyList), (data1, data2) => {
            if (data1._2 > data2._2) {
                data1
            } else {
                data2
            }
        }).foreachPartition(iter => {
            println(s"分区号：${TaskContext.get.partitionId} 数据：${iter.toList}")
        })
    }

    @Test
    def testTakeOrder(): Unit = {
        val sparkSession = getSparkSession
        val dataList = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9)
        val rdd = sparkSession.sparkContext.parallelize(dataList, 1).cache()
        val order = new Ordering[Int] {
            override def compare(x: Int, y: Int): Int = {
                x.compareTo(y)
            }
        }
        rdd.takeOrdered(5)(order).foreach(println(_))

        val dataList2 = ArrayBuffer[JSONObject]()
        for (i <- 1 to 10) {
            val json = new JSONObject()
            json.put("f1", i)
            dataList2.append(json)
        }
        val rdd2 = sparkSession.sparkContext.parallelize(dataList2, 1).cache()
        val order2 = new Ordering[JSONObject] {
            override def compare(x: JSONObject, y: JSONObject): Int = {
                x.getIntValue("f1").compareTo(y.getIntValue("f1"))
            }
        }
        rdd2.takeOrdered(5)(order2).foreach(println(_))
    }

    /**
     * 自定义分区器
     */
    class MyPartitioner(keyList: Array[String]) extends Partitioner {

        def numPartitions: Int = keyList.length

        def getPartition(key: Any): Int = {
            val table = key.toString.split("#")(0)
            keyList.indexOf(table)
        }
    }


}
