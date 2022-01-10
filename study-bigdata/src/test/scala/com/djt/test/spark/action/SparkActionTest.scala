package com.djt.test.spark.action

import com.alibaba.fastjson.JSONObject
import com.djt.spark.action.impl.FirstSparkAction
import com.djt.test.dto.CaseClass.Person
import com.djt.utils.RandomUtils
import org.apache.commons.lang3.StringUtils
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
    def mkateDate(): Unit = {
        val sparkSession = getSparkSession
        val hiveTable = "base_info.t_l_terminal"
        val selectFields = sparkSession.table(hiveTable).schema.fieldNames.filter(!_.equalsIgnoreCase("data_from"))
        val dataMap = List[Map[String, String]](
            Map("lterm_no" -> "A5289286", "lmer_no" -> "K20070405441076", "lmer_name" -> "张三", "agt_id" -> "50593337", "r_agt_id" -> "50591978", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "A1997395", "lmer_no" -> "K20051902073715", "lmer_name" -> "张三", "agt_id" -> "50593337", "r_agt_id" -> "50591978", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "M5836049", "lmer_no" -> "M50000004621192", "lmer_name" -> "张三", "agt_id" -> "50593337", "r_agt_id" -> "50591978", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "M7069911", "lmer_no" -> "M50000006018731", "lmer_name" -> "张三", "agt_id" -> "50593337", "r_agt_id" -> "50591978", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "A5667636", "lmer_no" -> "K20072005875636", "lmer_name" -> "李四", "agt_id" -> "50627050", "r_agt_id" -> "50553928", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "M6948725", "lmer_no" -> "M50000005889114", "lmer_name" -> "李四", "agt_id" -> "50627050", "r_agt_id" -> "50553928", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "A7307818", "lmer_no" -> "K21012207763638", "lmer_name" -> "李四", "agt_id" -> "50627050", "r_agt_id" -> "50553928", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "A5194595", "lmer_no" -> "K20120906214175", "lmer_name" -> "李四", "agt_id" -> "50627050", "r_agt_id" -> "50553928", "belong_branch" -> "201810111071"),
            Map("lterm_no" -> "A5249996", "lmer_no" -> "K20070305397156", "lmer_name" -> "王五", "agt_id" -> "50588769", "r_agt_id" -> "50587040", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "M6159400", "lmer_no" -> "M50000004995339", "lmer_name" -> "王五", "agt_id" -> "50588769", "r_agt_id" -> "50587040", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "A5523155", "lmer_no" -> "K20122506622525", "lmer_name" -> "王五", "agt_id" -> "50588769", "r_agt_id" -> "50587040", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "M7180336", "lmer_no" -> "M50000006133623", "lmer_name" -> "王五", "agt_id" -> "50588769", "r_agt_id" -> "50587040", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "A6487408", "lmer_no" -> "K20060202372195", "lmer_name" -> "赵六", "agt_id" -> "50646066", "r_agt_id" -> "50634434", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "M6788438", "lmer_no" -> "M50000005710652", "lmer_name" -> "赵六", "agt_id" -> "50646066", "r_agt_id" -> "50634434", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "A7523528", "lmer_no" -> "K21020508048238", "lmer_name" -> "赵六", "agt_id" -> "50646066", "r_agt_id" -> "50634434", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "M7141612", "lmer_no" -> "M50000006093658", "lmer_name" -> "赵六", "agt_id" -> "50646066", "r_agt_id" -> "50634434", "belong_branch" -> "201810121091"),
            Map("lterm_no" -> "A1979885", "lmer_no" -> "K20051401951335", "lmer_name" -> "马七", "agt_id" -> "50648999", "r_agt_id" -> "50596415", "belong_branch" -> "201810121073"),
            Map("lterm_no" -> "A6556248", "lmer_no" -> "K20101806898668", "lmer_name" -> "马七", "agt_id" -> "50648999", "r_agt_id" -> "50596415", "belong_branch" -> "201810121073"),
            Map("lterm_no" -> "M6791375", "lmer_no" -> "M50000005714067", "lmer_name" -> "马七", "agt_id" -> "50648999", "r_agt_id" -> "50596415", "belong_branch" -> "201810121073"),
            Map("lterm_no" -> "A6330076", "lmer_no" -> "K20091406635916", "lmer_name" -> "马七", "agt_id" -> "50648999", "r_agt_id" -> "50596415", "belong_branch" -> "201810121073"),
            Map("lterm_no" -> "A4579455", "lmer_no" -> "K20103105233255", "lmer_name" -> "候八", "agt_id" -> "50641002", "r_agt_id" -> "50634073", "belong_branch" -> "201810121073"),
            Map("lterm_no" -> "A4467806", "lmer_no" -> "K20060504577426", "lmer_name" -> "候八", "agt_id" -> "50641002", "r_agt_id" -> "50634073", "belong_branch" -> "201810121073"),
            Map("lterm_no" -> "A7045825", "lmer_no" -> "K21062309329245", "lmer_name" -> "候八", "agt_id" -> "50641002", "r_agt_id" -> "50634073", "belong_branch" -> "201810121073")
        )

        val selectSqlList = ListBuffer[String]()
        dataMap.foreach(map => {
            val fieldList = ListBuffer[String]()
            selectFields.foreach(field => {
                var value = map.getOrElse(field, "null")
                if (!"null".equalsIgnoreCase(value)) {
                    value = StringUtils.wrap(value, "\"")
                }
                fieldList.append(s"$value as $field")
            })
            selectSqlList.append("select " + fieldList.mkString(","))
        })
        val tmpView = selectSqlList.mkString("\nunion all\n")
        val insertSQL = s"INSERT INTO TABLE $hiveTable PARTITION(data_from=5) SELECT ${selectFields.mkString(",")} FROM (\n$tmpView\n)"
        sparkSession.sql(insertSQL)
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


    @Test
    def testSparkSqlKafka(): Unit = {
        val sql = "select 'a' as key, '888' as value limit 1"
        val df = getSparkSession.sql(sql)
        df.write.format("kafka")
                .option("kafka.bootstrap.servers", "172.20.7.33:9092,172.20.7.34:9092,172.20.7.35:9092")
                .option("topic", "TEST_topic")
                .save()

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
