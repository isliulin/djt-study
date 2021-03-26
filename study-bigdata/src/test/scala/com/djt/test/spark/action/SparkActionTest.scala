package com.djt.test.spark.action

import com.djt.spark.action.impl.FirstSparkAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.junit.Test

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

    case class Person(id: Int, name: String, age: Int)

}
