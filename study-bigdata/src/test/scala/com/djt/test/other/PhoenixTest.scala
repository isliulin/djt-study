package com.djt.test.other

import com.djt.test.dto.CaseClass.TestCase
import com.djt.test.spark.action.AbsActionTest
import com.djt.utils.ConfigConstant
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.junit.{Before, Test}

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-16
 */
class PhoenixTest extends AbsActionTest {

    private var zkUrl: String = _

    @Before
    override def before(): Unit = {
        super.before()
        zkUrl = config.getProperty(ConfigConstant.Phoenix.PHOENIX_ZK_URL)
    }

    override def setSparkConf(sparkConf: SparkConf): Unit = {
        sparkConf.set("phoenix.schema.mapSystemTablesToNamespace", "true")
        sparkConf.set("phoenix.schema.isNamespaceMappingEnabled", "true")
    }

    @Test
    def testSparkRead(): Unit = {
        val sparkSession = getSparkSession
        sparkSession.read
                .format("org.apache.phoenix.spark")
                .option("table", "TEST.T_TEST_DJT")
                .option("zkUrl", "xdata-uat03.jlpay.io,cdh-dev01.jlpay.io,cdh-dev02.jlpay.io:2181")
                .load()
                .show(20, truncate = false)
    }

    @Test
    def testSparkWrite(): Unit = {
        val table = "TEST.T_TEST_DJT"
        val sparkSession = getSparkSession
        import sparkSession.implicits._
        val dataArr = Array(TestCase(f1 = "111", f2 = "111", f3 = "111"),
            TestCase(f1 = "222", f2 = "222", f3 = "222"),
            TestCase(f1 = "333", f2 = "333", f3 = "333"))

        val df = sparkSession.sparkContext.parallelize(dataArr).toDF()
        df.saveToPhoenix(Map("table" -> table, "zkUrl" -> "172.20.7.34:2181"))
    }

}
