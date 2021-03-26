package com.djt.test.spark.action

import com.djt.utils.ParamConstant
import org.apache.commons.lang3.Validate
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Before
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.collection.JavaConversions._


/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
 */
abstract class AbsActionTest extends Serializable {

    private val LOG = LoggerFactory.getLogger(this.getClass)

    private var sparkSession: SparkSession = _

    protected val config = new Properties

    @Before
    def before(): Unit = {
        LOG.info("=======初始化开始...=======")
        config.setProperty(ParamConstant.SPARK_MASTER, "local[*]")
        config.setProperty(ParamConstant.SPARK_APP_NAME, "SparkTest")
        config.setProperty(ParamConstant.SPARK_LOG_LEVEL, "WARN")
        config.setProperty(ParamConstant.ES_NODES, "172.20.7.33")
        config.setProperty(ParamConstant.ES_PORT, "9200")
        config.setProperty(ParamConstant.ES_HOST, "172.20.7.33:9200,172.20.7.34:9200,172.20.7.35:9200")
        config.setProperty(ParamConstant.ES_INDEX_AUTO_CREATE, "false")
        config.setProperty(ParamConstant.HBASE_ZK_QUORUM, "172.20.4.91,172.20.4.92,172.20.4.93")
        config.setProperty(ParamConstant.HBASE_ZK_PORT, "2181")
        config.setProperty("中文", "中文测试")
        setConfig(config)
        config.keySet().foreach(key => {
            println(s"$key : ${config.getProperty(key.toString)}")
        })
        LOG.info("=======初始化完成...=======")
    }

    protected def setConfig(config: Properties): Unit = {}

    /**
     * 获取 SparkSession
     *
     * @return ss
     */
    protected def getSparkSession: SparkSession = {
        if (null == sparkSession) {
            this.synchronized {
                if (null == sparkSession) {
                    sparkSession = SparkSession.builder.config(getSparkConf).enableHiveSupport.getOrCreate
                }
            }
        }
        sparkSession
    }

    /**
     * 获取SparkConf
     *
     * @return sc
     */
    private def getSparkConf: SparkConf = {
        val sparkMaster = config.getProperty(ParamConstant.SPARK_MASTER)
        Validate.notNull(sparkMaster, ParamConstant.SPARK_MASTER + " can not be null!")
        val sparkAppName = config.getProperty(ParamConstant.SPARK_APP_NAME, this.getClass.getSimpleName)
        val sparkConf = new SparkConf()
        sparkConf.setMaster(sparkMaster)
        sparkConf.setAppName(sparkAppName)
        sparkConf
    }

}
