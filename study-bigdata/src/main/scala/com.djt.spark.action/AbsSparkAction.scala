package com.djt.spark.action

import com.djt.utils.ParamConstant
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

/**
 * spark任务基础类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-02 15:23
 */
abstract class AbsSparkAction(config: Properties) extends Serializable {

    protected val LOG: Logger = LoggerFactory.getLogger(this.getClass)

    private var sparkSession: SparkSession = _

    /**
     * 任务入口
     */
    def action(): Unit = {
        LOG.info("任务开始...")
        val start = System.currentTimeMillis()
        val ss = getSparkSession
        beforeAction(ss)
        executeAction(ss)
        ss.close()
        LOG.info("任务结束...共耗时：{} 秒", (System.currentTimeMillis() - start) / 1000)
    }

    /**
     * 任务执行前的工作 子类可重写
     *
     * @param sparkSession ss
     */
    def beforeAction(sparkSession: SparkSession): Unit = {}

    /**
     * 任务执行实体 由子类实现
     *
     * @param sparkSession ss
     */
    def executeAction(sparkSession: SparkSession): Unit

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
        if (StringUtils.isBlank(sparkMaster)) {
            throw new IllegalArgumentException(ParamConstant.SPARK_MASTER + " can not be null!")
        }
        val sparkAppName = config.getProperty(ParamConstant.SPARK_APP_NAME, this.getClass.getSimpleName)
        val sparkConf = new SparkConf()
        sparkConf.setMaster(sparkMaster)
        sparkConf.setAppName(sparkAppName)
        setSparkConf(sparkConf)
        sparkConf
    }

    /**
     * 设置SparkConf 主要用于子类重写 添加相关配置
     *
     * @param sparkConf sc
     */
    protected def setSparkConf(sparkConf: SparkConf): Unit = {}

}
