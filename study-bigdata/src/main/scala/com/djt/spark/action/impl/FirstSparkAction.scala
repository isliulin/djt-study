package com.djt.spark.action.impl

import com.djt.spark.action.AbsSparkAction
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * Spark Demo
 *
 * @author 　djt317@qq.com
 * @date 　  2021-02-03 10:20
 */
class FirstSparkAction(config: Properties) extends AbsSparkAction(config) {

    /**
     * 执行SQL 打印结果
     *
     * @param sparkSession ss
     */
    override def executeAction(sparkSession: SparkSession): Unit = {
        sparkSession.sql("show schemas").show(20, truncate = false)
    }

}
