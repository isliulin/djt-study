package com.djt.spark.action.impl

import com.djt.spark.action.AbsSparkAction
import org.apache.commons.lang3.StringUtils
import org.apache.spark.TaskContext
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
     * Spark 常用操作
     *
     * @param sparkSession ss
     */
    override def executeAction(sparkSession: SparkSession): Unit = {
        //FirstSparkAction.executeSql(sparkSession)
        //FirstSparkAction.readFile(sparkSession)
    }

}

/**
 * Companion object
 */
object FirstSparkAction {

    /**
     * 执行SQL并打印结果
     *
     * @param sparkSession ss
     */
    def executeSql(sparkSession: SparkSession): Unit = {
        sparkSession.sql("show schemas").show(20, truncate = false)
    }

    /**
     * 读取文件(本地/HDFS)
     *
     * @param sparkSession ss
     */
    def readFile(sparkSession: SparkSession): Unit = {
        val filePath = "file:\\C:\\Windows\\System32\\drivers\\etc\\hosts.txt"
        val lineRdd = sparkSession.sparkContext.textFile(filePath, 3)
        lineRdd.filter(line => {
            StringUtils.isNotBlank(line) && !StringUtils.startsWith(line, "#")
        }).mapPartitions(iter => {
            iter.map(ipHost => {
                val ipHostArr = StringUtils.split(ipHost, " ")
                val ip = ipHostArr(0)
                val host = if (ipHostArr.length >= 2) ipHostArr(1) else null
                (ip, host)
            })
        }).foreachPartition(iter => {
            val ptId = TaskContext.getPartitionId()
            println(s"当前分区ID：$ptId")
            iter.foreach(tup2 => {
                println(tup2)
            })
        })


    }
}
