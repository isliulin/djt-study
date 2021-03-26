package com.djt.spark.action.impl

import com.djt.spark.action.AbsSparkAction
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties
import java.util.regex.Pattern
import scala.collection.mutable

/**
 * Spark Demo
 *
 * @author 　djt317@qq.com
 * @since 　 2021-02-03
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

    /**
     * 多线程读 数据库
     *
     * @param sparkSession ss
     * @return
     */
    def oracleDataTmpView(sparkSession: SparkSession): Unit = {
        val select = "select * from xxx where 1=1"
        val fetchSize = "100000"
        val partitions = 10
        val options = mutable.Map[String, String]()
        options.put("driver", "xxx")
        options.put("url", "xxx")
        options.put("user", "xxx")
        options.put("password", "xxx")
        options.put("dbtable", s"(SELECT count(*) AS CNT FROM ($select) t) tt")
        val cnt = sparkSession.read.format("jdbc").options(options).load.first().
                getAs[java.math.BigDecimal]("CNT").longValue() + 1000
        options.put("lowerBound", "0")
        options.put("upperBound", cnt.toString)
        options.put("fetchsize", fetchSize)
        options.put("numPartitions", partitions.toString)
        options.put("partitionColumn", "ROWNUM__RN")
        options.put("dbtable", s"(SELECT t.*,ROWNUM AS ROWNUM__RN FROM ($select) t) tt")
        sparkSession.read.format("jdbc").options(options).load.drop("ROWNUM__RN").show(20, truncate = false)
    }

    /**
     * 数据写 Hive ORC 文件
     * 先写临时目录 再替换原文件
     *
     * @param sparkSession ss
     * @param df           数据
     */
    def writeOrcFile(sparkSession: SparkSession, df: DataFrame): Unit = {
        var fs: FileSystem = null
        val basePath = "/user/hive/warehouse/test.db/t_test/"
        try {
            //创建HDFS会话
            fs = FileSystem.newInstance(sparkSession.sparkContext.hadoopConfiguration)
            //生成文件前缀
            val prefix = getFilePrefix("ORC-001")
            //生成临时目录 并在退出时自动删除
            val tmpDir = basePath + ".tmp_" + prefix
            createTmpDir(fs, tmpDir)
            //写数据到临时目录
            df.write.mode(SaveMode.Overwrite).orc(tmpDir)
            //删除主目录旧数据文件
            deleteFiles(fs, basePath, prefix)
            //将新数据文件改名并移至主目录
            moveFiles(fs, tmpDir, basePath, prefix)
        } catch {
            case e: Exception => throw new RuntimeException(e)
        } finally {
            if (null != fs) fs.close()
        }
    }

    /**
     * 生成临时目录 若存在则删除 并设置在关闭会话时候自动删除
     *
     * @param fs         hdfs
     * @param tmpDirName 临时目录名
     */
    def createTmpDir(fs: FileSystem, tmpDirName: String): Unit = {
        val tmpDir = new Path(tmpDirName)
        if (fs.exists(tmpDir) && fs.isDirectory(tmpDir)) {
            fs.delete(tmpDir, true)
        }
        fs.mkdirs(tmpDir)
        fs.deleteOnExit(tmpDir)
    }

    /**
     * 获取文件名前缀
     *
     * @param fileName 文件名
     * @return
     */
    def getFilePrefix(fileName: String): String = {
        var prefix: String = null
        val pattern = ".*-\\d*$"
        if (Pattern.matches(pattern, fileName)) {
            prefix = fileName.substring(0, fileName.lastIndexOf("-") + 1)
        } else {
            throw new IllegalArgumentException(s"[$fileName] 此hdfs文件名不合法！请参考[eg:ORC-001]")
        }
        prefix
    }

    /**
     * 删除指定目录下匹配前缀的文件
     *
     * @param fs     hdfs
     * @param prefix 文件名前缀
     */
    def deleteFiles(fs: FileSystem, path: String, prefix: String): Unit = {
        val filter = new PathFilter {
            override def accept(path: Path): Boolean = {
                path.getName.startsWith(prefix)
            }
        }
        val listStatus = fs.listStatus(new Path(path), filter)
        listStatus.foreach(ls => {
            val pathTmp = ls.getPath
            if (fs.exists(pathTmp) && fs.isFile(pathTmp)) {
                fs.delete(pathTmp, false)
            }
        })
    }

    /**
     * 将原目录下的文件改名（增加指定前缀）并移动移动至新目录
     * 只改 part- 开头的文件
     *
     * @param fs         hdfs
     * @param sourcePath 源目录
     * @param targetPath 目标目录
     * @param prefix     文件前缀
     */
    def moveFiles(fs: FileSystem, sourcePath: String, targetPath: String, prefix: String): Unit = {
        val filter = new PathFilter {
            override def accept(path: Path): Boolean = {
                path.getName.startsWith("part-")
            }
        }
        val listStatus = fs.listStatus(new Path(sourcePath), filter)
        listStatus.foreach(ls => {
            val pathTmp = ls.getPath
            if (fs.exists(pathTmp) && fs.isFile(pathTmp)) {
                val dstPath = new Path(targetPath + prefix + pathTmp.getName)
                fs.rename(pathTmp, dstPath)
            }
        })
    }
}
