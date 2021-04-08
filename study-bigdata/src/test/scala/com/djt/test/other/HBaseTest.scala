package com.djt.test.other

import com.djt.utils.RandomUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, MultiRowRangeFilter}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.junit.{After, Before, Test}

import scala.collection.mutable.ListBuffer

/**
 * @author 　djt317@qq.com
 * @since 　 2021-03-24
 */
class HBaseTest {

    var conn: Connection = _

    @Before
    def before(): Unit = {
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "xdata-uat03.jlpay.io,cdh-dev01.jlpay.io,cdh-dev02.jlpay.io")
        hbaseConf.set(HConstants.CLIENT_PORT_STR, "2181")
        conn = ConnectionFactory.createConnection(hbaseConf)
    }

    @After
    def after(): Unit = {
        if (null != conn) {
            conn.close()
        }
    }

    @Test
    def testPut(): Unit = {
        val tableName = "EDW:AGT_INC_ACT_TERMS_1D"
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")
        val row = Bytes.toBytes("666")
        val field = Bytes.toBytes("TERM_NO")
        println(exists(table, row) + ":" + getFieldValue(table, cf, row, field))

        val put = new Put(row)
        val value = Bytes.toBytes("888")
        put.addColumn(cf, field, value)

        val checkValue = Bytes.toBytes("888")
        //当 field <> checkValue 的时候插入
        table.checkAndPut(row, cf, field, CompareFilter.CompareOp.NOT_EQUAL, checkValue, put)
        println(exists(table, row) + ":" + getFieldValue(table, cf, row, field))

        table.close()
    }

    @Test
    def testNewApi(): Unit = {
        val cf = Bytes.toBytes("cf")
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "EDW:AGT_INC_ACT_TERMS_1D")
        //多范围查找
        import scala.collection.JavaConversions._
        val rangeList = new ListBuffer[MultiRowRangeFilter.RowRange]()
        for (x <- 0 to 9) {
            val startRow = s"${x}_20210224_0"
            val stopRow = s"${x}_20210224_0"
            rangeList.append(new MultiRowRangeFilter.RowRange(startRow, true, stopRow, true))
        }
        val multiRowRangeFilter = new MultiRowRangeFilter(rangeList)
        val scan = new Scan().addFamily(cf).setFilter(multiRowRangeFilter)
        val proto = ProtobufUtil.toScan(scan)
        val scanToString = Base64.encodeBytes(proto.toByteArray)
        hbaseConf.set(TableInputFormat.SCAN, scanToString)

        //sparkSession.sparkContext.newAPIHadoopRDD(
        //    hbaseConf,
        //    classOf[TableInputFormat],
        //    classOf[ImmutableBytesWritable],
        //    classOf[Result])
    }

    @Test
    def testPutAndScan(): Unit = {
        //先批量插入
        import scala.collection.JavaConversions._
        val tableName = "EDW:AGT_INC_ACT_TERMS_1D"
        clearHbaseData(tableName)
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")
        val putList = new ListBuffer[Put]()
        val field = Bytes.toBytes("TERM_NO")
        println("写入===========================")
        for (_ <- 1 to 100) {
            val date = RandomUtils.getRandomDate("20210301", "20210310", RandomUtils.YMD)
            val termNo = RandomUtils.getRandomNumber(100, 10000).toString
            val flag = RandomUtils.getRandomNumber(0, 1).toString
            val rowkey = createRowkey(date, termNo, flag)
            println(rowkey)
            val put = new Put(Bytes.toBytes(rowkey))
            val value = Bytes.toBytes(termNo)
            put.addColumn(cf, field, value)
            putList.append(put)
        }
        table.put(putList)

        //再查询
        println("查询===========================")
        val rangeList = new ListBuffer[MultiRowRangeFilter.RowRange]()
        for (x <- 0 to 9) {
            val startRow = s"${x}_0_20210303"
            val stopRow = s"${x}_0_20210306"
            val rowrange = new MultiRowRangeFilter.RowRange(startRow, true, stopRow, false)
            rangeList.append(rowrange)
        }
        val multiRowRangeFilter = new MultiRowRangeFilter(rangeList)
        val scan = new Scan().addFamily(cf).setFilter(multiRowRangeFilter)
        val resultScanner = table.getScanner(scan)
        val iter = resultScanner.iterator()
        while (iter.hasNext) {
            val result = iter.next()
            val rowString = Bytes.toString(result.getRow)
            println(rowString)
        }
        table.close()
    }


    def getFieldValue(table: Table, family: Array[Byte], row: Array[Byte], qualifier: Array[Byte]): String = {
        val get = new Get(row)
        val result = table.get(get)
        val value = result.getValue(family, qualifier)
        Bytes.toString(value)
    }

    def delete(table: Table, row: Array[Byte]): Unit = {
        val delete = new Delete(row)
        table.delete(delete)
    }

    def exists(table: Table, row: Array[Byte]): Boolean = {
        table.exists(new Get(row))
    }

    /**
     * 生成rowkey
     *
     * @return
     */
    def createRowkey(date: String, termNo: String, flag: String): String = {
        if (StringUtils.isAnyBlank(date, termNo, flag)) return null
        val randomNum = termNo.reverse.codePointAt(0).toString.last.toString
        val demo = Array(randomNum, flag, date, termNo).mkString("_")
        StringUtils.rightPad(demo, 25, '0')
    }

    /**
     * 清空HBase表
     *
     * @param tableName 表名
     */
    def clearHbaseData(tableName: String): Unit = {
        val admin = conn.getAdmin
        val table = TableName.valueOf(tableName)
        admin.disableTable(table)
        admin.truncateTable(table, true)
        admin.close()
    }

}
