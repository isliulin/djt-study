package com.djt.test.other

import com.djt.test.spark.action.AbsActionTest
import com.djt.utils.{DjtConstant, RandomUtils}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, MultiRowRangeFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.junit.{After, Before, Test}

import scala.collection.mutable.ListBuffer

/**
 * @author 　djt317@qq.com
 * @since 　 2021-03-24
 */
class HBaseTest extends AbsActionTest {

    val hbaseConf: Configuration = HBaseConfiguration.create()
    var conn: Connection = _

    @Before
    override def before(): Unit = {
        super.before()
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "cdh-dev01.jlpay.io,cdh-dev02.jlpay.io,cdh-dev03.jlpay.io")
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
    def testPut2(): Unit = {
        val tableName = "RC:STAT_RESULT"
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")
        val row = Bytes.toBytes(DigestUtils.md5Hex("84944037011A00O"))
        val field = Bytes.toBytes("merchValidTradeDays")
        val field2 = Bytes.toBytes("merchLast30DaysSumMax")

        val put = new Put(row)
        put.addColumn(cf, field, Bytes.toBytes("10"))
        put.addColumn(cf, field2, Bytes.toBytes("10000000"))
        table.put(put)
        println(Bytes.toString(row) + ":" + Bytes.toString(field) + "=" + getFieldValue(table, cf, row, field))
        table.close()

        val get = new Get(row)
        val result = table.get(get)
        val value2 = result.getValue(cf, field)
        Bytes.toString(value2)
    }

    @Test
    def testNewApi(): Unit = {
        val sparkSession = getSparkSession
        val cf = Bytes.toBytes("cf")
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "EDW:AGT_INC_ACT_TERMS_1D")
        //多范围查找
        import scala.collection.JavaConversions._
        val rangeList = new ListBuffer[MultiRowRangeFilter.RowRange]()
        for (x <- 0 to 9) {
            val startRow = s"${x}_0_20210303"
            val stopRow = s"${x}_0_20210306"
            rangeList.append(new MultiRowRangeFilter.RowRange(startRow, true, stopRow, true))
        }
        val multiRowRangeFilter = new MultiRowRangeFilter(rangeList)
        val scan = new Scan().addFamily(cf).setFilter(multiRowRangeFilter)
        val proto = ProtobufUtil.toScan(scan)
        val scanToString = Base64.encodeBytes(proto.toByteArray)
        hbaseConf.set(TableInputFormat.SCAN, scanToString)

        sparkSession.sparkContext.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]).foreach(tup2 => {
            println(Bytes.toString(tup2._2.getRow))
        })
    }

    @Test
    def testScan(): Unit = {
        import scala.collection.JavaConversions._
        val tableName = "EDW:AGT_INC_ACT_TERMS_1D"
        clearHbaseData(tableName)
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")

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

    @Test
    def testScan2(): Unit = {
        val tableName = "TEST:T_TEST_DJT"
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")

        val scan = new Scan().addFamily(cf)
        val resultScanner = table.getScanner(scan)
        val iter = resultScanner.iterator()
        while (iter.hasNext) {
            val sb = new StringBuilder
            val result = iter.next()
            val rowkey = Bytes.toString(result.getRow)
            sb.append(rowkey).append("=>")
            result.rawCells().foreach(cell => {
                val q = Bytes.toString(CellUtil.cloneQualifier(cell))
                val v = Bytes.toLong(CellUtil.cloneValue(cell))
                sb.append(q).append("=").append(v).append(" ")
            })
            println(sb)
        }
        table.close()
    }

    @Test
    def testBathPut(): Unit = {
        import scala.collection.JavaConversions._
        val tableName = "EDW:AGT_INC_ACT_TERMS_1D"
        clearHbaseData(tableName)
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")
        val putList = new ListBuffer[Put]()
        val field = Bytes.toBytes("TERM_NO")
        for (_ <- 1 to 100) {
            val date = RandomUtils.getRandomDate("20210301", "20210310", DjtConstant.YMD)
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
    }

    @Test
    def testBathPut2(): Unit = {
        //create 'RC:EVENT_FLOW',{ NAME => 'event', DATA_BLOCK_ENCODING => 'PREFIX_TREE', REPLICATION_SCOPE => '0'}
        val tableName = "TEST:T_TEST_DJT"
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")
        val put = new Put(Bytes.toBytes("123456"))
        put.addColumn(cf, Bytes.toBytes("F2"), Bytes.toBytes("444"))
        put.addColumn(cf, Bytes.toBytes("F3"), Bytes.toBytes("444"))
        table.put(put)
    }

    @Test
    def testIncrement(): Unit = {
        val tableName = "TEST:T_TEST_DJT"
        val table = conn.getTable(TableName.valueOf(tableName))
        val cf = Bytes.toBytes("cf")
        val ql = Bytes.toBytes("test1")
        val inc = new Increment(Bytes.toBytes("123456"))
        inc.addColumn(cf, ql, 1L)
        table.increment(inc)
        table.close()
        testScan2()
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
