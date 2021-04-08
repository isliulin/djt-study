package com.djt.test.other

import com.djt.test.dto.CaseClass.TermInfo
import com.djt.test.spark.action.AbsActionTest
import com.djt.utils.ParamConstant
import org.apache.kudu.client.{KuduClient, KuduPredicate, KuduScanner, SessionConfiguration}
import org.apache.kudu.spark.kudu.KuduContext
import org.junit.{After, Before, Test}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-01
 */
class KuduTest extends AbsActionTest {

    var kuduMaster: String = _
    private var kuduContext: KuduContext = _
    private var kuduClient: KuduClient = _

    @Before
    override def before(): Unit = {
        super.before()
        kuduMaster = config.getProperty(ParamConstant.KUDU_MASTER)
        kuduContext = new KuduContext(kuduMaster, getSparkSession.sparkContext)
        kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    }

    @After
    def after(): Unit = {
        if (kuduClient != null) {
            kuduClient.close()
        }
    }

    @Test
    def testKuduContext(): Unit = {
        val sparkSession = getSparkSession
        import sparkSession.implicits._
        val dataArr = Array(TermInfo(dt = "20210403", flag = "0", mer_no = "666", term_no = "123", min_trd_date = "20210401"),
            TermInfo(dt = "20210403", flag = "0", mer_no = "777", term_no = "456", min_trd_date = "20210401"),
            TermInfo(dt = "20210403", flag = "1", mer_no = "888", term_no = "789", min_trd_date = "20210402"),
            TermInfo(dt = "20210403", flag = "1", mer_no = "999", term_no = "987", min_trd_date = "20210403"))

        val df = sparkSession.sparkContext.parallelize(dataArr).toDF()
        df.show(20, truncate = false)

        val tableName = "edw.agt_inc_act_terms_1d"
        kuduContext.upsertRows(df, tableName)
    }


    @Test
    def testInsert(): Unit = {
        val tableName = "edw.agt_inc_act_terms_1d"
        val table = kuduClient.openTable(tableName)
        val session = kuduClient.newSession()
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
        val insert = table.newInsert()
        val row = insert.getRow
        row.addString("dt", "20210406")
        row.addString("flag", "1")
        row.addString("mer_no", "999")
        row.addString("term_no", "321")
        row.addString("min_trd_date", "20210406")
        session.apply(insert)

        val upsert = table.newUpsert()
        val row2 = upsert.getRow


        session.close()
    }


    @Test
    def testQuery(): Unit = {
        val tableName = "edw.agt_inc_act_terms_1d"
        val table = kuduClient.openTable(tableName)
        //等于查询
        var predicate = KuduPredicate.newComparisonPredicate(table.getSchema.getColumn("mer_term_no"), KuduPredicate.ComparisonOp.EQUAL, "666")
        var scanner = getScanner(tableName, predicate)
        printScanner(scanner)

        //in 查询
        val inValues = new ListBuffer[String]()
        inValues.append("777")
        inValues.append("888")
        predicate = KuduPredicate.newInListPredicate(table.getSchema.getColumn("mer_term_no"), inValues)
        scanner = getScanner(tableName, predicate)
        printScanner(scanner)
    }

    /**
     * 获取Scanner
     *
     * @param tableName 表名
     * @param predicate 查询条件
     * @return
     */
    def getScanner(tableName: String, predicate: KuduPredicate*): KuduScanner = {
        val table = kuduClient.openTable(tableName)
        val builder = kuduClient.newScannerBuilder(table)
        if (predicate != null && predicate.nonEmpty) {
            predicate.foreach(p => {
                builder.addPredicate(p)
            })
        }
        builder.build()
    }

    /**
     * 打印查询结果
     *
     * @param scanner 查询结果
     */
    def printScanner(scanner: KuduScanner): Unit = {
        println("=================================================")
        while (scanner.hasMoreRows) {
            val iterator = scanner.nextRows()
            while (iterator.hasNext) {
                val result = iterator.next()
                result.getSchema.getColumns.foreach(s => {
                    val field = s.getName
                    val value = if (!result.isNull(field)) {
                        result.getString(field)
                    } else {
                        null
                    }
                    print(field + "=" + value + " ")
                })
                println()
            }
        }
    }


    @Test
    def testSparkKudu(): Unit = {
        val tableName = "edw.agt_inc_act_terms_1d"
        val df1 = getSparkSession.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" -> tableName))
                .format("kudu").load().where("dt = '20210403'")


        val df2 = getSparkSession.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" -> tableName))
                .format("kudu").load().where("dt = '20210404'")

        //df1.show(20, truncate = false)
        //df2.show(20, truncate = false)
        df1.join(df2, Array("mer_no", "term_no"))
                .select(df1("dt"), df1("flag"), df1("mer_no"), df1("term_no"))
                .selectExpr("*", "20210407 AS min_trd_date")
                .show(20, truncate = false)

    }

    @Test
    def testSparkKudu2(): Unit = {
        val tableName = "edw.agt_inc_act_terms_1d"
        val execDate = "20210402"

        val df1 = getSparkSession.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" -> tableName))
                .format("kudu").load().where(s"dt = '20210405' and (dt = min_trd_date or '$execDate' < min_trd_date)")
        df1.show(20, truncate = false)
    }


}

