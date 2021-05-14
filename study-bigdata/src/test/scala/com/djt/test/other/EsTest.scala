package com.djt.test.other

import com.djt.test.spark.action.AbsActionTest
import com.djt.utils.RandomUtils
import org.apache.commons.lang3.Validate
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.{ActionRequest, ActionResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{LoggingDeprecationHandler, NamedXContentRegistry, XContentFactory, XContentType}
import org.elasticsearch.plugins.SearchPlugin
import org.elasticsearch.search.SearchModule
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.junit.{After, Before, Test}

import java.util
import java.util.Collections
import java.util.concurrent.Executors

/**
 * @author 　djt317@qq.com
 * @since 　 2021-05-06
 */
class EsTest extends AbsActionTest {

    var esClient: RestHighLevelClient = _
    val options: RequestOptions = RequestOptions.DEFAULT

    @Before
    override def before(): Unit = {
        super.before()
        //val esHosts = config.getProperty(ParamConstant.ES_HOST)
        val esHosts = "172.20.20.183:9201"
        esClient = new RestHighLevelClient(RestClient.builder(extractHosts(esHosts): _*))
    }

    @After
    def afterTest(): Unit = {
        try if (esClient != null) {
            esClient.close()
        }
    }

    def extractHosts(esHosts: String): Array[HttpHost] = {
        Validate.notNull(esHosts, "es连接地址不能为空！")
        val hostAndPortArray = esHosts.split(",")
        val httpHostArray = new Array[HttpHost](hostAndPortArray.length)
        for (i <- 0 until hostAndPortArray.length) {
            val address = hostAndPortArray(i).split(":")
            val host = address(0)
            var port = 9200
            if (address.length > 1) port = address(1).toInt
            httpHostArray(i) = new HttpHost(host, port)
        }
        httpHostArray
    }

    def query(transTime: String): Unit = {
        val index = "t_test_oom_*"
        val query =
            s"""
               |{
               |  "size": 0,
               |  "aggs": {
               |    "test_group_by": {
               |      "terms": {
               |        "field": "F3",
               |        "order": {
               |          "amt": "desc"
               |        }
               |      },
               |      "aggs": {
               |        "amt": {
               |          "sum": {
               |            "field": "F2"
               |          }
               |        }
               |      }
               |    }
               |  },
               |  "query": {
               |    "bool": {
               |      "must": [
               |        {
               |          "range": {
               |            "F1": {
               |              "gte": "$transTime"
               |            }
               |          }
               |        }
               |      ]
               |    }
               |  }
               |}
               |""".stripMargin

        val searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList[SearchPlugin]())
        val registry = new NamedXContentRegistry(searchModule.getNamedXContents)
        val parser = XContentFactory.xContent(XContentType.JSON).createParser(registry, LoggingDeprecationHandler.INSTANCE, query)
        val searchSourceBuilder = SearchSourceBuilder.fromXContent(parser)
        //searchSourceBuilder.size(10);
        val request = new SearchRequest(index)
        request.source(searchSourceBuilder)
        val response = esClient.search(request, options)
        //printRR(request, response)
    }

    def insert(size: Int): Unit = {
        val bulkRequest = new BulkRequest
        for (_ <- 1 to size) {
            val startTime = "2020-01-01"
            val endTime = "2020-09-30"
            val dateTime = RandomUtils.getRandomDate(startTime, endTime)
            val indexName = s"t_test_oom_${dateTime.replaceAll("[-/:\\s]", "").substring(0, 6)}"
            val indexType = "xdata"
            val id = System.currentTimeMillis() + RandomUtils.getString(0, Long.MaxValue - 1, 14).substring(0, 14)
            val dataMap = new util.HashMap[String, Any]()
            dataMap.put("F1", dateTime)
            for (i <- 2 to 20) {
                dataMap.put(s"F$i", RandomUtils.getRandomNumber(0, Long.MaxValue - 1).toString)
            }

            val updateRequest = new UpdateRequest(indexName, indexType, id).doc(dataMap).upsert(dataMap)
            bulkRequest.add(updateRequest)
            //val response = esClient.update(request, options)
            //printRR(request, response)
        }
        val bulkResponse = esClient.bulk(bulkRequest, options)
        //printRR(bulkRequest, bulkResponse)
    }

    val poolSize = 200

    @Test
    def testInsert(): Unit = {
        val pool = Executors.newFixedThreadPool(poolSize)
        while (true) {
            pool.execute(new Runnable {
                override def run(): Unit = {
                    val start = System.currentTimeMillis()
                    insert(1000)
                    val end = System.currentTimeMillis()
                    println(s"${Thread.currentThread().getName} 插入耗时：${(end - start) / 1000}s")
                }
            })
            Thread.sleep(10)
        }
        pool.shutdown()
        while (!pool.isTerminated) ()
    }

    @Test
    def testQuery(): Unit = {
        val startTime = "2020-01-01"
        val endTime = "2020-09-30"
        val pool = Executors.newFixedThreadPool(poolSize)
        while (true) {
            pool.execute(new Runnable {
                override def run(): Unit = {
                    val start = System.currentTimeMillis()
                    val dateTime = RandomUtils.getRandomDate(startTime, endTime)
                    query(dateTime)
                    val end = System.currentTimeMillis()
                    println(s"${Thread.currentThread().getName} 查询耗时：${(end - start) / 1000}s")
                }
            })
            Thread.sleep(10)
        }
        pool.shutdown()
        while (!pool.isTerminated) ()
    }


    @Test
    def testQueryAndWrite(): Unit = {
        val size = 500
        val startTime = "2018-01-01"
        val endTime = "2021-05-06"
        val pool = Executors.newFixedThreadPool(size)
        while (true) {
            pool.execute(new Runnable {
                override def run(): Unit = {
                    val start = System.currentTimeMillis()
                    insert(1000)
                    val insertEnd = System.currentTimeMillis()
                    val randTime = RandomUtils.getRandomDate(startTime, endTime)
                    query(randTime)
                    val end = System.currentTimeMillis()
                    println(s"${Thread.currentThread().getName} 请求总耗时：${(end - start) / 1000}s " +
                            s"插入耗时：${(insertEnd - start) / 1000}s 查询耗时：${(end - insertEnd) / 1000}s")
                }
            })
            Thread.sleep(100)
        }
        pool.shutdown()
        while (!pool.isTerminated) ()
    }

    private def printRR(request: ActionRequest, response: ActionResponse): Unit = {
        println("请求 => ", request)
        println("响应 => ", response)
    }

    @Test
    def testSys(): Unit = {
        val timestamp = System.currentTimeMillis()
        val randNum = RandomUtils.getString(0, Long.MaxValue - 1, 14).substring(0, 14)
        val ss = timestamp + randNum
        println(timestamp)
        println(randNum)
        println(ss)
    }

}
