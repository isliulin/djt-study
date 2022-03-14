package com.djt.test.hbase

import com.djt.test.spark.action.AbsActionTest
import com.djt.utils.{ConfigConstant, RandomUtils}
import org.apache.commons.lang3.Validate
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.spark.SparkConf
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.{ActionRequest, ActionResponse}
import org.elasticsearch.client._
import org.elasticsearch.client.indices.GetMappingsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{LoggingDeprecationHandler, NamedXContentRegistry, XContentFactory, XContentType}
import org.elasticsearch.plugins.SearchPlugin
import org.elasticsearch.search.SearchModule
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.spark.rdd.EsSpark
import org.junit.{After, Before, Test}

import java.util
import java.util.Collections
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


/**
 * @author 　djt317@qq.com
 * @since 　 2021-05-06
 */
class EsTest extends AbsActionTest {

    var esClient: RestHighLevelClient = _
    val options: RequestOptions = RequestOptions.DEFAULT
    var esHosts: String = _

    var restClient: RestClient = _

    val clientPoolSize = 10
    val clientPool = new util.Vector[RestHighLevelClient](clientPoolSize)

    val atoInt: AtomicInteger = new AtomicInteger(0)

    @Before
    override def before(): Unit = {
        super.before()
        //esHosts = config.getProperty(ParamConstant.ES_HOST)
        //esHosts = "172.20.20.183:9200"
        esHosts = "172.20.4.87:9200,172.20.4.88:9200,172.20.4.89:9200"
        esClient = createClient

        for (_ <- 0 until clientPoolSize) {
            clientPool.add(createClient)
        }
        println(s"连接池大小:${clientPool.size()}")
        restClient = createRestClient()
    }

    @After
    def afterTest(): Unit = {
        closeClient(esClient)
        restClient.close()
        clientPool.foreach(_.close())
    }

    override def setSparkConf(sparkConf: SparkConf): Unit = {
        super.setSparkConf(sparkConf)
        sparkConf.set("es.nodes", config.getProperty(ConfigConstant.Es.ES_HOST))
        sparkConf.set("es.mapping.date.rich", "false")
        //sparkConf.set("es.port", "")
    }

    def createClient: RestHighLevelClient = {
        new RestHighLevelClient(RestClient.builder(extractHosts(esHosts): _*))
    }

    /**
     * 轮询获取客户端
     *
     * @return
     */
    def getClientFromPool: RestHighLevelClient = {
        var index = atoInt.getAndAdd(1)
        if (index >= clientPoolSize) {
            index = 0
            atoInt.set(1)
        }
        clientPool.get(index)
    }

    @Test
    def testGetClientFromPool(): Unit = {
        for (_ <- 1 to 100) {
            getClientFromPool
        }
    }

    def closeClient(esClient: RestHighLevelClient): Unit = {
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

    def createRestClient(): RestClient = {
        val builder = RestClient.builder(extractHosts(esHosts): _*).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback {
            override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = {
                requestConfigBuilder.setConnectTimeout(300000)
                requestConfigBuilder.setSocketTimeout(300000)
                requestConfigBuilder.setConnectionRequestTimeout(300000)
                requestConfigBuilder
            }
        }).setMaxRetryTimeoutMillis(300000)
        builder.build
    }

    def getQuery(transTime: String): String = {
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
           |  },
           |  "sort": {
           |    "F1": "DESC",
           |    "F10": "ASC"
           |  }
           |}
           |""".stripMargin
    }

    def query(transTime: String): Unit = {
        val index = "t_test_oom_*"
        val query = getQuery(transTime)

        val searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList[SearchPlugin]())
        val registry = new NamedXContentRegistry(searchModule.getNamedXContents)
        val parser = XContentFactory.xContent(XContentType.JSON).createParser(registry, LoggingDeprecationHandler.INSTANCE, query)
        val searchSourceBuilder = SearchSourceBuilder.fromXContent(parser)
        //searchSourceBuilder.size(10);
        val request = new SearchRequest(index)
        request.source(searchSourceBuilder)
        val esClient = getClientFromPool
        val response = esClient.search(request, options)
        //closeClient(esClient)
        //printRR(request, response)
    }

    def query2(transTime: String): Unit = {
        val index = "t_test_oom_*/xdata"
        val query = getQuery(transTime)

        val endpoint = index + "/_search"

        val entity = new NStringEntity(query, ContentType.APPLICATION_JSON)
        val request = new Request("POST", endpoint)
        request.setEntity(entity)
        //val restClient = createRestClient()
        val response = restClient.performRequest(request)
        //restClient.close()
        //val str = EntityUtils.toString(response.getEntity)
        //println(str)
    }

    def query3(): Unit = {
        val index = "t_test_oom_202001"
        val request = new GetMappingsRequest().indices(index)
        val client = createClient
        val response = client.indices.getMapping(request, RequestOptions.DEFAULT)
        client.close()
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
        val esClient = getClientFromPool
        val bulkResponse = esClient.bulk(bulkRequest, options)
        //printRR(bulkRequest, bulkResponse)
    }

    var poolSize = 2000

    @Test
    def testInsert(): Unit = {
        poolSize = 100
        val pool = Executors.newFixedThreadPool(poolSize)
        while (true) {
            pool.execute(new Runnable {
                override def run(): Unit = {
                    while (true) {
                        val start = System.currentTimeMillis()
                        insert(100)
                        val end = System.currentTimeMillis()
                        println(s"${Thread.currentThread().getName} 插入耗时：${(end - start) / 1000}s")
                    }
                }
            })
            Thread.sleep(10)
        }
        pool.shutdown()
        while (!pool.isTerminated) ()
    }

    @Test
    def testQuery(): Unit = {
        poolSize = 1
        val startTime = "2020-01-01"
        val endTime = "2020-09-30"
        val pool = Executors.newFixedThreadPool(poolSize)
        while (true) {
            pool.execute(new Runnable {
                override def run(): Unit = {
                    while (true) {
                        val start = System.currentTimeMillis()
                        val dateTime = RandomUtils.getRandomDate(startTime, endTime)
                        query(dateTime)
                        //query2(dateTime)
                        //query3()
                        val end = System.currentTimeMillis()
                        println(s"${Thread.currentThread().getName} 查询耗时：${(end - start) / 1000}s")
                    }
                }
            })
            Thread.sleep(10)
        }
        pool.shutdown()
        while (!pool.isTerminated) ()
    }

    @Test
    def testSparkWrite(): Unit = {
        poolSize = 10
        val sparkSession = getSparkSession
        val partitions = 100
        val indexName = "t_test_oom_202009/xdata"
        val startTime = "2020-07-01"
        val endTime = "2020-09-30"
        val pool = Executors.newFixedThreadPool(poolSize)
        while (true) {
            pool.execute(new Runnable {
                override def run(): Unit = {
                    while (true) {
                        val dataList = new ListBuffer[(String, util.HashMap[String, Any])]()
                        for (_ <- 1 to 1000) {
                            val dateTime = RandomUtils.getRandomDate(startTime, endTime)
                            val id = System.currentTimeMillis() + RandomUtils.getString(0, Long.MaxValue - 1, 14).substring(0, 14)
                            val dataMap = new util.HashMap[String, Any]()
                            dataMap.put("F1", dateTime)
                            for (i <- 2 to 20) {
                                dataMap.put(s"F$i", RandomUtils.getRandomNumber(0, Long.MaxValue - 1).toString)
                            }
                            dataList.append((id, dataMap))
                        }
                        val esRdd = sparkSession.sparkContext.parallelize(dataList, partitions)
                        EsSpark.saveToEsWithMeta(esRdd, indexName)
                    }
                }
            })
            Thread.sleep(10)
        }
    }

    @Test
    def testSparkQuery(): Unit = {
        val index = "t_test_oom_202001/xdata"
        val query = getQuery("2020-01-01 00:00:00")
        val count = EsSpark.esRDD(getSparkSession.sparkContext, index, query).count()
        println("==================" + count)
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
