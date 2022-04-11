package com.djt.test.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.setting.dialect.Props;
import com.alibaba.fastjson.JSON;
import com.djt.utils.EsUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-04-08
 */
public class EsUtilsTest {

    private static final Props props = new Props();
    private static RestHighLevelClient restHighLevelClient = null;
    private static final RequestOptions requestOptions = RequestOptions.DEFAULT;

    @Before
    public void before() {
        props.put("es.host", "172.20.4.87:9200,172.20.4.88:9200,172.20.4.89:9200");
        restHighLevelClient = EsUtils.getRestHighLevelClient(props);
    }

    @After
    public void after() {
        IoUtil.close(restHighLevelClient);
    }

    @Test
    public void testQuery() {
        String index = "t_test_oom_202006";
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(10);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, requestOptions);
            System.out.println(JSON.toJSONString(JSON.parseObject(searchResponse.toString()), true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpsert() {
        String indexName = "t_flink_metric_log_20220408";
        String indexType = "_doc";
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("c_time", LocalDateTime.now().format(DatePattern.NORM_DATETIME_FORMATTER));
        dataMap.put("key", "Status.JVM.Memory.Heap.Used");
        dataMap.put("value", "2750386672");
        String id = UUID.randomUUID().toString(true);
        UpdateRequest updateRequest = new UpdateRequest(indexName, indexType, id).doc(dataMap).upsert(dataMap);
        try {
            restHighLevelClient.update(updateRequest, requestOptions);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
