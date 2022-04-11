package com.djt.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.setting.dialect.Props;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

/**
 * ES工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-08
 */
public class EsUtils {

    /**
     * ES 客户端
     */
    private static RestHighLevelClient restHighLevelClient = null;

    /**
     * 获取ES客户端
     *
     * @param props 配置
     * @return RestHighLevelClient
     */
    public static RestHighLevelClient getRestHighLevelClient(Props props) {
        if (restHighLevelClient == null) {
            synchronized (EsUtils.class) {
                if (restHighLevelClient == null) {
                    restHighLevelClient = new RestHighLevelClient(RestClient.builder(getHttpHosts(props.getStr("es.host"))));
                }
            }
        }
        return restHighLevelClient;
    }

    /**
     * 获取httphost列表
     *
     * @param hostsStr host列表(逗号分隔)
     * @return HttpHost[]
     */
    public static HttpHost[] getHttpHosts(String hostsStr) {
        Validate.notBlank(hostsStr, "ES地址不能为空！");
        String[] hostArr = StringUtils.split(hostsStr, ',');
        HttpHost[] hosts = new HttpHost[hostArr.length];
        for (int i = 0; i < hostArr.length; i++) {
            hosts[i] = HttpHost.create(hostArr[i].trim());
        }
        return hosts;
    }

    /**
     * 批量upsert数据
     *
     * @param client         客户端
     * @param indexName      索引名
     * @param indexType      索引类型
     * @param requestOptions 请求配置
     * @param dataList       数据列表
     * @throws Exception e
     */
    public static <T> void upsert(RestHighLevelClient client, String indexName, String indexType,
                                  RequestOptions requestOptions, List<T> dataList) throws Exception {
        Validate.notNull(client, "ES客户端不能为空！");
        Validate.notEmpty(indexName, "索引名不能为空！");
        Validate.notEmpty(indexType, "索引类型不能为空！");
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        dataList.forEach(data -> {
            String id = UUID.randomUUID().toString(true);
            String json = JSON.toJSONString(data);
            UpdateRequest updateRequest = new UpdateRequest(indexName, indexType, id)
                    .doc(json, XContentType.JSON).upsert(json, XContentType.JSON);
            bulkRequest.add(updateRequest);
        });
        BulkResponse response = client.bulk(bulkRequest, requestOptions);
        if (response.hasFailures()) {
            throw new RuntimeException("ES插入失败: " + response.buildFailureMessage());
        }
    }

}
