package com.djt.test.utils;

import cn.hutool.core.io.IoUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
public class HttpTest {

    @Test
    public void testIp() throws UnknownHostException {
        System.out.println(InetAddress.getLocalHost().getHostName());
        System.out.println(InetAddress.getLocalHost().getHostAddress());
        System.out.println(Arrays.toString(InetAddress.getLocalHost().getAddress()));
    }

    @Test
    public void testHttpUtil() {
        String content = HttpUtil.get("https://www.baidu.com");
        System.out.println(content);
    }

    @Test
    public void testHttpClientGet() {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        System.out.println(httpClient.getClass().getName());
        String url = "https://www.baidu.com/";
        HttpGet httpGet = new HttpGet(url);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000).setConnectionRequestTimeout(1000)
                .setSocketTimeout(5000).build();
        httpGet.setConfig(requestConfig);
        httpGet.addHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        System.out.println("发送请求=>" + httpGet);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            String result = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            System.out.println("请求响应=>" + response);
            System.out.println("返回结果=>" + result);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(response);
            IoUtil.close(httpClient);
        }
    }

    @Test
    public void testHttpClientPost() {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        String url = "https://www.baidu.com/";
        HttpPost httpPost = new HttpPost(url);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setConnectionRequestTimeout(1000)
                .setSocketTimeout(5000)
                .build();
        httpPost.setConfig(requestConfig);

        JSONObject jsonObject = new JSONObject();
        StringEntity stringEntity = new StringEntity(jsonObject.toJSONString(), ContentType.APPLICATION_JSON);
        stringEntity.setContentEncoding(StandardCharsets.UTF_8.name());
        httpPost.setEntity(stringEntity);
        System.out.println("发送请求=>" + httpPost);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            String result = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            System.out.println("请求响应=>" + response);
            System.out.println("返回结果=>" + result);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(response);
            IoUtil.close(httpClient);
        }
    }

}
