package com.djt.entity;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;

import java.util.List;

import static com.djt.tools.impl.FlinkMetricsCollect.JOB_URL;
import static com.djt.tools.impl.FlinkMetricsCollect.TM_URL;

/**
 * 监控类型
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-08
 */
public enum MetricType {

    /**
     * 算子
     */
    OPERATOR(JOB_URL + "/vertices/{}/metrics?get={}"),

    /**
     * TM
     */
    TASKMANAGER(TM_URL + "/{}/metrics?get={}");

    /**
     * 请求URL
     */
    private final String url;

    MetricType(String url) {
        this.url = url;
    }

    /**
     * 发送请求
     *
     * @param args 参数
     * @return List<MetricEntity>
     */
    public List<MetricEntity> queryMetrics(Object... args) {
        return JSON.parseArray(HttpUtil.get(StrUtil.format(url, args)), MetricEntity.class);
    }
}
