package com.djt.entity;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

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
    OPERATOR(JOB_URL + "/vertices/{0}/metrics?get={1}"),

    /**
     * TM
     */
    TASKMANAGER(TM_URL + "/{0}/metrics?get={1}");

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
        List<MetricEntity> resultList = JSON.parseArray(HttpUtil.get(StrUtil.indexedFormat(url, args)), MetricEntity.class);
        resultList.forEach(metricEntity -> {
            switch (this) {
                case OPERATOR:
                    metricEntity.setGroup(StringUtils.substringAfter(metricEntity.getKey(), "."));
                    metricEntity.setKey(StringUtils.substringBefore(metricEntity.getKey(), "."));
                    break;
                case TASKMANAGER:
                    metricEntity.setGroup(metricEntity.getKey());
                    metricEntity.setKey(String.valueOf(args[0]));
                    break;
                default:
                    break;
            }
        });
        return resultList;
    }
}
