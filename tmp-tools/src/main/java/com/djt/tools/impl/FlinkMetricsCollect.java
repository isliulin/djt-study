package com.djt.tools.impl;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.entity.FlinkJobEntity;
import com.djt.entity.MetricEntity;
import com.djt.entity.MetricType;
import com.djt.tools.AbsTools;
import com.djt.utils.EsUtils;
import com.google.common.collect.HashMultimap;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Flink监控指标采集
 * Http请求写入ES
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-07
 */
@Log4j2
public class FlinkMetricsCollect extends AbsTools {

    /**
     * ES 索引名
     */
    private static final String ES_INDEX_NAME = "t_flink_metric_log_";

    /**
     * ES 索引类型
     */
    private static final String ES_INDEX_TYPE = "_doc";

    /**
     * 基础URL
     */
    public static final String BASE_URL = "http://flink.jlpay.io/static";

    /**
     * 任务信息URL
     */
    public static final String JOB_URL = BASE_URL + "/jobs/00000000000000000000000000000000";

    /**
     * TM信息URL
     */
    public static final String TM_URL = BASE_URL + "/taskmanagers";

    /**
     * 变量名前缀映射
     */
    private static final Map<String, String> PREFIX_MAP = new HashMap<>();

    /**
     * 算子监控指标
     */
    private static final List<String> OP_METRIC_KEYS = new ArrayList<>();

    /**
     * tm监控指标
     */
    private static final List<String> TM_METRIC_KEYS = new ArrayList<>();

    /**
     * 单次请求最大批次
     */
    private static final int REQUEST_BATCH_SIZE = 100;

    /**
     * Flink任务信息
     */
    private final FlinkJobEntity flinkJob;

    static {
        PREFIX_MAP.put("Source__KafkaSource", "Source: KafkaSource");
        PREFIX_MAP.put("Sink__addSink_Kafka", "Sink: addSink_Kafka");

        OP_METRIC_KEYS.add("*.Source__KafkaSource.numRecordsOutPerSecond");
        OP_METRIC_KEYS.add("*.Source__KafkaSource.user.kafkaRecordsInToday");
        OP_METRIC_KEYS.add("*.Source__KafkaSource.user.kafkaRecordsInYesterday");
        OP_METRIC_KEYS.add("*.Sink__addSink_Kafka.user.statDelayMillis");

        TM_METRIC_KEYS.add("Status.JVM.Memory.Heap.Used");
    }

    public FlinkMetricsCollect() {
        this.flinkJob = getFlinkJob();
    }

    /**
     * 定时执行获取监控数据
     *
     * @param args 参数类表
     */
    @Override
    public void doExecute(String[] args) {
        long delay = PROPS.getLong("flink.delay.seconds", 10L);
        HashMultimap<String, String> opMetrics = getOpMetrics();
        HashMultimap<String, String> tmMetrics = getTmMetrics();
        RestHighLevelClient esClient = EsUtils.getRestHighLevelClient(PROPS);
        ScheduledThreadPoolExecutor executor = ThreadUtil.createScheduledExecutor(1);
        ScheduledFuture<?> future = executor.scheduleWithFixedDelay(() -> {
            try {
                String indexSuffix = LocalDate.now().format(DatePattern.PURE_DATE_FORMATTER);
                List<MetricEntity> resultList = new ArrayList<>();
                resultList.addAll(queryMetrics(opMetrics, MetricType.OPERATOR));
                resultList.addAll(queryMetrics(tmMetrics, MetricType.TASKMANAGER));
                String esIndex = ES_INDEX_NAME + indexSuffix;
                EsUtils.upsert(esClient, esIndex, ES_INDEX_TYPE, RequestOptions.DEFAULT, resultList);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 0, delay, TimeUnit.SECONDS);

        try {
            future.get();
        } catch (Exception e) {
            throw new RuntimeException("监控数据采集异常: ", e);
        }
    }

    /**
     * 获取算子所有分区监控指标
     *
     * @return HashMultimap<String, String>
     */
    private HashMultimap<String, String> getOpMetrics() {
        HashMultimap<String, String> metrics = HashMultimap.create();
        OP_METRIC_KEYS.forEach(key -> {
            String[] strArr = StringUtils.split(key, '.');
            FlinkJobEntity.Vertex vertex = flinkJob.getVertices().get(PREFIX_MAP.get(strArr[1]));
            if (vertex == null) {
                return;
            }
            for (int i = 0; i < vertex.getParallelism(); i++) {
                strArr[0] = String.valueOf(i);
                metrics.put(vertex.getId(), StringUtils.join(strArr, '.'));
            }
        });
        return metrics;
    }

    /**
     * 获取所有TM监控指标
     *
     * @return HashMultimap<String, String>
     */
    private HashMultimap<String, String> getTmMetrics() {
        HashMultimap<String, String> metrics = HashMultimap.create();
        Set<String> tmIds = flinkJob.getTaskmanagers().keySet();
        TM_METRIC_KEYS.forEach(key -> tmIds.forEach(tm -> metrics.put(tm, key)));
        return metrics;
    }


    /**
     * 获取Flink任务整体信息
     *
     * @return FlinkJobEntity
     */
    private static FlinkJobEntity getFlinkJob() {
        String vts = "vertices";
        String tms = "taskmanagers";
        JSONObject jobEntity = JSON.parseObject(HttpUtil.get(JOB_URL));
        JSONObject tmEntity = JSON.parseObject(HttpUtil.get(TM_URL));

        JSONObject vtJson = new JSONObject();
        jobEntity.getJSONArray(vts).forEach(json -> vtJson.put(((JSONObject) json).getString("name"), json));
        jobEntity.put(vts, vtJson);

        JSONObject tmJson = new JSONObject();
        tmEntity.getJSONArray(tms).forEach(json -> tmJson.put(((JSONObject) json).getString("id"), json));
        jobEntity.put(tms, tmJson);

        return JSON.toJavaObject(jobEntity, FlinkJobEntity.class);
    }

    /**
     * 获取监控结果
     *
     * @param metricMap  监控指标
     * @param metricType 监控类型
     * @return List<MetricEntity>
     */
    private List<MetricEntity> queryMetrics(HashMultimap<String, String> metricMap, MetricType metricType) {
        List<MetricEntity> metricEntityList = new ArrayList<>();
        for (String id : metricMap.keySet()) {
            List<String> tmpList = new ArrayList<>();
            Iterator<String> iter = metricMap.get(id).iterator();
            while (iter.hasNext()) {
                tmpList.add(iter.next());
                if (tmpList.size() >= REQUEST_BATCH_SIZE || !iter.hasNext()) {
                    String metrics = StringUtils.join(tmpList, ',');
                    metricEntityList.addAll(metricType.queryMetrics(id, metrics));
                    tmpList.clear();
                }
            }
        }
        return metricEntityList;
    }

}
