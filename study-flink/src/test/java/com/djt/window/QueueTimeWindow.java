package com.djt.window;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 基于优先级队列的时间窗口
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-22
 */
public class QueueTimeWindow<V extends Serializable> implements Serializable {

    /**
     * 数据保留最近的时长
     */
    private final long keepLatestMillis;

    /**
     * 数据最大无序时长
     */
    private final long maxOutOfOrderMillis;

    /**
     * 窗口最新时间
     */
    private long currentTimestamp = 0;

    /**
     * 窗口最大时间
     */
    private long maxTimestamp = 0;

    /**
     * 数据优先级队列(根据时间排序)
     */
    private final PriorityQueue<Tuple2<Long, V>> queue;

    public QueueTimeWindow(long keepLatestMillis, long maxOutOfOrderMillis) {
        Validate.isTrue(keepLatestMillis > 0, "keepLatestMillis 必须大于0！");
        Validate.isTrue(maxOutOfOrderMillis >= 0, "maxOutOfOrderMillis 不能为负数！");
        this.keepLatestMillis = keepLatestMillis;
        this.maxOutOfOrderMillis = maxOutOfOrderMillis;
        queue = new PriorityQueue<>(1, new TupleComparator<>(0));
    }

    /**
     * 添加数据
     *
     * @param timestamp 时间戳
     * @param value     数据
     * @return 是否是迟到数据
     */
    public boolean add(long timestamp, V value) {
        boolean isLate = false;
        //更新时间戳
        currentTimestamp = timestamp;
        if (timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
        }
        //检查是否迟到
        if (timestamp < maxTimestamp - maxOutOfOrderMillis + 1) {
            isLate = true;
        }
        //数据入队
        queue.offer(Tuple2.of(timestamp, value));
        //调整队列
        adjustQueue();
        return isLate;
    }

    /**
     * 获取最新数据结果
     * 取数范围为: [currentTimestamp - keepLatestMillis , currentTimestamp]
     *
     * @return List<V>
     */
    public List<V> getResult() {
        List<V> result = new ArrayList<>();
        if (queue.isEmpty()) {
            return result;
        }
        queue.iterator().forEachRemaining(v -> {
            if (v.f0 > currentTimestamp - keepLatestMillis && v.f0 <= currentTimestamp) {
                result.add(v.f1);
            }
        });
        return result;
    }

    /**
     * 调整队列 移除失效数据
     */
    private void adjustQueue() {
        while (true) {
            Tuple2<Long, V> data = queue.peek();
            //超出窗口阈值 则移除
            if (data != null && maxTimestamp - data.f0 >= keepLatestMillis + maxOutOfOrderMillis) {
                queue.poll();
            } else {
                break;
            }
        }
    }

    /**
     * 清空队列
     */
    public void clear() {
        queue.clear();
        currentTimestamp = 0;
        maxTimestamp = 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("keepLatestMillis=").append(keepLatestMillis).append(",");
        sb.append("maxOutOfOrderMillis=").append(maxOutOfOrderMillis).append(",");
        sb.append("maxTimestamp=").append(LocalDateTimeUtil.of(maxTimestamp)
                .format(DatePattern.NORM_DATETIME_FORMATTER)).append(",");
        sb.append("currentTimestamp=").append(LocalDateTimeUtil.of(currentTimestamp)
                .format(DatePattern.NORM_DATETIME_FORMATTER)).append(",");
        sb.append("queue=");
        if (queue == null || queue.isEmpty()) {
            return sb.append("[]").toString();
        }
        PriorityQueue<Tuple2<Long, V>> queueTmp = new PriorityQueue<>(queue);
        sb.append("[");
        while (!queueTmp.isEmpty()) {
            Tuple2<Long, V> tpv = queueTmp.poll();
            sb.append("(");
            sb.append(LocalDateTimeUtil.of(tpv.f0).format(DatePattern.NORM_DATETIME_FORMATTER));
            sb.append(",");
            sb.append(tpv.f1);
            sb.append(")");
            if (!queueTmp.isEmpty()) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }

}
