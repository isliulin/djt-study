package com.djt.window;

import cn.hutool.core.comparator.CompareUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Scanner;

/**
 * @author 　djt317@qq.com
 * @since 　 2022-04-25
 */
public class WindowTest {

    @Test
    public void testDynamicTimeWindow() {
        QueueTimeWindow<Integer> window = new QueueTimeWindow<>(5000, 2000);
        long tms = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            System.out.println("==============================");
            tms += 1000;
            addAndPrint(window, tms, i);
            ThreadUtil.sleep(1000);
        }

        System.out.println("==============================");
        window.clear();
        System.out.println(window);
    }

    @Test
    public void testDynamicTimeWindow2() {
        QueueTimeWindow<Integer> window = new QueueTimeWindow<>(5000, 2000);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("==============================");
            String inputStr = scanner.nextLine();
            if (StringUtils.isBlank(inputStr)) {
                System.err.println("无效输入!");
                continue;
            }
            if ("exit".equalsIgnoreCase(inputStr)) {
                System.out.println("程序结束.");
                return;
            }
            String[] strArr = StringUtils.split(inputStr, ',');
            if (ArrayUtils.isEmpty(strArr) || strArr.length != 2) {
                System.err.println("无效输入!");
                continue;
            }
            LocalDateTime dt;
            long tms;
            int v;
            try {
                dt = LocalDateTime.parse(strArr[0].trim(), DatePattern.NORM_DATETIME_FORMATTER);
                tms = LocalDateTimeUtil.toEpochMilli(dt);
                v = Integer.parseInt(strArr[1].trim());
            } catch (Exception e) {
                System.err.println("无效输入!");
                continue;
            }
            addAndPrint(window, tms, v);
            ThreadUtil.sleep(1000);
        }
    }

    /**
     * 添加并打印数据
     *
     * @param window    window
     * @param timestamp timestamp
     * @param v         v
     * @param <V>       V
     */
    private <V extends Serializable> void addAndPrint(QueueTimeWindow<V> window, long timestamp, V v) {
        String dt = LocalDateTimeUtil.of(timestamp).format(DatePattern.NORM_DATETIME_FORMATTER);
        if (window.add(timestamp, v)) {
            System.out.println("数据迟到:" + dt + ":" + v);
        }
        System.out.println("当前队列:" + window);
        System.out.println("当前数据:" + window.getResult());
    }

    @Test
    public void testSerialize() {
        QueueTimeWindow<Integer> window = new QueueTimeWindow<>(5000, 2000);
        window.add(System.currentTimeMillis(), 1);
        System.out.println(window);
        byte[] windowBytes = ObjectUtil.serialize(window);
        window = ObjectUtil.deserialize(windowBytes);
        System.out.println(window);
    }

    @Test
    public void testTupleComparator() {
        TupleComparator<Tuple1<Integer>> tuple1TupleComparator = new TupleComparator<>(0);
        System.out.println(tuple1TupleComparator.compare(Tuple1.of(1), Tuple1.of(2)));

        TupleComparator<Tuple2<Integer, Integer>> tuple2TupleComparator = new TupleComparator<>(0);
        System.out.println(tuple2TupleComparator.compare(Tuple2.of(1, 2), Tuple2.of(2, 3)));

        tuple2TupleComparator = new TupleComparator<>(2);
        try {
            System.out.println(tuple2TupleComparator.compare(Tuple2.of(1, 2), Tuple2.of(2, 3)));
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        TupleComparator<Tuple3<JSONObject, Integer, Integer>> tuple3TupleComparator = new TupleComparator<>(0);
        try {
            System.out.println(tuple3TupleComparator.compare(Tuple3.of(new JSONObject(), 1, 2), Tuple3.of(new JSONObject(), 2, 3)));
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        Comparator<JSONObject> comparator = (o1, o2) -> CompareUtil.compare(o1.getInteger("a"), o2.getInteger("a"));
        tuple3TupleComparator = new TupleComparator<>(0, comparator);
        System.out.println(tuple3TupleComparator.compare(Tuple3.of(JSON.parseObject("{\"a\":1}"), 1, 2),
                Tuple3.of(JSON.parseObject("{\"a\":2}"), 2, 3)));

    }

}
