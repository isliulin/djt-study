package com.djt.test.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.util.StrUtil;
import com.djt.utils.DjtConstant;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-07
 */
public class OtherTest {

    @Test
    public void test1() {
        int size = 30;
        LocalDate date = LocalDate.now();
        for (int i = 0; i < size; i++) {
            LocalDate dateTmp = date.minusDays(i);
            int num = Integer.parseInt(dateTmp.format(DjtConstant.YMD));
            System.out.println(num + "======" + num % size);
        }
    }

    @Test
    public void test2() {
        List<String> list = new ArrayList<>();
        list.add("test-1");
        list.add("test-3");
        list.add("test-11");
        list.add("test-24");
        System.out.println("排序前：" + list);
        Collections.sort(list);
        System.out.println("排序后：" + list);
    }

    @Test
    public void test3() {
        YearMonth ym = YearMonth.parse("202107", DjtConstant.YM);
        System.out.println(ym.format(DjtConstant.YM));

        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("MMdd");
        LocalDate date = LocalDate.parse("20210706", DjtConstant.YMD);
        System.out.println(date.format(DjtConstant.YM));
        System.out.println(date.format(DjtConstant.YMD));
        System.out.println(date.format(formatter1));
    }

    @Test
    public void test4() {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 101; i++) {
            list.add(i);
        }

        int batchSize = 9;
        int batchNo = 0;
        List<Integer> tmpList = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            tmpList.add(list.get(i));
            if (tmpList.size() >= batchSize || i == list.size() - 1) {
                ++batchNo;
                System.out.println(StrUtil.format("批次：{} 数据：{}", batchNo, tmpList));
                tmpList.clear();
            }
        }
    }

    @Test
    public void test5() {
        LocalDateTime start = LocalDateTime.parse("1970-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);
        LocalDateTime end = LocalDateTime.parse("1970-01-02 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            LocalDateTime now = start.plus(i * 21L, ChronoUnit.MILLIS);
            if (now.isAfter(end)) {
                System.out.println(StrUtil.format("计时结束:{} date_time:{}",
                        i, now.format(DatePattern.NORM_DATETIME_FORMATTER)));
                break;
            }
        }
    }

    @Test
    public void test6() {
        LocalDateTime start = LocalDateTime.parse("1970-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);
        LocalDateTime end = LocalDateTime.parse("1970-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);
        System.out.println(start.isBefore(end));
    }

    @Test
    public void test7() {
        System.out.println(null instanceof String);
    }

    @Test
    public void test8() {
        System.out.println(Long.MAX_VALUE);
    }
}
