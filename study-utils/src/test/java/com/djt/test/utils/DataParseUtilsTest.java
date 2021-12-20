package com.djt.test.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.djt.utils.DataParseUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-04
 */
public class DataParseUtilsTest {

    @Test
    public void test1() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("A", "123");
        System.out.println(DataParseUtils.getNumFromJson(jsonObject, "A"));
        jsonObject.put("B", 456);
        System.out.println(DataParseUtils.getNumFromJson(jsonObject, "B"));
    }

    @Test
    public void test2() {
        DateTimeFormatter formatter = DatePattern.NORM_DATETIME_MS_FORMATTER;
        long dayMillis = 86400000;
        LocalDateTime endToday = LocalDateTimeUtil.endOfDay(LocalDateTime.now());
        long m1 = LocalDateTimeUtil.toEpochMilli(endToday);
        System.out.println(StrUtil.format("endToday={}", endToday.format(formatter)));
        LocalDateTime endYesterday = LocalDateTimeUtil.of(m1 - dayMillis);
        System.out.println(StrUtil.format("endYesterday={}", endYesterday.format(formatter)));
        LocalDateTime beginYesterday = LocalDateTimeUtil.of(m1 - dayMillis - dayMillis + 1);
        System.out.println(StrUtil.format("beginYesterday={}", beginYesterday.format(formatter)));

        System.out.println(Duration.between(endYesterday, endToday).toMillis());
    }

}
