package com.djt.test.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.StrUtil;
import org.junit.Test;

import java.time.LocalDateTime;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-02
 */
public class HutoolTest {

    @Test
    public void testStrUtil() {
        String sql = StrUtil.format("ALTER TABLE {} ADD PARTITION P{} VALUES ({})", "xdata.t_test", "20210101", "20210101");
        System.out.println(sql);
    }

    @Test
    public void testDateUtils() {
        LocalDateTime dateTime = LocalDateTimeUtil.of(System.currentTimeMillis());
        String str = LocalDateTimeUtil.format(dateTime, DatePattern.NORM_DATETIME_FORMATTER);
        System.out.println(str);
    }

}
