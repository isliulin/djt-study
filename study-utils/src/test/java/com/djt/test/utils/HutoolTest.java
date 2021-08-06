package com.djt.test.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.annotation.JSONField;
import com.djt.test.bean.PayOrder;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

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
        long curTs = System.currentTimeMillis();
        System.out.println("当前时间戳:" + curTs);
        //时间戳转日期
        LocalDateTime dateTime = LocalDateTimeUtil.of(curTs);
        String str = LocalDateTimeUtil.format(dateTime, DatePattern.NORM_DATETIME_FORMATTER);
        System.out.println("转换为日期:" + str);
        //日期转时间戳
        long timestamp = LocalDateTimeUtil.toEpochMilli(dateTime);
        System.out.println("转换为时间戳:" + timestamp);
    }

    @Test
    public void testDateUtils2() {
        long timeStamp = System.currentTimeMillis();
        LocalDateTime dateTime = LocalDateTimeUtil.of(timeStamp);
        long ms = dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        long ms2 = dateTime.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli();
        System.out.println(timeStamp);
        System.out.println(ms);
        System.out.println(ms2);
        System.out.println(ZoneOffset.of("+8"));
        System.out.println(OffsetDateTime.now().getOffset());
        System.out.println(ZoneOffset.systemDefault());
    }

    @Test
    public void testReflectUti() {
        List<String> fieldNames = new ArrayList<>();
        Field[] fields = ReflectUtil.getFields(PayOrder.class);
        for (Field field : fields) {
            JSONField jsonField = field.getAnnotation(JSONField.class);
            String fieldName = jsonField.name();
            fieldNames.add(fieldName);
        }
        System.out.println(fieldNames);
    }

    @Test
    public void testUUID() {
        UUID uuid = UUID.randomUUID();
        System.out.println(uuid.toString(false));
        System.out.println(uuid.toString(true));
    }

}
