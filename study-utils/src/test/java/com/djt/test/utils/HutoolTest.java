package com.djt.test.utils;

import cn.hutool.Hutool;
import cn.hutool.core.comparator.CompareUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.net.NetUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.system.HostInfo;
import com.alibaba.fastjson.annotation.JSONField;
import com.djt.test.bean.PayOrder;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-02
 */
public class HutoolTest {

    @Test
    public void test() {
        Hutool.printAllUtils();
    }

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


        for (int i = 0; i < 10; i++) {
            uuid = UUID.randomUUID();
            //System.out.println(Math.abs(uuid.hashCode()));
            //System.out.println(uuid.variant());
            //System.out.println(uuid.getLeastSignificantBits());
            System.out.println(Math.abs(uuid.getMostSignificantBits()));
        }
    }

    @Test
    public void testHostInfo() {
        HostInfo hostInfo = new HostInfo();
        System.out.println(hostInfo.toString());
    }

    @Test
    public void testNetUtil() {
        System.out.println(NetUtil.getIpByHost("www.baidu.com"));
        System.out.println(NetUtil.getLocalHostName());
        System.out.println(NetUtil.getLocalhostStr());
        System.out.println(NetUtil.isUsableLocalPort(8080));
        System.out.println(NetUtil.getLocalMacAddress());
        System.out.println(NetUtil.localIpv4s());
        System.out.println(NetUtil.ping("www.baidu.com"));
        System.out.println(NetUtil.isOpen(new InetSocketAddress("172.20.20.183", 2181), 1000));
    }

    @Test
    public void testStrUtil2() {
        String str = "a, b ,c  ,  d,";
        System.out.println(Arrays.toString(StrUtil.splitToArray(str, ",")));
        System.out.println(Arrays.toString(StrUtil.splitTrim(str, ",").toArray(new String[0])));
    }

    @Test
    public void testArrayUtil() {
        String[] a = {"a", "b", "c"};
        String[] b = {"c", "d", "e", "f"};
        System.out.println(ArrayUtil.containsAny(a, b));
        System.out.println(ArrayUtil.containsAny(b, a));
    }

    @Test
    public void testJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.set("A", "1");
        jsonObject.set("B", "2");
        jsonObject.set("C", "3");
        System.out.println(jsonObject.toString());
        System.out.println(jsonObject.toJSONString(2));
    }

    @Test
    public void testDate1() {
        LocalDate start = LocalDate.parse("20220101", DatePattern.PURE_DATE_FORMATTER);
        LocalDate end = LocalDate.parse("20220102", DatePattern.PURE_DATE_FORMATTER);
        Period period = LocalDateTimeUtil.betweenPeriod(start, end);
        System.out.println(period.getDays());
    }

    @Test
    public void testDate2() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssZ");
        System.out.println(ZonedDateTime.now().format(formatter));
    }

    @Test
    public void testCompareUtil() {
        Long a = null;
        Long b = null;
        System.out.println(CompareUtil.compare(a, b));
    }


}
