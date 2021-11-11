package com.djt.utils;

import cn.hutool.setting.dialect.Props;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 配置测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-27
 */
public class ConfigTest {

    @Test
    public void testConfig() {
        Props props = ConfigConstants.PROPS;
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }

    @Test
    public void testMap() {
        Map<String, Tuple2<String, String>> map = new HashMap<>();
        map.put("A", Tuple2.of("1", "2"));
        map.put("B", Tuple2.of("3", "4"));
        map.put("C", Tuple2.of("5", "6"));
        System.out.println(map);
        map.forEach((k, v) -> {
            v.f0 = "6";
            v.f1 = "6";
        });
        System.out.println(map);
    }
}
