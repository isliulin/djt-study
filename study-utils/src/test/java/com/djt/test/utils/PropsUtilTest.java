package com.djt.test.utils;

import cn.hutool.setting.dialect.Props;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 配置文件工具
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-09
 */
public class PropsUtilTest {

    @Test
    public void testPropsUtil() {
        Props props = Props.getProp("log4j2.xml", StandardCharsets.UTF_8);
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }
}
