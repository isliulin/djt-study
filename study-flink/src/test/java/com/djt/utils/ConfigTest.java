package com.djt.utils;

import cn.hutool.setting.dialect.Props;
import org.junit.Test;

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
}
