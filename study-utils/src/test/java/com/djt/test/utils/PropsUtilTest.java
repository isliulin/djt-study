package com.djt.test.utils;

import cn.hutool.setting.Setting;
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

    @Test
    public void testSetting() {
        Setting setting = new Setting("config.properties");
        //必须带上分组 否则找不到
        System.out.println(setting.get("a"));
        System.out.println(setting.get("test1", "b"));
        System.out.println(setting.getByGroup("c", "test2"));
        System.out.println(setting.getSetting("test2"));
        System.out.println(setting.getProperties("test2"));
        System.out.println(setting.getProperties("test3"));

        //无分组才可直接获取
        setting.set("f", "6");
        System.out.println(setting.get("f"));
    }

    @Test
    public void testPropsUtil2() {
        Props props = Props.getProp("log4j2.xml", StandardCharsets.UTF_8);
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }
}
