package com.djt.test.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-02 19:24
 */
public class StringTest {

    @Test
    public void testStringUtils() {
        String sql = StrUtil.format("ALTER TABLE {} ADD PARTITION P{} VALUES ({})", "xdata.t_test", "20210101", "20210101");
        System.out.println(sql);
        System.out.println(StringUtils.isNumeric("0123"));
        System.out.println(StringUtils.isNumeric("123x"));
        System.out.println(StringUtils.isNumeric("123 456"));
        System.out.println(StringUtils.isNumericSpace("123 456 "));
        System.out.println(StringUtils.isNumericSpace("123 456 x"));
    }

    @Test
    public void testJson() {
        //判断json是否合法
        String str = "{\"a\":\"1\"}";
        System.out.println(JSONValidator.fromUtf8(str.getBytes(StandardCharsets.UTF_8)).validate());
        System.out.println(JSONObject.isValid("666"));
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("A", "123");
        System.out.println("A:" + jsonObject.getLongValue("A"));
        jsonObject.put("B", "12 3");
        System.out.println("B:" + jsonObject.getLongValue("B"));
        jsonObject.put("C", "12 x");
        System.out.println("C:" + jsonObject.getLongValue("C"));
    }

    @Test
    public void testJson2() {
        System.out.println(JSONUtil.isJson("666"));
        System.out.println(JSONUtil.isJson("{}"));
        System.out.println(JSONUtil.isJson("{\"A\"}"));
        System.out.println(JSONUtil.isJsonObj("{}"));
        System.out.println(JSONUtil.isJsonObj("{\"A\"}"));
    }

    @Test
    public void testMap() {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("A", null);
        System.out.println(dataMap.get("A"));
        System.out.println(dataMap.getOrDefault("A", "").toString());
    }

    @Test
    public void testMOptional() {
        String str = null;
        Optional<String> optional = Optional.ofNullable(str);
        System.out.println(optional.orElse("666"));
        str = "xxx";
        optional = Optional.ofNullable(str);
        System.out.println(optional.orElse("777"));
    }

}
