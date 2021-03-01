package com.djt.test.utils;

import com.alibaba.fastjson.JSONObject;
import com.djt.utils.DataParseUtils;
import org.junit.Test;

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

}
