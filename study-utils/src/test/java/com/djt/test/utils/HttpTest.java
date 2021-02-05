package com.djt.test.utils;

import cn.hutool.http.HttpUtil;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-01 17:56
 */
public class HttpTest {

    @Test
    public void testHttpUtil() {
        String content = HttpUtil.get("https://www.baidu.com");
        System.out.println(content);

    }
}
