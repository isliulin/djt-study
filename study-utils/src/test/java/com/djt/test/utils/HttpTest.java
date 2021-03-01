package com.djt.test.utils;

import cn.hutool.http.HttpUtil;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
public class HttpTest {

    @Test
    public void testIp() throws UnknownHostException {
        System.out.println(InetAddress.getLocalHost().getHostName());
        System.out.println(InetAddress.getLocalHost().getHostAddress());
        System.out.println(Arrays.toString(InetAddress.getLocalHost().getAddress()));
    }

    @Test
    public void testHttpUtil() {
        String content = HttpUtil.get("https://www.baidu.com");
        System.out.println(content);
    }

    @Test
    public void testByte() {
        byte a = 20;
        System.out.println(a);
    }
}
