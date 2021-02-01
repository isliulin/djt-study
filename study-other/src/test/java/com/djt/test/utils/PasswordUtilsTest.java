package com.djt.test.utils;

import com.alibaba.druid.filter.config.ConfigTools;
import com.alibaba.druid.util.Base64;
import com.djt.utils.PasswordUtils;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-01-28 19:16
 */
public class PasswordUtilsTest {

    private static final String PRI_KEY = "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEBwrYV4xVuvH3D+u9fwnTXnoyFaXqz1EyZW83uBKRssWqlFd8AZWL54cSCx8aTEaMWr/ztj0fcmnFuF4GRdzUQXwIDAQABAkAtUFtA+n2oq8hRaVGqXk7zXNNo+HIuDQl6JmkgxFs9NuGNj6Nt9u2uh5vjmLmH8EQCB/S7XY5E+I++Jg46BjuZAiEBw4W4GUUJ5EUEiQhGAKT7AXwJfs63EqPcQ9iGn8RcEnsCIQD/ikccrkNDUvUcFfbfj1wgxP07kiAhZmg5QR/+tg32bQIgHXsCnSvDzJKxDB1tLKeY9+zYVd47V805GXjuuUnB1TcCIQC7NwVuX8Vrt4VX9EeP9ina4Ddew2nCzpIhEDNumtaT6QIgdK9HwXajtDImXt3ZPDC4ecTSkxFKUCqgcmELLRVnKgA=";
    private static final String PUB_KEY = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAcK2FeMVbrx9w/rvX8J0156MhWl6s9RMmVvN7gSkbLFqpRXfAGVi+eHEgsfGkxGjFq/87Y9H3JpxbheBkXc1EF8CAwEAAQ==";

    @Test
    public void testPwd() throws Exception {
        String password = "123456";
        String encryptPwd = ConfigTools.encrypt(PRI_KEY, password);
        System.out.println("加密后:" + encryptPwd);
        String decryptPwd = ConfigTools.decrypt(PUB_KEY, encryptPwd);
        System.out.println("解密后:" + decryptPwd);
    }

    @Test
    public void testPwd2() {
        String password = "xdata_edw";  //密码明文
        String encryptPwd = PasswordUtils.encrypt(PRI_KEY, password);
        System.out.println("加密后:" + encryptPwd);
        String decryptPwd = PasswordUtils.decrypt(PUB_KEY, encryptPwd);
        System.out.println("解密后:" + decryptPwd);

        encryptPwd = PasswordUtils.encrypt(password);
        System.out.println("加密后(默认秘钥):" + encryptPwd);
        decryptPwd = PasswordUtils.decrypt(encryptPwd);
        System.out.println("解密后(默认秘钥):" + decryptPwd);
    }


    @Test
    public void testHex() throws Exception {
        String password = "123ABC";  //密码明文
        String hex = Hex.encodeHexString(password.getBytes(StandardCharsets.UTF_8));
        System.out.println("编码后：" + hex);
        String pwd = new String(Hex.decodeHex(hex.toCharArray()));
        System.out.println("解码后：" + pwd);
    }

    @Test
    public void testBase64() {
        String ss = "6e63627a2437346c";
        String str = Base64.byteArrayToBase64(ss.getBytes(StandardCharsets.UTF_8));
        System.out.println(str);

        String df = new String(Base64.base64ToByteArray(ConfigTools.DEFAULT_PUBLIC_KEY_STRING), StandardCharsets.UTF_8);
        System.out.println(df);
    }

    @Test
    public void testKey() {
        String[] keyPair = com.djt.utils.PasswordUtils.genKeyPair(513);
        System.out.println("privateKey:" + keyPair[0]);
        System.out.println("publicKey:" + keyPair[1]);
    }


}
