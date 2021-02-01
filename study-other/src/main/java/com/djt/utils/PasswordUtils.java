package com.djt.utils;

import com.alibaba.druid.filter.config.ConfigTools;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

/**
 * 密码工具类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-01-28 20:00
 */
public class PasswordUtils {


    /**
     * 默认密码私钥 用于加密
     */
    private static final String DEFAULT_PRIVATE_KEY = "MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA0d58C/g8u9+jMQsH50XB4tLDmTaubkk+t/hj2g/NAHlVQ/vD2A7/L8SPP7pH1nCUdYd4pfe3KCBfJQXIu4aA7QIDAQABAkAZYAE3oUgWny+oGmFWQUT0G++ycr4cb5a5v7qy/v4WdF0hoXaLRW9rxfYb+8YmLFQd447HwD62xJ1euc6mc6KBAiEA6AYfA+e276m2Ooyiy0q37xzfLHN/MKJKwT+yi5U9LkkCIQDnjkmNxlh9liou5vr8zlKcfLjyRuuWcx48/xFq40XNhQIhAIJbDgnPoUO9AZibcsrsS7KXcfszWH4mcAFqnBE344uhAiBl9Eh+nC6qXUwFir5IQbAuJtxoEMH6ZIWT5dsNbTR24QIhAM5AUYLi7TYpN1CCeFNf5tyFDm7DZH6RzHxjg6a5hxu8";

    /**
     * 默认密码公钥 用于解密
     */
    public static final String DEFAULT_PUBLIC_KEY = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBANHefAv4PLvfozELB+dFweLSw5k2rm5JPrf4Y9oPzQB5VUP7w9gO/y/Ejz+6R9ZwlHWHeKX3tyggXyUFyLuGgO0CAwEAAQ==";

    /**
     * 密码加密
     *
     * @param password 明文
     * @return 密文
     */
    public static String encrypt(String password) {
        try {
            return ConfigTools.encrypt(DEFAULT_PRIVATE_KEY, password);
        } catch (Exception e) {
            throw new RuntimeException("加密失败！");
        }
    }

    /**
     * 密码加密
     *
     * @param password 明文
     * @param priKey   私钥
     * @return 密文
     */
    public static String encrypt(String priKey, String password) {
        try {
            return ConfigTools.encrypt(priKey, password);
        } catch (Exception e) {
            throw new RuntimeException("加密失败！");
        }
    }

    /**
     * 密码解密 使用默认公钥
     *
     * @param password 密文
     * @return 明文
     */
    public static String decrypt(String password) {
        return decrypt(DEFAULT_PUBLIC_KEY, password);
    }

    /**
     * 密码解密
     *
     * @param password 密文
     * @param pubKey   公钥
     * @return 明文
     */
    public static String decrypt(String pubKey, String password) {
        try {
            return ConfigTools.decrypt(pubKey, password);
        } catch (Exception e) {
            throw new RuntimeException("解密失败！");
        }
    }

    /**
     * 生成秘钥
     *
     * @param keySize 秘钥长度(至少512)
     * @return 0-私钥 1-公钥
     */
    public static String[] genKeyPair(int keySize) {
        try {
            return ConfigTools.genKeyPair(keySize);
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new IllegalArgumentException("生成秘钥失败", e);
        }
    }

}
