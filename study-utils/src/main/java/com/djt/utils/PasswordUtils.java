package com.djt.utils;


import cn.hutool.core.util.HexUtil;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;

import javax.crypto.SecretKey;

/**
 * 密码工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-28
 */
public class PasswordUtils {

    /**
     * 秘钥
     */
    private static final String SECRET_KEY = "c7f5285a8c88ab48b17b98ef01389f29";

    /**
     * 密码加密
     *
     * @param password 明文
     * @return 密文
     */
    public static String encrypt(String password) {
        return encrypt(password, SECRET_KEY);
    }

    /**
     * 密码加密
     *
     * @param password  明文
     * @param secretKey 私钥
     * @return 密文
     */
    public static String encrypt(String password, String secretKey) {
        try {
            SecretKey key = SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), HexUtil.decodeHex(secretKey));
            AES aes = SecureUtil.aes(key.getEncoded());
            return aes.encryptHex(password);
        } catch (Exception e) {
            throw new RuntimeException("加密失败！", e);
        }
    }

    /**
     * 密码解密 使用默认公钥
     *
     * @param password 密文
     * @return 明文
     */
    public static String decrypt(String password) {
        return decrypt(password, SECRET_KEY);
    }

    /**
     * 密码解密
     *
     * @param password  密文
     * @param secretKey 公钥
     * @return 明文
     */
    public static String decrypt(String password, String secretKey) {
        try {
            SecretKey key = SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), HexUtil.decodeHex(secretKey));
            AES aes = SecureUtil.aes(key.getEncoded());
            return aes.decryptStr(password);
        } catch (Exception e) {
            throw new RuntimeException("解密失败！", e);
        }
    }

}
