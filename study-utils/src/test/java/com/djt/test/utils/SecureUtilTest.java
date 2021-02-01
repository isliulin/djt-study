package com.djt.test.utils;

import cn.hutool.core.util.HexUtil;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.DES;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;
import org.junit.Test;

import javax.crypto.SecretKey;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-01 19:27
 */
public class SecureUtilTest {

    /**
     * 较为流行
     * 用于替代DES
     */
    @Test
    public void testAES() {
        String pwd = "123456";
        String keyStr = "c7f5285a8c88ab48b17b98ef01389f29";
        SecretKey key = SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), HexUtil.decodeHex(keyStr));
        keyStr = HexUtil.encodeHexStr(key.getEncoded());
        System.out.println("秘钥:" + keyStr);
        AES aes = SecureUtil.aes(key.getEncoded());
        String en = aes.encryptHex(pwd);
        System.out.println("加密后:" + en);
        String de = aes.decryptStr(en);
        System.out.println("解密后:" + de);
    }

    @Test
    public void testDES() {
        String pwd = "123456";
        String keyStr = "ef2ce6a2ce515191";
        SecretKey key = SecureUtil.generateKey(SymmetricAlgorithm.DES.getValue(), HexUtil.decodeHex(keyStr));
        String kk = HexUtil.encodeHexStr(key.getEncoded());
        System.out.println(kk);
        DES des = SecureUtil.des(key.getEncoded());
        String en = des.encryptHex(pwd);
        System.out.println("加密后:" + en);
        String de = des.decryptStr(en);
        System.out.println("解密后:" + de);
    }

    @Test
    public void testPasswordUtils() {
        String en = PasswordUtils.encrypt("123456");
        System.out.println("加密后:" + en);
        String de = PasswordUtils.decrypt(en);
        System.out.println("解密后:" + de);
    }
}
