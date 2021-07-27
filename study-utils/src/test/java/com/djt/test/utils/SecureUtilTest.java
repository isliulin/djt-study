package com.djt.test.utils;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.util.HexUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.RSA;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.DES;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;
import com.djt.utils.PasswordUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.security.KeyPair;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-01
 */
@Slf4j
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
    public void testRSA() {
        String pwd = "123456";
        KeyPair pair = SecureUtil.generateKeyPair("RSA");
        String pviKey = Base64.encode(pair.getPrivate().getEncoded());
        String pubKey = Base64.encode(pair.getPublic().getEncoded());
        System.out.println("私钥：" + pviKey);
        System.out.println("公钥：" + pubKey);

        RSA rsa = SecureUtil.rsa(pviKey, pubKey);
        String en = rsa.encryptBase64(StrUtil.utf8Bytes(pwd), KeyType.PrivateKey);
        System.out.println("加密后:" + en);
        String de = rsa.decryptStr(en, KeyType.PublicKey);
        System.out.println("解密后:" + de);
    }

    @Test
    public void testPasswordUtils() {
        String en = PasswordUtils.encrypt("123456");
        System.out.println("加密后:" + en);
        String de = PasswordUtils.decrypt(en);
        System.out.println("解密后:" + de);
    }

    @Test
    public void testDigestUtils() {
        String str = "123456";
        String md5 = DigestUtils.md5Hex(str);
        System.out.println(md5);

        str = "123456";
        md5 = DigestUtils.md5Hex(str);
        System.out.println(md5);

        str = "20210727张三";
        md5 = DigestUtils.md5Hex(str);
        System.out.println(md5);

        str = "20210727李四";
        md5 = DigestUtils.md5Hex(str);
        System.out.println(md5);

        str = "20210727王五";
        md5 = DigestUtils.md5Hex(str);
        System.out.println(md5);
    }


}
