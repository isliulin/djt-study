package com.djt.test.utils;

import cn.binarywang.tools.generator.ChineseMobileNumberGenerator;
import cn.binarywang.tools.generator.ChineseNameGenerator;
import cn.binarywang.tools.generator.bank.BankCardNumberGenerator;
import cn.binarywang.tools.generator.bank.BankCardTypeEnum;
import cn.binarywang.tools.generator.bank.BankNameEnum;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * 测试 造数据工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-11
 */
public class GeneratorTest {

    @Test
    public void testGenerate() {
        String generatedName = ChineseNameGenerator.getInstance().generate();
        assertNotNull(generatedName);
        System.err.println(generatedName);
    }

    @Test
    public void testGenerateOdd() {
        String generatedName = ChineseNameGenerator.getInstance().generateOdd();
        assertNotNull(generatedName);
        System.err.println(generatedName);
    }

    @Test
    public void testGenerate_by_bankName() {
        String bankCardNo = BankCardNumberGenerator.generate(BankNameEnum.CR, null);
        System.err.println(bankCardNo);
        assertNotNull(bankCardNo);

        bankCardNo = BankCardNumberGenerator.generate(BankNameEnum.ICBC, BankCardTypeEnum.CREDIT);
        System.err.println(bankCardNo);
        assertNotNull(bankCardNo);

        bankCardNo = BankCardNumberGenerator.generate(BankNameEnum.ICBC, BankCardTypeEnum.DEBIT);
        System.err.println(bankCardNo);
        assertNotNull(bankCardNo);
    }

    @Test
    public void testGenerateByPrefix() {
        String bankCardNo = BankCardNumberGenerator.generateByPrefix(436742);
        System.err.println(bankCardNo);
        assertNotNull(bankCardNo);
    }

    @Test
    public void testGenerateBankCardNumber() {
        String bankCardNo = BankCardNumberGenerator.getInstance().generate();
        System.err.println(bankCardNo);
        assertNotNull(bankCardNo);
    }

    @Test
    public void testGenerateBankCardNumber2() {
        for (int i = 0; i < 10; i++) {
            String bankCardNo = BankCardNumberGenerator.generate(BankNameEnum.ICBC, BankCardTypeEnum.CREDIT);
            System.err.println(bankCardNo);
        }
    }

    @Test
    public void testChineseMobileNumberGenerator() {
        for (int i = 0; i < 10; i++) {
            String no = ChineseMobileNumberGenerator.getInstance().generate();
            System.err.println(no);
        }
    }

}
