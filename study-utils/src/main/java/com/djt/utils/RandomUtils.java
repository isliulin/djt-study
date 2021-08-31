package com.djt.utils;

import cn.hutool.core.lang.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import static com.djt.utils.DjtConstant.SUR_NAME_ARR;

/**
 * 随机数工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-27
 */
public class RandomUtils {

    public static final Random RANDOM = new Random();

    /**
     * 生成指定长度的数字字符串 末尾补0
     *
     * @param min    最小值
     * @param max    最大值
     * @param length 长度
     * @return xxx
     */
    public static String getString(long min, long max, int length) {
        String baseNum = String.valueOf(getRandomNumber(min, max));
        return StringUtils.rightPad(baseNum, length, "0");
    }

    /**
     * 生成随机数
     *
     * @param min 最小值
     * @param max 最大值
     * @return long型
     */
    public static long getRandomNumber(long min, long max) {
        return (long) (min + Math.random() * (max - min + 1));
    }

    /**
     * 生成随机数
     *
     * @param min 最小值
     * @param max 最大值
     * @return int型
     */
    public static int getRandomNumber(int min, int max) {
        return (int) (min + Math.random() * (max - min + 1));
    }

    /**
     * 生成随机日期
     *
     * @param start 起始日期
     * @param end   截止日期
     * @return 日期字符串yyyy-MM-dd HH:mm:ss
     */
    public static String getRandomDate(String start, String end) {
        String startYmd = start.replaceAll("[-/:\\s]", "").substring(0, 8);
        String endYmd = end.replaceAll("[-/:\\s]", "").substring(0, 8);
        return getRandomDate(startYmd, endYmd, DjtConstant.YMDHMS_FORMAT);
    }

    /**
     * 生成随机日期
     *
     * @param start  起始日期
     * @param end    截止日期
     * @param format 日期格式
     * @return 日期字符串
     */
    public static String getRandomDate(String start, String end, DateTimeFormatter format) {
        String startYmd = start.replaceAll("[-/:\\s]", "").substring(0, 8);
        String endYmd = end.replaceAll("[-/:\\s]", "").substring(0, 8);
        long startTmp = LocalDateTime.of(LocalDate.parse(startYmd, DjtConstant.YMD), LocalTime.of(0, 0, 0)).toEpochSecond(DjtConstant.ZONE_OFFSET);
        long endTmp = LocalDateTime.of(LocalDate.parse(endYmd, DjtConstant.YMD), LocalTime.of(23, 59, 59)).toEpochSecond(DjtConstant.ZONE_OFFSET);
        long diff = getRandomNumber(startTmp, endTmp);
        Instant instant = Instant.ofEpochSecond(diff);
        return LocalDateTime.ofInstant(instant, DjtConstant.ZONE_ID).format(format);
    }

    /**
     * 生成随机姓名
     *
     * @return name
     */
    public static String getRandomName() {
        return getRandomName((byte) (RANDOM.nextInt(2)));
    }

    /**
     * 生成随机姓名
     *
     * @param sex 性别 0-男 1-女
     * @return name
     */
    public static String getRandomName(byte sex) {
        Validate.isTrue(sex == 0 || sex == 1, "性别不合法！");
        //随机取个姓
        String surName = SUR_NAME_ARR[RANDOM.nextInt(SUR_NAME_ARR.length)];
        //根据性别取名字列表
        String[] nameArr = sex == 0 ? DjtConstant.NAME_MALE_ARR : DjtConstant.NAME_FEMALE_ARR;
        //随机名字长度 1-2
        int nameLen = getRandomNumber(1, 2);
        String char1 = nameArr[RANDOM.nextInt(nameArr.length)];
        String char2 = "";
        if (nameLen > 1) {
            char2 = nameArr[RANDOM.nextInt(nameArr.length)];
        }
        String name = char1 + char2;
        //姓+名
        return surName + name;
    }

    /**
     * 生成UUID
     *
     * @return String
     */
    public static String getUuid() {
        return UUID.randomUUID().toString(true);
    }

}
