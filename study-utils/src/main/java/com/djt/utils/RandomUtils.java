package com.djt.utils;

import org.apache.commons.lang3.StringUtils;

import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * 随机数工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-01-27
 */
public class RandomUtils {


    public final static DateTimeFormatter YMDHMS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public final static DateTimeFormatter YMD = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final static ZoneId ZONE_ID = ZoneId.systemDefault();
    private final static ZoneOffset ZONE_OFFSET = OffsetDateTime.now().getOffset();


    /**
     * 生成指定长度的数字字符串 末尾补0
     *
     * @param start  起始值
     * @param end    截止值
     * @param length 长度
     * @return xxx
     */
    public static String getString(long start, long end, int length) {
        String baseNum = String.valueOf(getRandomNumber(start, end));
        return StringUtils.rightPad(baseNum, length, "0");
    }

    /**
     * 生成随机数
     *
     * @param start 起始值
     * @param end   截止值
     * @return long型
     */
    public static long getRandomNumber(long start, long end) {
        return (long) (start + Math.random() * (end - start + 1));
    }

    /**
     * 生成随机数
     *
     * @param start 起始值
     * @param end   截止值
     * @return int型
     */
    public static int getRandomNumber(int start, int end) {
        return (int) (start + Math.random() * (end - start + 1));
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
        return getRandomDate(startYmd, endYmd, YMDHMS_FORMAT);
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
        long startTmp = LocalDateTime.of(LocalDate.parse(startYmd, YMD), LocalTime.of(0, 0, 0)).toEpochSecond(ZONE_OFFSET);
        long endTmp = LocalDateTime.of(LocalDate.parse(endYmd, YMD), LocalTime.of(23, 59, 59)).toEpochSecond(ZONE_OFFSET);
        long diff = getRandomNumber(startTmp, endTmp);
        Instant instant = Instant.ofEpochSecond(diff);
        return LocalDateTime.ofInstant(instant, ZONE_ID).format(format);
    }
}
