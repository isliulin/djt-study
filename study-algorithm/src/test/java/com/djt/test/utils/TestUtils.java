package com.djt.test.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * 测试工具类
 *
 * @author 　djt317@qq.com
 * @date 　  2021-01-27 20:02
 */
public class TestUtils {


    public final static DateTimeFormatter YMDHMS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public final static DateTimeFormatter YMD = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final static ZoneId zoneId = ZoneId.systemDefault();
    private final static ZoneOffset zoneOffset = OffsetDateTime.now().getOffset();


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
        return getRandomDate(start, end, YMDHMS_FORMAT);
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
        long startTmp = LocalDateTime.of(LocalDate.parse(start, YMD), LocalTime.of(0, 0, 0)).toEpochSecond(zoneOffset);
        long endTmp = LocalDateTime.of(LocalDate.parse(end, YMD), LocalTime.of(23, 59, 59)).toEpochSecond(zoneOffset);
        long diff = getRandomNumber(startTmp, endTmp);
        Instant instant = Instant.ofEpochSecond(diff);
        return LocalDateTime.ofInstant(instant, zoneId).format(format);
    }
}
