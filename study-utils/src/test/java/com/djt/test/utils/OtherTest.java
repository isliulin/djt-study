package com.djt.test.utils;

import com.djt.utils.DjtConstant;
import org.junit.Test;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-07
 */
public class OtherTest {

    @Test
    public void test1() {
        int size = 30;
        LocalDate date = LocalDate.now();
        for (int i = 0; i < size; i++) {
            LocalDate dateTmp = date.minusDays(i);
            int num = Integer.parseInt(dateTmp.format(DjtConstant.YMD));
            System.out.println(num + "======" + num % size);
        }
    }

    @Test
    public void test2() {
        List<String> list = new ArrayList<>();
        list.add("test-1");
        list.add("test-3");
        list.add("test-11");
        list.add("test-24");
        System.out.println("排序前：" + list);
        Collections.sort(list);
        System.out.println("排序后：" + list);
    }

    @Test
    public void test3() {
        YearMonth ym = YearMonth.parse("202107", DjtConstant.YM);
        System.out.println(ym.format(DjtConstant.YM));

        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("MMdd");
        LocalDate date = LocalDate.parse("20210706", DjtConstant.YMD);
        System.out.println(date.format(DjtConstant.YM));
        System.out.println(date.format(DjtConstant.YMD));
        System.out.println(date.format(formatter1));
    }


}
