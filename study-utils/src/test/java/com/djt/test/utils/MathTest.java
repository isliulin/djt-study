package com.djt.test.utils;

import cn.hutool.core.math.Calculator;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-09-04
 */
public class MathTest {

    /**
     * 判断数A是否是数B的倍数 且允许存在误差
     *
     * @param a 数A
     * @param b 数B
     * @param e 误差
     * @return boolean
     */
    public static boolean isMultiple(long a, long b, long e) {
        if (a < b) {
            return a > (b - e);
        }
        long m = (a / b) * b;
        return (a >= m && a <= (m + e)) || (a >= (m + b - e) && a <= (m + b));
    }

    @Test
    public void test1() {
        for (int i = 0; i < 1001; i++) {
            if (isMultiple(i, 100, 0)) {
                System.out.println(i);
            }
        }
    }

    @Test
    public void test2() {
        System.out.println(Math.floor(10.0 / 3));
        System.out.println(Math.ceil(10.0 / 3));
        System.out.println((long) Math.floor(10.0 / 3));
        System.out.println(Math.floorDiv(10, 3));
        System.out.println(Math.floorDiv(10, 4));
    }

    @Test
    public void test3() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");

        String str;
        str = "100";
        System.out.println(engine.eval(str));

        str = "3*(2+3)";
        System.out.println(engine.eval(str));

        str = "3*2/(5-1)";
        System.out.println(engine.eval(str));

        str = "(3*2/(5-2)+1)%2";
        System.out.println(engine.eval(str));
    }

    @Test
    public void test4() {
        String str;

        str = "100";
        System.out.println(Calculator.conversion(str));

        str = "3*(2+3)";
        System.out.println(Calculator.conversion(str));

        str = "3*2/(5-2)";
        System.out.println(Calculator.conversion(str));

        str = "(3*2/(5-2)+1)%2";
        System.out.println(Calculator.conversion(str));
    }


}
