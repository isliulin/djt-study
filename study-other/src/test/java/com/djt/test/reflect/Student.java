package com.djt.test.reflect;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-03-02 10:06
 */
public class Student extends Person<String, Integer> {

    public Student(String key, Integer value) {
        super(key, value);
    }
}
