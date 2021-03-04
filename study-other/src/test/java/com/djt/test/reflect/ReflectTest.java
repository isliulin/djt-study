package com.djt.test.reflect;

import org.junit.Test;

/**
 * 反射与泛型-测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-02 10:09
 */
public class ReflectTest {

    @Test
    public void testPerson() {
        Student student = new Student("张三", 666);
        System.out.println(student);
        student.getType();
    }

    @Test
    public void testHome() {
        Student student = new Student("张三", 666);
        Home<Student> home = new Home<>(student);
        System.out.println(home);
        home.getType();
    }
}
