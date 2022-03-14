package com.djt.test.utils;

import cn.hutool.core.util.ObjectUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 序列化测试
 *
 * @author 　djt317@qq.com
 * @since 　 2022-03-04
 */
public class SerializeTest {

    @Test
    public void test1() {
        List<String> list = new ArrayList<>();
        list.add("A");
        list.add("B");
        list.add("C");

        byte[] bytes = ObjectUtil.serialize(list);
        List<String> list2 = ObjectUtil.deserialize(bytes);
        System.out.println(list2);
    }
}
