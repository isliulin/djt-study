package com.djt.test.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.HashUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.djt.utils.ParamUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-02-02
 */
@SuppressWarnings("ConstantConditions")
public class StringTest {

    @Test
    public void testStringUtils() {
        String sql = StrUtil.format("ALTER TABLE {} ADD PARTITION P{} VALUES ({})", "xdata.t_test", "20210101", "20210101");
        System.out.println(sql);
        System.out.println(StringUtils.isNumeric("0123"));
        System.out.println(StringUtils.isNumeric("123x"));
        System.out.println(StringUtils.isNumeric("123 456"));
        System.out.println(StringUtils.isNumericSpace("123 456 "));
        System.out.println(StringUtils.isNumericSpace("123 456 x"));
    }

    @Test
    public void testJson() {
        //判断json是否合法
        String str = "{\"a\":\"1\"}";
        System.out.println(JSONValidator.fromUtf8(str.getBytes(StandardCharsets.UTF_8)).validate());
        System.out.println(JSONObject.isValid("666"));
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("A", "123");
        System.out.println("A:" + jsonObject.getLongValue("A"));
        jsonObject.put("B", "12 3");
        System.out.println("B:" + jsonObject.getLongValue("B"));
        jsonObject.put("C", "12 x");
        System.out.println("C:" + jsonObject.getLongValue("C"));
    }

    @Test
    public void testJson2() {
        System.out.println(JSONUtil.isJson("666"));
        System.out.println(JSONUtil.isJson("{}"));
        System.out.println(JSONUtil.isJson("{\"A\"}"));
        System.out.println(JSONUtil.isJsonObj("{}"));
        System.out.println(JSONUtil.isJsonObj("{\"A\"}"));
    }

    @Test
    public void testMap() {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("A", null);
        System.out.println(dataMap.get("A"));
        System.out.println(dataMap.getOrDefault("A", "").toString());
    }

    @Test
    public void testMOptional() {
        String str = null;
        Optional<String> optional = Optional.ofNullable(str);
        System.out.println(optional.orElse("666"));
        str = "xxx";
        optional = Optional.ofNullable(str);
        System.out.println(optional.orElse("777"));
    }

    @Test
    public void testValidate() {
        String a = "666";
        Validate.notNull(a);
        a = null;
        Validate.notNull(a);
    }

    @Test
    public void testSplit() {
        String a = "  1   2 3  4 5       6 ";
        //自带分割
        String[] strArr = a.split(" ");
        for (String s : strArr) {
            System.out.println(s);
        }
        System.out.println(Arrays.toString(strArr));

        //工具分割
        strArr = StringUtils.split(a, " ");
        for (String s : strArr) {
            System.out.println(s);
        }
        System.out.println(Arrays.toString(strArr));
    }

    @Test
    public void testList() {
        List<String> list = new ArrayList<>(10);
        //list.add(0, "666");
        //list.add(1, "A");
        //list.add(2, "B");
        //list.add(3, "C");
        list.set(1, "666");
        System.out.println(list);
    }

    @Test
    public void testInt() {
        Integer a = new Integer(1);
        Integer b = new Integer(1);
        System.out.println(a == b);
        System.out.println(a.equals(b));
        System.out.println(a.compareTo(b));
    }

    @Test
    public void testBean() {
        JSONObject a = new JSONObject();
        a.put("A", "1");
        JSONObject b = new JSONObject();
        BeanUtil.copyProperties(a, b, false);
        System.out.println(b);
        a.put("A", "2");
        System.out.println(b);
    }

    @Test
    public void testBean2() {
        JSONObject a = new JSONObject();
        JSONObject b = new JSONObject(a);
        System.out.println(b);
        a.put("A", "2");
        System.out.println(b);
    }

    @Test
    public void testBool() {
        String a = null;
        String b = null;
        printAB(a, b);
        a = "a";
        printAB(a, b);
        a = null;
        b = "b";
        printAB(a, b);
        a = "a";
        b = "b";
        printAB(a, b);

    }

    private void printAB(String a, String b) {
        if (a == null && b == null) {
            System.out.println("ab都为空");
        } else if (a != null && b == null) {
            System.out.println("a不为空b为空");
        } else if (a == null && b != null) {
            System.out.println("a为空b不为空");
        } else {
            System.out.println("ab都不为空");
        }
    }

    @Test
    public void testParama() {
        String a = "Abc_deF_GHI";
        System.out.println(ParamUtils.toLowerCamel(a));
        System.out.println(ParamUtils.toUpperUnderline(a));
        String b = "abcDefGHi";
        System.out.println(ParamUtils.toLowerCamel(b));
        System.out.println(ParamUtils.toUpperUnderline(b));
    }

    @Test
    public void testPrintChar() {
        System.out.println("「￣￣");
        System.out.println("│￣￣");
        System.out.println("┍━━━━");
        System.out.println("┕━━━━");
        System.out.println("┍━━━━");
        System.out.println("▁");
        System.out.println("▏");
    }

    @Test
    public void testArray() {
        char[] arr = new char[1];
        System.out.println(arr);
        String str = new String(arr);
        System.out.println(str);
        if (" ".equals(str)) {
            System.out.println("char默认值是空格");
        }

        //浅拷贝
        List<Map<String, String>> mapList = Collections.nCopies(3, new HashMap<>());
        mapList.get(0).put("A", "1");
        mapList.get(1).put("B", "2");
        mapList.get(2).put("C", "3");
        for (Map<String, String> map : mapList) {
            System.out.println(map);
        }

        ArrayList<String> list = new ArrayList<>(3);
        //list.set(1, "666"); //报错
        System.out.println(list);
    }

    @Test
    public void testBitSet() {
        BitSet bitSet = new BitSet(10);
        bitSet.set(1);
        bitSet.set(3);
        bitSet.set(5);
        bitSet.set(7);
        bitSet.set(11);
        System.out.println(bitSet.get(2));
        System.out.println(bitSet.get(7));
        System.out.println(bitSet.toString());

        String a = "666";
        String b = "666";
        System.out.println(a.hashCode());
        System.out.println(b.hashCode());


    }

    @Test
    public void testHash() {
        int size = 10000;
        Set<Integer> set = new HashSet<>();
        for (int i = 1; i <= size; i++) {
            int hash = HashUtil.oneByOneHash(String.valueOf(i));
            System.out.println(hash);
            set.add(hash);
        }
        System.out.println(set.size());
    }

    @Test
    public void testMath() {
        System.out.println(Math.pow(2, 3));
    }

    @Test
    public void testAscii() {
        String a = "0123456";
        System.out.println(a.codePointAt(0));
        System.out.println(a.codePointAt(a.length() - 1));
        System.out.println(StringUtils.substring(a, -1));
    }

    @Test
    public void testJoin() {

    }


}
