package com.djt.test.utils;

import cn.hutool.core.util.StrUtil;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @date 　  2021-02-02 10:24
 */
public class HutoolTest {

    @Test
    public void testStrUtil() {
        String sql = StrUtil.format("ALTER TABLE {} ADD PARTITION P{} VALUES ({})", "xdata.t_test", "20210101", "20210101");
        System.out.println(sql);
    }
}
