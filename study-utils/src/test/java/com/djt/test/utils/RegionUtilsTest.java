package com.djt.test.utils;

import com.alibaba.fastjson.JSONObject;
import com.djt.utils.RegionUtils;
import org.junit.Test;

/**
 * 地图工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-04
 */
public class RegionUtilsTest {

    @Test
    public void testGetRegionByLngLat() {
        JSONObject jsonObject = RegionUtils.getRegionByLngLat("111.55", "23.85");
        System.out.println(jsonObject);
    }

}
