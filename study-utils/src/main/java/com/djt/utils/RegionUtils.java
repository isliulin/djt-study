package com.djt.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 地图工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-04
 */
public class RegionUtils {

    /**
     * 百度接口认证码
     */
    private static final String AK = "edGc5mIugVxx7lwUx9YpraKeWmExG64o";

    /**
     * 根据地址解析经纬度
     *
     * @param address 中文地址
     * @return json
     */
    public static JSONObject getRegionByAddress(String address) {
        String url = "http://api.map.baidu.com/geocoding/v3/?ak={}&address={}&output=json";
        String responseStr = HttpUtil.get(StrUtil.format(url, AK, address));
        return JSON.parseObject(responseStr);
    }

    /**
     * 根据经纬度解析地址
     *
     * @param lng 经度
     * @param lat 维度
     * @return json
     */
    public static JSONObject getRegionByLngLat(String lng, String lat) {
        String url = "http://api.map.baidu.com/reverse_geocoding/v3/?ak={}&output=json&coordtype=bd09ll&location={}";
        String location = lat + "," + lng;
        String responseStr = HttpUtil.get(StrUtil.format(url, AK, location));
        return JSON.parseObject(responseStr);
    }


}
