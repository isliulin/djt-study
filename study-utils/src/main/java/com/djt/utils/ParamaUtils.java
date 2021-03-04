package com.djt.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.StringUtils;

/**
 * 参数转换工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-03-04
 */
public class ParamaUtils {


    /**
     * 字段命名转换
     * 大写下划线 --> 小写驼峰
     *
     * @param attrName 参数名
     * @return p
     */
    public static String toLowerCamel(String attrName) {
        String newAttrName = attrName;
        if (StringUtils.isNotBlank(newAttrName) && newAttrName.contains("_")) {
            newAttrName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, newAttrName);
        }
        return newAttrName;
    }

    /**
     * 字段命名转换
     * 小写驼峰 --> 大写下划线
     *
     * @param attrName 参数名
     * @return p
     */
    public static String toUpperUnderline(String attrName) {
        String newAttrName = attrName;
        if (StringUtils.isNotBlank(newAttrName) && !newAttrName.contains("_")) {
            newAttrName = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, newAttrName);
        }
        return newAttrName;
    }
}
