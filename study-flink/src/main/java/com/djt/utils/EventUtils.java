package com.djt.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.ReflectUtil;
import com.alibaba.fastjson.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.ApiExpression;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-08-30
 */
public class EventUtils {


    /**
     * 获取类对应的表字段表达式
     *
     * @param clazz class
     * @return ApiExpression[]
     */
    public static ApiExpression[] getExpressions(Class<?> clazz) {
        Field[] fields = ReflectUtil.getFields(clazz);
        List<ApiExpression> expressionList = new ArrayList<>();
        for (Field field : fields) {
            String expressionName = field.getName();
            JSONField jsonField = field.getAnnotation(JSONField.class);
            if (jsonField != null && StringUtils.isNotBlank(jsonField.name())) {
                expressionName = jsonField.name();
            }
            ApiExpression expression = $(field.getName()).as(expressionName);
            if (StringUtils.containsAny(expressionName, "eventTime", "EVENT_TIME", "event_time")) {
                expression = $(field.getName()).rowtime().as(expressionName);
            }
            expressionList.add(expression);
        }
        return expressionList.toArray(new ApiExpression[0]);
    }

    public static String getTimeWindowStr(TimeWindow window) {
        return "win_start=" + LocalDateTimeUtil.of(window.getStart()).format(DatePattern.NORM_DATETIME_FORMATTER) +
                " ,win_end=" + LocalDateTimeUtil.of(window.getEnd()).format(DatePattern.NORM_DATETIME_FORMATTER);
    }
}
