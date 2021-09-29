package com.djt.event;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * 测试事件对象
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MyEvent implements Serializable {

    @JSONField(name = "id")
    private String id;
    @JSONField(name = "name")
    private String name;
    @JSONField(name = "num")
    private Long num;
    @JSONField(name = "time")
    private String time;

    public long getEventTime() {
        return null == time ? 0L :
                LocalDateTimeUtil.toEpochMilli(LocalDateTimeUtil.parse(time, DatePattern.NORM_DATETIME_FORMATTER));
    }

    private Set<String> nameSet = new HashSet<>();

}
