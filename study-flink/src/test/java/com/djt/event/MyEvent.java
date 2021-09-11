package com.djt.event;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

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

    @JSONField(name = "id", ordinal = 1)
    private String id;
    @JSONField(name = "name", ordinal = 2)
    private String name;
    @JSONField(name = "num", ordinal = 3)
    private Long num;
    @JSONField(name = "time", ordinal = 4)
    private String time;

    @JSONField(name = "event_time", ordinal = 5, serialize = false, deserialize = false)
    private long eventTime;

    public void setTime(String time) {
        this.time = time;
        eventTime = null == time ? 0L :
                LocalDateTimeUtil.toEpochMilli(LocalDateTimeUtil.parse(time, DatePattern.NORM_DATETIME_FORMATTER));
    }

}
