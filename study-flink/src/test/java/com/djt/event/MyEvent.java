package com.djt.event;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.*;

import java.util.HashSet;
import java.util.Set;

/**
 * 测试事件对象
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-27
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MyEvent extends BaseEvent implements Comparable<MyEvent> {

    @JSONField(name = "id")
    private String id;
    @JSONField(name = "name")
    private String name;
    @JSONField(name = "num")
    private Long num;
    @JSONField(name = "time")
    private String time;

    @Override
    public long getEventTime() {
        return null == time ? 0L :
                LocalDateTimeUtil.toEpochMilli(LocalDateTimeUtil.parse(time, DatePattern.NORM_DATETIME_FORMATTER));
    }

    private Set<String> nameSet = new HashSet<>();

    @Override
    public int compareTo(@NonNull MyEvent o) {
        if (this.getEventTime() > o.getEventTime()) {
            return 0;
        } else if (this.getEventTime() < o.getEventTime()) {
            return -1;
        } else {
            return 0;
        }
    }
}
