package com.djt.event;

import lombok.Data;

import java.io.Serializable;

/**
 * 事件基础类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-03
 */
@Data
public abstract class BaseEvent implements Serializable {

    /**
     * 上层对象的引用
     */
    protected EventRecord eventRecord;

    /**
     * 获取时间时间
     *
     * @return timestamp
     */
    public abstract long getEventTime();

}
