package com.djt.event;

import java.io.Serializable;

/**
 * 测试事件对象
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-27
 */
public abstract class BaseEvent implements Serializable {

    /**
     * 获取事件事件
     *
     * @return long
     */
    public abstract long getEventTime();
}
