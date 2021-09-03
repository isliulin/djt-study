package com.djt.enums;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.event.BaseEvent;
import com.djt.event.impl.GetEvent;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * 事件类型
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-03
 */
public enum EventType {

    /**
     * GET
     */
    GET("01", GetEvent.class);

    /**
     * 类型编码
     */
    @Getter
    private final String code;

    /**
     * 类型class
     */
    @Getter
    private final Class<? extends BaseEvent> clazz;

    EventType(String code, Class<? extends BaseEvent> clazz) {
        this.code = code;
        this.clazz = clazz;
    }

    /**
     * 根据类型编码获取事件
     *
     * @param code 类型编码
     * @return EventType
     */
    public static EventType getByCode(String code) {
        if (StringUtils.isEmpty(code)) {
            return null;
        }
        for (EventType type : EventType.values()) {
            if (code.equals(type.code)) {
                return type;
            }
        }
        return null;
    }

    /**
     * 根据class获取事件
     *
     * @param clazz clazz
     * @return EventType
     */
    public static EventType getByClazz(Class<? extends BaseEvent> clazz) {
        if (null == clazz) {
            return null;
        }
        for (EventType type : EventType.values()) {
            if (clazz.equals(type.clazz)) {
                return type;
            }
        }
        return null;
    }

    /**
     * 根据编码获取事件的class
     *
     * @param code 类型编码
     * @return Class
     */
    public static Class<? extends BaseEvent> getClazzByCode(String code) {
        if (StringUtils.isEmpty(code)) {
            return null;
        }
        for (EventType type : EventType.values()) {
            if (code.equals(type.code)) {
                return type.clazz;
            }
        }
        return null;
    }

    /**
     * 根据class获取事件的编码
     *
     * @param clazz class
     * @return code
     */
    public static String getCodeByClazz(Class<? extends BaseEvent> clazz) {
        if (null == clazz) {
            return null;
        }
        for (EventType type : EventType.values()) {
            if (clazz.equals(type.clazz)) {
                return type.code;
            }
        }
        return null;
    }

    /**
     * 事件对象转换
     *
     * @param code      类型编码
     * @param eventJson 原数据
     * @return EventValue
     */
    public static BaseEvent parseEventValue(String code, JSONObject eventJson) {
        if (null == eventJson) {
            return null;
        }
        Class<? extends BaseEvent> clazz = getClazzByCode(code);
        if (null == clazz) {
            return null;
        }
        return JSON.parseObject(eventJson.toJSONString(), clazz);
    }

}
