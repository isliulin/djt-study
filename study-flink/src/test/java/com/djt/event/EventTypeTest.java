package com.djt.event;

import com.djt.enums.EventType;
import com.djt.event.impl.GetEvent;
import org.junit.Test;

/**
 * 事件类型测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-04
 */
public class EventTypeTest {

    @Test
    public void testGetByCode() {
        EventType type = EventType.getByCode(EventType.GET.getCode());
        System.out.println(type);
        assert type == EventType.GET;
    }

    @Test
    public void testGetByClazz() {
        EventType type = EventType.getByClazz(GetEvent.class);
        System.out.println(type);
        assert type == EventType.GET;
    }

    @Test
    public void testGetClazzByCode() {
        Class<?> clazz = EventType.getClazzByCode(EventType.GET.getCode());
        assert clazz != null;
        System.out.println(clazz.getName());
        assert GetEvent.class.equals(clazz);
    }

    @Test
    public void testGetCodeByClazz() {
        String code = EventType.getCodeByClazz(GetEvent.class);
        System.out.println(code);
        assert "03".equals(code);
    }

}
