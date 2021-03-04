package com.djt.test.reflect;

import lombok.ToString;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-03-02 10:05
 */
@ToString
public abstract class Person<K, V> {

    protected K key;
    protected V value;

    public Person(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public void getType() {
        ParameterizedType parameterizedType = (ParameterizedType) getClass().getGenericSuperclass();
        System.out.println(parameterizedType.getClass().getName());
        System.out.println(parameterizedType);
        Type[] typeArr = parameterizedType.getActualTypeArguments();
        for (Type type : typeArr) {
            if (type instanceof Class) {
                Class<?> clazz = (Class<?>) type;
                System.out.println(clazz.getName());
            }
        }
    }

}
