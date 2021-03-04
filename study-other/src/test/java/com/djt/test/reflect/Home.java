package com.djt.test.reflect;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-03-02 10:30
 */
@Setter
@Getter
@ToString
public class Home<P extends Person<?, ?>> {

    private P person;

    public Home(P person) {
        this.person = person;
    }

    public void getType() {
        ParameterizedType parameterizedType = (ParameterizedType) person.getClass().getGenericSuperclass();
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
